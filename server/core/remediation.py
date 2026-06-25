"""ChatOps remediation — apply optimization actions against Kubernetes or simulate locally."""

import logging
import os
import time
from typing import Any

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from core.analytics_engine import init_incident_events, record_incident_event

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
SUPPORTED_ACTIONS = frozenset({"downscale_nodes", "upscale_nodes", "reduce_replicas"})
DEFAULT_CLUSTER_ID = "omnikube-cluster"
SUCCESS_MESSAGE = (
    "Cluster automation triggered successfully. Remediating infrastructure scale."
)


def _utc_timestamp() -> str:
    return time.strftime(TIMESTAMP_FORMAT, time.gmtime())


def _load_k8s_clients() -> tuple[client.CoreV1Api, client.AppsV1Api] | None:
    for loader in (config.load_incluster_config, config.load_kube_config):
        try:
            loader()
            return client.CoreV1Api(), client.AppsV1Api()
        except config.ConfigException:
            continue
    return None


def _node_is_ready(node: client.V1Node) -> bool:
    conditions = node.status.conditions or []
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in conditions
    )


def _execute_downscale_nodes(
    v1: client.CoreV1Api,
    *,
    cluster_id: str,
    target: str | None = None,
) -> dict[str, Any]:
    target_size = str(target or "medium").strip().lower()
    nodes = v1.list_node(watch=False).items
    ready_nodes = [node for node in nodes if _node_is_ready(node)]

    if len(ready_nodes) <= 1:
        return {
            "mode": "simulated",
            "detail": (
                f"Single-node cluster — simulated downscale to {target_size} instance tier"
            ),
            "nodes_targeted": 0,
            "target": target_size,
            "cluster_id": cluster_id,
        }

    target_node = ready_nodes[-1]
    node_name = target_node.metadata.name
    v1.patch_node(
        node_name,
        body={
            "metadata": {
                "annotations": {
                    "omnikube.cloud/remediation": "downscale_nodes",
                    "omnikube.cloud/remediation-at": _utc_timestamp(),
                    "omnikube.cloud/target-instance-size": target_size,
                }
            }
        },
    )
    return {
        "mode": "executed",
        "detail": (
            f"Annotated node {node_name} for downscale remediation "
            f"(target instance tier: {target_size})"
        ),
        "nodes_targeted": 1,
        "node_name": node_name,
        "target": target_size,
        "cluster_id": cluster_id,
    }


def _execute_upscale_nodes(
    v1: client.CoreV1Api,
    *,
    cluster_id: str,
    target: str | None = None,
) -> dict[str, Any]:
    target_size = str(target or "large").strip().lower()
    nodes = v1.list_node(watch=False).items
    ready_nodes = [node for node in nodes if _node_is_ready(node)]

    if not ready_nodes:
        return {
            "mode": "simulated",
            "detail": f"No ready nodes found — simulated upscale to {target_size} instance tier",
            "nodes_targeted": 0,
            "target": target_size,
            "cluster_id": cluster_id,
        }

    target_node = ready_nodes[0]
    node_name = target_node.metadata.name
    v1.patch_node(
        node_name,
        body={
            "metadata": {
                "annotations": {
                    "omnikube.cloud/remediation": "upscale_nodes",
                    "omnikube.cloud/remediation-at": _utc_timestamp(),
                    "omnikube.cloud/target-instance-size": target_size,
                }
            }
        },
    )
    return {
        "mode": "executed",
        "detail": (
            f"Annotated node {node_name} for predictive scale-up remediation "
            f"(target instance tier: {target_size})"
        ),
        "nodes_targeted": 1,
        "node_name": node_name,
        "target": target_size,
        "cluster_id": cluster_id,
    }


def _execute_reduce_replicas(
    apps_v1: client.AppsV1Api,
    *,
    namespace: str | None,
    deployment: str | None,
    target_replicas: int | None,
    cluster_id: str,
) -> dict[str, Any]:
    if namespace and deployment:
        current = apps_v1.read_namespaced_deployment(deployment, namespace)
        replicas = int(current.spec.replicas or 1)
        new_replicas = (
            max(1, int(target_replicas))
            if target_replicas is not None
            else max(1, replicas - 1)
        )
        if new_replicas >= replicas:
            new_replicas = max(1, replicas - 1) if replicas > 1 else 1

        apps_v1.patch_namespaced_deployment_scale(
            deployment,
            namespace,
            body={"spec": {"replicas": new_replicas}},
        )
        return {
            "mode": "executed",
            "detail": (
                f"Scaled deployment {namespace}/{deployment} "
                f"from {replicas} to {new_replicas} replicas"
            ),
            "namespace": namespace,
            "deployment": deployment,
            "previous_replicas": replicas,
            "replicas": new_replicas,
            "cluster_id": cluster_id,
        }

    deployments = apps_v1.list_deployment_for_all_namespaces(watch=False).items
    candidates = [
        dep
        for dep in deployments
        if int(dep.spec.replicas or 0) > 1
        and (namespace is None or dep.metadata.namespace == namespace)
    ]
    if not candidates:
        return {
            "mode": "simulated",
            "detail": "No multi-replica deployments found — replica reduction simulated",
            "deployments_targeted": 0,
            "cluster_id": cluster_id,
        }

    target = sorted(
        candidates,
        key=lambda dep: int(dep.spec.replicas or 0),
        reverse=True,
    )[0]
    dep_name = target.metadata.name
    dep_namespace = target.metadata.namespace
    replicas = int(target.spec.replicas or 1)
    new_replicas = (
        max(1, int(target_replicas))
        if target_replicas is not None
        else max(1, replicas - 1)
    )

    apps_v1.patch_namespaced_deployment_scale(
        dep_name,
        dep_namespace,
        body={"spec": {"replicas": new_replicas}},
    )
    return {
        "mode": "executed",
        "detail": (
            f"Scaled deployment {dep_namespace}/{dep_name} "
            f"from {replicas} to {new_replicas} replicas"
        ),
        "namespace": dep_namespace,
        "deployment": dep_name,
        "previous_replicas": replicas,
        "replicas": new_replicas,
        "cluster_id": cluster_id,
    }


def apply_optimization_remediation(
    db_path: str,
    *,
    tenant_id: str,
    action: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Execute or simulate a cluster remediation action and persist a mitigation incident.
    """
    payload = payload or {}
    action_key = str(action or "").strip().lower()
    if action_key not in SUPPORTED_ACTIONS:
        raise ValueError(
            f"Unsupported action {action!r}. "
            f"Supported values: {', '.join(sorted(SUPPORTED_ACTIONS))}"
        )

    cluster_id = str(
        payload.get("cluster_id")
        or os.environ.get("OMNIKUBE_CLUSTER_ID", DEFAULT_CLUSTER_ID)
    )
    namespace = str(payload.get("namespace") or "").strip() or None
    deployment = str(payload.get("deployment") or "").strip() or None
    target = str(payload.get("target") or "").strip() or None
    target_replicas_raw = payload.get("target_replicas")
    target_replicas = (
        int(target_replicas_raw)
        if target_replicas_raw is not None
        else None
    )

    init_incident_events(db_path)
    clients = _load_k8s_clients()
    execution: dict[str, Any]

    try:
        if clients is None:
            execution = {
                "mode": "simulated",
                "detail": f"Kubernetes API unavailable — simulated {action_key}",
                "cluster_id": cluster_id,
            }
            if action_key == "downscale_nodes":
                execution["target"] = str(target or "medium").lower()
            elif action_key == "upscale_nodes":
                execution["target"] = str(target or "large").lower()
        elif action_key == "downscale_nodes":
            execution = _execute_downscale_nodes(
                clients[0],
                cluster_id=cluster_id,
                target=target,
            )
        elif action_key == "upscale_nodes":
            execution = _execute_upscale_nodes(
                clients[0],
                cluster_id=cluster_id,
                target=target,
            )
        else:
            execution = _execute_reduce_replicas(
                clients[1],
                namespace=namespace,
                deployment=deployment,
                target_replicas=target_replicas,
                cluster_id=cluster_id,
            )
    except ApiException as exc:
        logger.error("Kubernetes remediation API error (%s): %s", action_key, exc)
        execution = {
            "mode": "simulated",
            "detail": f"Kubernetes API error — simulated {action_key}: {exc.reason}",
            "cluster_id": cluster_id,
        }

    timestamp = _utc_timestamp()
    target_note = f" target={target}" if target and action_key == "downscale_nodes" else ""
    message = (
        f"ChatOps remediation ({action_key}{target_note}): "
        f"{execution.get('detail', 'completed')} "
        f"[{execution.get('mode', 'unknown')}]"
    )
    record_incident_event(
        db_path,
        tenant_id=tenant_id,
        timestamp=timestamp,
        event_type="mitigation",
        pod_name=str(deployment or ""),
        namespace=str(namespace or ""),
        cluster_id=cluster_id,
        metric="remediation",
        severity="info",
        message=message,
    )
    logger.info("[Remediation] %s tenant=%s %s", action_key, tenant_id, message)

    return {
        "status": "success",
        "message": SUCCESS_MESSAGE,
        "action": action_key,
        "target": execution.get("target") or target,
        "tenant_id": tenant_id,
        "execution": execution,
        "incident_logged": True,
    }
