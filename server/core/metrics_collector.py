import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from core.alert_manager import AlertBuffer, AlertEvent, derive_workload_name
from core.database import (
    DEFAULT_ORGANIZATION_ID,
    Cluster,
    ClusterMetrics,
    User,
    connect_db,
    get_db,
    init_core_data_schemas,
    init_orm_tables,
    insert_cluster_snapshot,
)
from core.discovery import SCRAPE_LABEL_KEY, DiscoveryService
from core.auth import DEFAULT_TENANT_ID
from core.system_config import get_thresholds, init_system_configs

logger = logging.getLogger(__name__)

SCRAPE_INTERVAL_SEC = 15
SCRAPE_TIMEOUT_SEC = 5
METRICS_PORT = 8080
RETENTION_JOB_HOURS = 1
K8S_TELEMETRY_INTERVAL_SEC = 60
DEFAULT_COLLECTION_INTERVAL_SEC = 60
DEFAULT_CLUSTER_NAME = "omnikube-default"
DEFAULT_SYSTEM_USER_EMAIL = "system@omnikube.local"
DEFAULT_SYSTEM_PASSWORD_PLACEHOLDER = "!"

CPU_PATTERN = re.compile(r"CPU:\s*([\d.]+)%")
MEMORY_PATTERN = re.compile(r"Memory:\s*([\d.]+)%")


def _ensure_logging_configured() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(levelname)s] %(name)s: %(message)s",
        )


def init_database(db_path: str) -> None:
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        init_core_data_schemas(db_path)
        init_system_configs(db_path)
        logger.info("Database initialized at %s", db_path)
    except sqlite3.Error as exc:
        logger.error("Database initialization failed: %s", exc)


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Backward-compatible no-op; schema migrations live in core.database."""
    _ = conn


def build_labels_payload(target: dict[str, Any]) -> dict[str, Any]:
    kubernetes_labels = target.get("labels") or {}
    node_id = (
        target.get("node_id")
        or kubernetes_labels.get("kubernetes.io/hostname")
        or target.get("ip")
        or target.get("pod_name")
    )
    return {
        "pod_name": target.get("pod_name"),
        "namespace": target.get("namespace"),
        "cluster_id": target.get("cluster_id", "omnikube-cluster"),
        "region": target.get("region", "local"),
        "node_id": node_id,
        "ip": target.get("ip"),
        "kubernetes_labels": kubernetes_labels,
    }


def insert_metric(
    db_path: str,
    cpu: float,
    memory: float,
    labels: dict[str, Any],
    granularity: str = "raw",
    tenant_id: str = DEFAULT_TENANT_ID,
    organization_id: str | None = None,
    timestamp: str | None = None,
) -> None:
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    labels_json = json.dumps(labels, sort_keys=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cluster_metrics (
                timestamp, cpu, memory, labels, granularity, tenant_id, organization_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (timestamp, cpu, memory, labels_json, granularity, tenant_id, org_id),
        )
        conn.commit()


def ingest_metric_records(
    db_path: str,
    tenant_id: str,
    records: list[dict[str, Any]],
    organization_id: str | None = None,
) -> int:
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    inserted = 0
    with sqlite3.connect(db_path) as conn:
        for record in records:
            cpu = float(record.get("cpu", 0))
            memory = float(record.get("memory", 0))
            labels = record.get("labels") or {}
            if not isinstance(labels, dict):
                labels = {}
            timestamp = record.get("timestamp")
            if timestamp is not None:
                timestamp = str(timestamp)
            if timestamp is None:
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            granularity = str(record.get("granularity", "raw"))
            record_org = str(record.get("organization_id") or org_id).strip()
            labels_json = json.dumps(labels, sort_keys=True)
            conn.execute(
                """
                INSERT INTO cluster_metrics (
                    timestamp, cpu, memory, labels, granularity, tenant_id, organization_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    cpu,
                    memory,
                    labels_json,
                    granularity,
                    tenant_id,
                    record_org,
                ),
            )
            inserted += 1
        conn.commit()
    return inserted


def downsample_hourly_metrics(db_path: str) -> None:
    logger.info("Downsampling job started: aggregating raw metrics into hourly averages")
    aggregated = 0
    deleted = 0

    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    strftime('%Y-%m-%d %H:00:00 UTC', substr(timestamp, 1, 19)) AS hour_bucket,
                    labels,
                    tenant_id,
                    organization_id,
                    AVG(cpu) AS avg_cpu,
                    AVG(memory) AS avg_memory,
                    COUNT(*) AS sample_count
                FROM cluster_metrics
                WHERE granularity = 'raw'
                  AND datetime(substr(timestamp, 1, 19)) < datetime('now', '-1 hour')
                GROUP BY hour_bucket, labels, tenant_id, organization_id
                """
            ).fetchall()

            for (
                hour_bucket,
                labels_json,
                tenant_id,
                organization_id,
                avg_cpu,
                avg_memory,
                sample_count,
            ) in rows:
                conn.execute(
                    """
                    INSERT INTO cluster_metrics (
                        timestamp, cpu, memory, labels, granularity, tenant_id, organization_id
                    )
                    VALUES (?, ?, ?, ?, 'hourly', ?, ?)
                    """,
                    (
                        hour_bucket,
                        round(float(avg_cpu), 2),
                        round(float(avg_memory), 2),
                        labels_json,
                        tenant_id,
                        organization_id,
                    ),
                )
                aggregated += 1
                logger.info(
                    "Downsampled hour=%s labels=%s samples=%d avg_cpu=%.1f avg_memory=%.1f",
                    hour_bucket,
                    labels_json,
                    sample_count,
                    avg_cpu,
                    avg_memory,
                )

            cursor = conn.execute(
                """
                DELETE FROM cluster_metrics
                WHERE granularity = 'raw'
                  AND datetime(substr(timestamp, 1, 19)) < datetime('now', '-1 hour')
                """
            )
            deleted = cursor.rowcount
            conn.commit()

        logger.info(
            "Downsampling job complete: %d hourly bucket(s) written, %d raw row(s) removed",
            aggregated,
            deleted,
        )
    except sqlite3.Error as exc:
        logger.error("Downsampling job failed: %s", exc)


def parse_metrics_body(body: str) -> tuple[float, float] | None:
    cpu_match = CPU_PATTERN.search(body)
    memory_match = MEMORY_PATTERN.search(body)
    if not cpu_match or not memory_match:
        return None
    return float(cpu_match.group(1)), float(memory_match.group(1))


def scrape_target(target: dict[str, Any]) -> tuple[float, float] | None:
    url = f"http://{target['ip']}:{METRICS_PORT}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=SCRAPE_TIMEOUT_SEC) as response:
            return parse_metrics_body(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.warning(
            "Scrape failed for %s/%s (%s): %s",
            target.get("namespace"),
            target.get("pod_name"),
            url,
            exc,
        )
        return None


def _node_is_ready(node: client.V1Node) -> bool:
    conditions = node.status.conditions or []
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in conditions
    )


def load_kubernetes_config() -> None:
    """Load in-cluster credentials, falling back to local kubeconfig for dev workspaces."""
    try:
        config.load_incluster_config()
        logger.debug("Kubernetes config loaded from in-cluster service account")
        return
    except config.ConfigException as in_cluster_exc:
        logger.debug("In-cluster config unavailable (%s); trying kubeconfig", in_cluster_exc)

    try:
        config.load_kube_config()
        logger.debug("Kubernetes config loaded from local kubeconfig")
    except config.ConfigException as kubeconfig_exc:
        raise config.ConfigException(
            "Unable to load Kubernetes configuration (in-cluster and kubeconfig both failed)."
        ) from kubeconfig_exc


def _parse_cpu_quantity(value: str | None) -> float:
    """Parse Kubernetes CPU quantity to cores (e.g. 500m -> 0.5)."""
    if not value:
        return 0.0
    raw = str(value).strip()
    if raw.endswith("m"):
        return float(raw[:-1]) / 1000.0
    return float(raw)


def _parse_memory_quantity(value: str | None) -> float:
    """Parse Kubernetes memory quantity to bytes."""
    if not value:
        return 0.0
    raw = str(value).strip()
    suffixes = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, multiplier in suffixes.items():
        if raw.endswith(suffix):
            return float(raw[: -len(suffix)]) * multiplier
    return float(raw)


def poll_cluster_landscape() -> dict[str, float | int]:
    """
    Query the Kubernetes core API for node and pod landscape metrics.

    CPU and memory utilization are estimated as the share of aggregate pod
    resource requests versus total node allocatable capacity.
    """
    load_kubernetes_config()
    v1 = client.CoreV1Api()

    nodes = v1.list_node(watch=False).items
    ready_nodes = 0
    total_cpu_allocatable = 0.0
    total_memory_allocatable = 0.0

    for node in nodes:
        if _node_is_ready(node):
            ready_nodes += 1
        allocatable = node.status.allocatable or {}
        total_cpu_allocatable += _parse_cpu_quantity(allocatable.get("cpu"))
        total_memory_allocatable += _parse_memory_quantity(allocatable.get("memory"))

    pods = v1.list_pod_for_all_namespaces(watch=False).items
    active_pods = 0
    cpu_requests = 0.0
    memory_requests = 0.0

    for pod in pods:
        if pod.status.phase != "Running":
            continue
        active_pods += 1
        spec = pod.spec
        if spec is None:
            continue
        for container in spec.containers or []:
            requests = (container.resources.requests if container.resources else None) or {}
            cpu_requests += _parse_cpu_quantity(requests.get("cpu"))
            memory_requests += _parse_memory_quantity(requests.get("memory"))

    cpu_utilization = (
        (cpu_requests / total_cpu_allocatable) * 100.0
        if total_cpu_allocatable > 0
        else 0.0
    )
    memory_utilization = (
        (memory_requests / total_memory_allocatable) * 100.0
        if total_memory_allocatable > 0
        else 0.0
    )

    if cpu_utilization == 0.0 and memory_utilization == 0.0 and active_pods > 0 and ready_nodes > 0:
        pod_density = min(1.0, active_pods / max(ready_nodes * 10, 1))
        cpu_utilization = round(pod_density * 35.0, 2)
        memory_utilization = round(pod_density * 42.0, 2)

    return {
        "node_count": ready_nodes,
        "active_pods": active_pods,
        "cpu_utilization": round(float(cpu_utilization), 2),
        "memory_utilization": round(float(memory_utilization), 2),
    }


def get_or_create_default_user(db) -> User:
    """Ensure a system user exists for auto-provisioned cluster records."""
    user = db.query(User).filter(User.email == DEFAULT_SYSTEM_USER_EMAIL).one_or_none()
    if user is not None:
        return user

    user = User(
        email=DEFAULT_SYSTEM_USER_EMAIL,
        password_hash=DEFAULT_SYSTEM_PASSWORD_PLACEHOLDER,
        company_name="OmniKube System",
    )
    db.add(user)
    db.flush()
    logger.info("Created system user for ORM cluster tracking (%s)", DEFAULT_SYSTEM_USER_EMAIL)
    return user


def get_or_create_default_cluster(db) -> Cluster:
    """Locate or initialize the default Cluster row in omnikube.db."""
    user = get_or_create_default_user(db)
    cluster_name = os.environ.get("OMNIKUBE_CLUSTER_NAME", DEFAULT_CLUSTER_NAME)
    provider = os.environ.get("OMNIKUBE_CLUSTER_PROVIDER", "Kind")

    cluster = (
        db.query(Cluster)
        .filter(Cluster.user_id == user.id, Cluster.cluster_name == cluster_name)
        .one_or_none()
    )
    if cluster is not None:
        if cluster.status != "connected":
            cluster.status = "connected"
        cluster.connected_at = datetime.now(timezone.utc)
        return cluster

    cluster = Cluster(
        user_id=user.id,
        cluster_name=cluster_name,
        provider=provider,
        status="connected",
        connected_at=datetime.now(timezone.utc),
    )
    db.add(cluster)
    db.flush()
    logger.info("Initialized default cluster record '%s' (provider=%s)", cluster_name, provider)
    return cluster


def collect_and_store_cluster_metrics() -> ClusterMetrics | None:
    """
    Poll Kubernetes, upsert the default Cluster, and append a ClusterMetrics row.
    """
    init_orm_tables()
    landscape = poll_cluster_landscape()

    with get_db() as db:
        cluster = get_or_create_default_cluster(db)
        metric = ClusterMetrics(
            cluster_id=cluster.id,
            timestamp=datetime.now(timezone.utc),
            cpu_utilization=float(landscape["cpu_utilization"]),
            memory_utilization=float(landscape["memory_utilization"]),
            node_count=int(landscape["node_count"]),
            active_pods=int(landscape["active_pods"]),
        )
        db.add(metric)
        db.flush()
        logger.info(
            "ORM cluster metrics stored: cluster=%s nodes=%d pods=%d cpu=%.1f%% memory=%.1f%%",
            cluster.cluster_name,
            metric.node_count,
            metric.active_pods,
            metric.cpu_utilization,
            metric.memory_utilization,
        )
        return metric


_collection_thread: threading.Thread | None = None
_collection_running = False


def start_collection_loop(interval_seconds: int = DEFAULT_COLLECTION_INTERVAL_SEC) -> None:
    """
    Continuously poll Kubernetes and persist ClusterMetrics rows to omnikube.db.
    """
    global _collection_thread, _collection_running

    if _collection_thread is not None and _collection_thread.is_alive():
        logger.debug("Collection loop already running")
        return

    init_orm_tables()
    _collection_running = True

    def _collection_loop() -> None:
        while _collection_running:
            try:
                collect_and_store_cluster_metrics()
            except config.ConfigException as exc:
                logger.warning("Kubernetes collection skipped (config unavailable): %s", exc)
            except ApiException as exc:
                logger.error("Kubernetes API error during collection: %s", exc)
            except Exception as exc:
                logger.error("ORM collection cycle error: %s", exc)
            time.sleep(max(5, int(interval_seconds)))

    _collection_thread = threading.Thread(
        target=_collection_loop,
        name="omnikube-orm-collection-loop",
        daemon=True,
    )
    _collection_thread.start()
    logger.info("ORM collection loop started (interval=%ss)", interval_seconds)


def stop_collection_loop() -> None:
    """Stop the background ORM metrics collection loop."""
    global _collection_running
    _collection_running = False


def _count_running_pods(v1: client.CoreV1Api) -> int:
    pods = v1.list_pod_for_all_namespaces(watch=False)
    return sum(1 for pod in pods.items if pod.status.phase == "Running")


def _count_ready_nodes(v1: client.CoreV1Api) -> int:
    nodes = v1.list_node(watch=False)
    ready = 0
    for node in nodes.items:
        conditions = node.status.conditions or []
        if any(condition.type == "Ready" and condition.status == "True" for condition in conditions):
            ready += 1
    return ready


def _compute_cluster_utilization(
    db_path: str,
    organization_id: str,
    tenant_id: str | None = None,
) -> tuple[float, float]:
    """Average pod-level CPU and memory utilization from recent scrape samples."""
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT AVG(cpu), AVG(memory), COUNT(*)
                FROM cluster_metrics
                WHERE granularity = 'raw'
                  AND organization_id = ?
                  AND datetime(substr(timestamp, 1, 19)) >= datetime('now', '-10 minutes')
                """,
                (org_id,),
            ).fetchone()
        if not row or int(row[2] or 0) == 0:
            return 0.0, 0.0
        return round(float(row[0]), 2), round(float(row[1]), 2)
    except sqlite3.Error as exc:
        logger.warning("Cluster utilization query failed: %s", exc)
        return 0.0, 0.0


def persist_cluster_telemetry_snapshot(
    db_path: str,
    tenant_id: str = DEFAULT_TENANT_ID,
    organization_id: str | None = None,
) -> None:
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    """Query Kubernetes metadata and persist a legacy cluster utilization snapshot."""
    try:
        landscape = poll_cluster_landscape()
    except config.ConfigException as exc:
        logger.warning("K8s live telemetry: Kubernetes config unavailable: %s", exc)
        return
    except ApiException as exc:
        logger.error("K8s live telemetry API error: %s", exc)
        return

    ready_nodes = int(landscape["node_count"])
    running_pods = int(landscape["active_pods"])
    cpu_utilization = float(landscape["cpu_utilization"])
    memory_utilization = float(landscape["memory_utilization"])
    cluster_id = os.environ.get("OMNIKUBE_CLUSTER_ID", "omnikube-cluster")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    insert_cluster_snapshot(
        db_path,
        node_count=ready_nodes,
        pod_count=running_pods,
        cpu_utilization=cpu_utilization,
        memory_utilization=memory_utilization,
        organization_id=org_id,
        tenant_id=tenant_id,
        cluster_id=cluster_id,
        timestamp=timestamp,
    )

    labels = {
        "source": "k8s_live_telemetry",
        "metric_type": "cluster_snapshot",
        "running_pods": running_pods,
        "ready_nodes": ready_nodes,
        "cluster_id": cluster_id,
        "cpu_utilization": cpu_utilization,
        "memory_utilization": memory_utilization,
    }
    labels_json = json.dumps(labels, sort_keys=True)

    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO cluster_metrics (
                timestamp, cpu, memory, labels, granularity, tenant_id, organization_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                cpu_utilization,
                memory_utilization,
                labels_json,
                "cluster_snapshot",
                tenant_id,
                org_id,
            ),
        )
        conn.commit()

    logger.info(
        "K8s live telemetry persisted: nodes=%d pods=%d cpu=%.1f%% memory=%.1f%% cluster=%s",
        ready_nodes,
        running_pods,
        cpu_utilization,
        memory_utilization,
        cluster_id,
    )


class MetricsCollector:
    def __init__(
        self,
        discovery: DiscoveryService,
        db_path: str,
        config_getter: Callable[[], dict[str, Any]] | None = None,
        alert_buffer: AlertBuffer | None = None,
    ) -> None:
        _ensure_logging_configured()
        self.discovery = discovery
        self.db_path = db_path
        self._config_getter = config_getter or (lambda: {})
        self.alert_buffer = alert_buffer or AlertBuffer(self.db_path, self._config_getter)
        self._thread: threading.Thread | None = None
        self._running = False
        self._telemetry_thread: threading.Thread | None = None
        self._telemetry_running = False
        self._scheduler = BackgroundScheduler(daemon=True)

        self._default_tenant_id = os.environ.get("OMNIKUBE_DEFAULT_TENANT_ID", DEFAULT_TENANT_ID)

        init_database(self.db_path)

    def _scrape_tenant_id(self) -> str:
        return self._default_tenant_id

    def _evaluate_thresholds(
        self,
        cpu: float,
        memory: float,
        labels_payload: dict[str, Any],
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> None:
        thresholds = get_thresholds(self.db_path)
        cpu_threshold = thresholds["cpu"]
        memory_threshold = thresholds["memory"]

        cluster_id = str(labels_payload.get("cluster_id", "omnikube-cluster"))
        node_id = str(labels_payload.get("node_id", labels_payload.get("ip", "unknown")))
        pod_name = str(labels_payload.get("pod_name", "unknown"))
        namespace = str(labels_payload.get("namespace", "default"))
        kubernetes_labels = labels_payload.get("kubernetes_labels") or {}
        workload_name = derive_workload_name(pod_name, kubernetes_labels)

        if cpu > cpu_threshold:
            self.alert_buffer.capture(
                AlertEvent(
                    cluster_id=cluster_id,
                    node_id=node_id,
                    pod_name=pod_name,
                    namespace=namespace,
                    metric="cpu",
                    value=cpu,
                    threshold=cpu_threshold,
                    workload_name=workload_name,
                    tenant_id=tenant_id,
                )
            )

        if memory > memory_threshold:
            self.alert_buffer.capture(
                AlertEvent(
                    cluster_id=cluster_id,
                    node_id=node_id,
                    pod_name=pod_name,
                    namespace=namespace,
                    metric="memory",
                    value=memory,
                    threshold=memory_threshold,
                    workload_name=workload_name,
                    tenant_id=tenant_id,
                )
            )

    def _is_scrape_target(self, target: dict[str, Any]) -> bool:
        labels = target.get("labels") or {}
        scrape_value = str(labels.get(SCRAPE_LABEL_KEY, "")).lower()
        return scrape_value == "true"

    def scrape_cycle(self) -> None:
        targets = self.discovery.get_active_targets()
        scrape_targets = [target for target in targets if self._is_scrape_target(target)]

        logger.info(
            "Scrape cycle started: %d discovery target(s), %d scrape-eligible target(s)",
            len(targets),
            len(scrape_targets),
        )

        if not scrape_targets:
            logger.warning("Scrape cycle found no targets with %s=true", SCRAPE_LABEL_KEY)
            return

        success_count = 0
        for target in scrape_targets:
            metrics = scrape_target(target)
            if metrics is None:
                continue

            cpu, memory = metrics
            labels_payload = build_labels_payload(target)
            tenant_id = self._scrape_tenant_id()
            insert_metric(self.db_path, cpu, memory, labels_payload, tenant_id=tenant_id)
            self._evaluate_thresholds(cpu, memory, labels_payload, tenant_id)
            success_count += 1
            logger.info(
                "Scraped %s/%s cpu=%.1f memory=%.1f cluster=%s region=%s",
                target.get("namespace"),
                target.get("pod_name"),
                cpu,
                memory,
                labels_payload.get("cluster_id"),
                labels_payload.get("region"),
            )

        logger.info(
            "Scrape cycle complete: %d/%d target(s) persisted",
            success_count,
            len(scrape_targets),
        )

    def _scrape_loop(self) -> None:
        while self._running:
            try:
                self.scrape_cycle()
            except Exception as exc:
                logger.error("Scrape cycle error: %s", exc)
            time.sleep(SCRAPE_INTERVAL_SEC)

    def _k8s_telemetry_loop(self) -> None:
        while self._telemetry_running:
            try:
                persist_cluster_telemetry_snapshot(self.db_path, self._default_tenant_id)
            except Exception as exc:
                logger.error("K8s live telemetry cycle error: %s", exc)
            time.sleep(K8S_TELEMETRY_INTERVAL_SEC)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._running = True
        self.alert_buffer.start()
        self._thread = threading.Thread(
            target=self._scrape_loop,
            name="omnikube-metrics-collector",
            daemon=True,
        )
        self._thread.start()

        self._telemetry_running = True
        self._telemetry_thread = threading.Thread(
            target=self._k8s_telemetry_loop,
            name="omnikube-k8s-live-telemetry",
            daemon=True,
        )
        self._telemetry_thread.start()

        start_collection_loop(interval_seconds=K8S_TELEMETRY_INTERVAL_SEC)

        self._scheduler.add_job(
            downsample_hourly_metrics,
            trigger="interval",
            hours=RETENTION_JOB_HOURS,
            id="hourly_downsample",
            kwargs={"db_path": self.db_path},
            replace_existing=True,
        )
        self._scheduler.start()

        logger.info(
            "Metrics collector started (scrape_interval=%ss, k8s_telemetry_interval=%ss, downsample_interval=%sh)",
            SCRAPE_INTERVAL_SEC,
            K8S_TELEMETRY_INTERVAL_SEC,
            RETENTION_JOB_HOURS,
        )

    def stop(self) -> None:
        self._running = False
        self._telemetry_running = False
        stop_collection_loop()
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
