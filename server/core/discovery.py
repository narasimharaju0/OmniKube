import logging
import os
import threading
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

DISCOVERY_INTERVAL_SEC = 30
SCRAPE_LABEL_KEY = "omnikube.io/scrape"
SCRAPE_LABEL_SELECTOR = f"{SCRAPE_LABEL_KEY}=true"
SERVICE_ACCOUNT_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SERVICE_ACCOUNT_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


def _ensure_logging_configured() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(levelname)s] %(name)s: %(message)s",
        )


class DiscoveryService:
    def __init__(self) -> None:
        _ensure_logging_configured()
        self._lock = threading.Lock()
        self._active_targets: list[dict[str, str]] = []
        self._thread: threading.Thread | None = None
        self._running = False
        self._api: client.CoreV1Api | None = None
        self._connect()

    def _log_service_account_diagnostics(self) -> None:
        logger.info("Service account token path: %s", SERVICE_ACCOUNT_TOKEN_PATH)
        token_exists = os.path.exists(SERVICE_ACCOUNT_TOKEN_PATH)
        logger.info("Service account token exists: %s", token_exists)

        if token_exists:
            try:
                with open(SERVICE_ACCOUNT_TOKEN_PATH, encoding="utf-8") as handle:
                    token = handle.read()
                logger.info("Service account token is readable: yes (length=%d)", len(token))
            except OSError as exc:
                logger.error("Service account token is readable: no (%s)", exc)
        else:
            logger.warning(
                "Service account token not found — discovery will fall back to kubeconfig if available"
            )

        if os.path.exists(SERVICE_ACCOUNT_NAMESPACE_PATH):
            try:
                with open(SERVICE_ACCOUNT_NAMESPACE_PATH, encoding="utf-8") as handle:
                    namespace = handle.read().strip()
                logger.info("Service account namespace: %s", namespace)
            except OSError as exc:
                logger.warning("Could not read service account namespace: %s", exc)

    def _connect(self) -> None:
        self._log_service_account_diagnostics()

        if os.path.exists(SERVICE_ACCOUNT_TOKEN_PATH):
            try:
                config.load_incluster_config()
                logger.info("Discovery authenticated via load_incluster_config() (pod service account)")
            except config.ConfigException as exc:
                logger.error("Discovery in-cluster config error: %s", exc)
                self._api = None
                return
        else:
            try:
                config.load_kube_config()
                logger.info("Discovery authenticated via load_kube_config() (local kubeconfig)")
            except config.ConfigException as exc:
                logger.error("Discovery kubeconfig error: %s", exc)
                self._api = None
                return

        try:
            self._api = client.CoreV1Api()
        except Exception as exc:
            logger.error("Discovery connection error: %s", exc)
            self._api = None

    def _log_unfiltered_pods_diagnostic(self) -> None:
        logger.warning(
            "Label selector %r returned 0 pods — listing pods without label selector to verify API connectivity",
            SCRAPE_LABEL_SELECTOR,
        )

        try:
            response = self._api.list_pod_for_all_namespaces()
            all_pods = response.items or []
            logger.info(
                "Unfiltered list_pod_for_all_namespaces returned %d pod(s)",
                len(all_pods),
            )
            for pod in all_pods:
                metadata = pod.metadata
                if not metadata:
                    continue
                labels = metadata.labels or {}
                logger.info(
                    "Unfiltered pod namespace=%s name=%s labels=%s",
                    metadata.namespace,
                    metadata.name,
                    labels,
                )
            return
        except ApiException as exc:
            logger.warning(
                "Unfiltered all-namespace pod list failed (status=%s reason=%s): %s",
                exc.status,
                exc.reason,
                exc.body,
            )
        except Exception as exc:
            logger.warning("Unfiltered all-namespace pod list failed: %s", exc)

        try:
            response = self._api.list_namespaced_pod(namespace="default")
            default_pods = response.items or []
            logger.info(
                "Fallback list_namespaced_pod(namespace='default') returned %d pod(s)",
                len(default_pods),
            )
            for pod in default_pods:
                metadata = pod.metadata
                if not metadata:
                    continue
                labels = metadata.labels or {}
                logger.info(
                    "Default namespace pod name=%s labels=%s",
                    metadata.name,
                    labels,
                )
        except ApiException as exc:
            logger.error(
                "Fallback default namespace pod list failed (status=%s reason=%s): %s",
                exc.status,
                exc.reason,
                exc.body,
            )
        except Exception as exc:
            logger.error("Fallback default namespace pod list failed: %s", exc)

    def get_scrape_targets(self) -> list[dict[str, str]]:
        if self._api is None:
            logger.error("Discovery connection error: Kubernetes client is not initialized")
            return []

        logger.info("SCRAPE_LABEL_SELECTOR=%r", SCRAPE_LABEL_SELECTOR)

        try:
            all_pods = self._api.list_pod_for_all_namespaces(label_selector=None)
            logging.info(f"DEBUG: Total pods in cluster: {len(all_pods.items)}")
            for p in all_pods.items:
                logging.info(f"DEBUG: Found pod {p.metadata.name} with labels {p.metadata.labels}")
        except Exception as exc:
            logging.info(f"DEBUG: Unfiltered pod list failed: {exc}")

        try:
            response = self._api.list_pod_for_all_namespaces(
                label_selector=SCRAPE_LABEL_SELECTOR,
            )
        except ApiException as exc:
            logger.error(
                "Discovery API error (status=%s reason=%s): %s",
                exc.status,
                exc.reason,
                exc.body,
            )
            return []
        except Exception as exc:
            logger.error("Discovery connection error: %s", exc)
            return []

        matched_pods = response.items or []
        total_pods = len(matched_pods)
        resource_version = (
            response.metadata.resource_version if response.metadata else "unknown"
        )

        logger.info(
            "Total pods found across all namespaces: %d (selector=%r, resource_version=%s)",
            total_pods,
            SCRAPE_LABEL_SELECTOR,
            resource_version,
        )

        if total_pods == 0:
            logger.warning(
                "Zero pods matched selector %r. API response: items=%s metadata.resource_version=%s",
                SCRAPE_LABEL_SELECTOR,
                response.items,
                resource_version,
            )
            self._log_unfiltered_pods_diagnostic()

        targets: list[dict[str, str]] = []
        for pod in matched_pods:
            metadata = pod.metadata
            status = pod.status
            pod_name = metadata.name if metadata else "unknown"
            namespace = metadata.namespace if metadata else "unknown"
            labels = metadata.labels if metadata and metadata.labels else {}
            scrape_label = labels.get(SCRAPE_LABEL_KEY)
            pod_ip = status.pod_ip if status else None
            phase = status.phase if status else "Unknown"

            logger.info(
                "Matched pod namespace=%s name=%s phase=%s ip=%s labels=%s scrape_label=%r",
                namespace,
                pod_name,
                phase,
                pod_ip or "none",
                labels,
                scrape_label,
            )

            if not metadata or not metadata.name or not metadata.namespace:
                logger.warning(
                    "Skipping pod namespace=%s name=%s: missing metadata",
                    namespace,
                    pod_name,
                )
                continue

            if not pod_ip:
                logger.warning(
                    "Skipping pod %s/%s (phase=%s): pod has no IP yet",
                    namespace,
                    pod_name,
                    phase,
                )
                continue

            targets.append(
                {
                    "pod_name": metadata.name,
                    "namespace": metadata.namespace,
                    "ip": pod_ip,
                    "labels": dict(labels),
                    "cluster_id": os.environ.get("OMNIKUBE_CLUSTER_ID", "omnikube-cluster"),
                    "region": os.environ.get("OMNIKUBE_REGION", "local"),
                    "node_id": labels.get("kubernetes.io/hostname", pod_ip),
                }
            )

        if matched_pods and not targets:
            logger.warning(
                "All %d matched pod(s) were skipped (usually missing pod IP or metadata)",
                len(matched_pods),
            )

        logger.info(
            "Discovery resolved %d scrape target(s): %s",
            len(targets),
            [f"{t['namespace']}/{t['pod_name']}" for t in targets],
        )
        return targets

    def _discovery_loop(self) -> None:
        while self._running:
            logger.info("Discovery cycle started")
            try:
                targets = self.get_scrape_targets()
                with self._lock:
                    self._active_targets = targets
                logger.info("Found %d targets", len(targets))
            except Exception as exc:
                logger.error("Discovery connection error: %s", exc)

            time.sleep(DISCOVERY_INTERVAL_SEC)

    def start_discovery(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        _ensure_logging_configured()
        self._running = True
        self._thread = threading.Thread(
            target=self._discovery_loop,
            name="omnikube-discovery",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Discovery background thread started (interval=%ss, selector=%s)",
            DISCOVERY_INTERVAL_SEC,
            SCRAPE_LABEL_SELECTOR,
        )

    @property
    def active_targets(self) -> list[dict[str, str]]:
        with self._lock:
            return list(self._active_targets)

    def get_active_targets(self) -> list[dict[str, str]]:
        return self.active_targets
