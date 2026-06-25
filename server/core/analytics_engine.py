"""Background analytics engine: incident event persistence and breach scanning."""

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any

from core.alert_manager import derive_workload_name
from core.auth import DEFAULT_TENANT_ID
from core.cost_optimizer import build_cost_optimization_report
from core.notifier import (
    build_cost_remediation_context,
    dispatch_cost_optimization_alert,
    get_cost_alert_limit_usd,
)
from core.optimizer import scan_predictive_scale_up
from core.system_config import get_thresholds

logger = logging.getLogger(__name__)

SCAN_INTERVAL_SEC = 60
CRITICAL_CPU_PCT = 95.0
CRITICAL_MEMORY_PCT = 95.0
COST_ALERT_COOLDOWN_SEC = int(os.environ.get("OMNIKUBE_COST_ALERT_COOLDOWN_SEC", "900"))

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def init_incident_events(db_path: str) -> None:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS incident_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    pod_name TEXT NOT NULL DEFAULT '',
                    namespace TEXT NOT NULL DEFAULT '',
                    cluster_id TEXT NOT NULL DEFAULT '',
                    metric TEXT NOT NULL DEFAULT '',
                    value REAL,
                    threshold REAL,
                    severity TEXT NOT NULL DEFAULT 'info',
                    message TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_incident_events_tenant_ts "
                "ON incident_events(tenant_id, timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_incident_events_pod "
                "ON incident_events(pod_name, timestamp)"
            )
            conn.commit()
        logger.info("Incident events table initialized")
    except sqlite3.Error as exc:
        logger.error("Incident events initialization failed: %s", exc)


def _parse_labels(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def record_incident_event(
    db_path: str,
    *,
    tenant_id: str,
    timestamp: str,
    event_type: str,
    pod_name: str = "",
    namespace: str = "",
    cluster_id: str = "",
    metric: str = "",
    value: float | None = None,
    threshold: float | None = None,
    severity: str = "info",
    message: str = "",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO incident_events (
                tenant_id, timestamp, event_type, pod_name, namespace, cluster_id,
                metric, value, threshold, severity, message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                timestamp,
                event_type,
                pod_name,
                namespace,
                cluster_id,
                metric,
                value,
                threshold,
                severity,
                message,
            ),
        )
        conn.commit()


def scan_recent_metrics_for_incidents(
    db_path: str,
    *,
    since_metric_id: int = 0,
) -> tuple[int, int]:
    """Scan telemetry and persist threshold breaches and critical error signals."""
    if not db_path or not os.path.exists(db_path):
        return 0, since_metric_id

    thresholds = get_thresholds(db_path)
    cpu_threshold = float(thresholds["cpu"])
    memory_threshold = float(thresholds["memory"])
    recorded = 0
    max_metric_id = since_metric_id

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, timestamp, cpu, memory, labels, tenant_id
                FROM cluster_metrics
                WHERE id > ?
                ORDER BY id ASC
                """,
                (since_metric_id,),
            ).fetchall()

        for row in rows:
            metric_id = int(row["id"])
            max_metric_id = max(max_metric_id, metric_id)
            labels = _parse_labels(row["labels"])
            pod_name = str(labels.get("pod_name") or "unknown")
            namespace = str(labels.get("namespace") or "default")
            cluster_id = str(labels.get("cluster_id") or "omnikube-cluster")
            tenant_id = str(row["tenant_id"] or DEFAULT_TENANT_ID)
            timestamp = str(row["timestamp"])
            cpu = float(row["cpu"])
            memory = float(row["memory"])
            workload = derive_workload_name(
                pod_name,
                labels.get("kubernetes_labels") if isinstance(labels.get("kubernetes_labels"), dict) else None,
            )

            if cpu > cpu_threshold:
                record_incident_event(
                    db_path,
                    tenant_id=tenant_id,
                    timestamp=timestamp,
                    event_type="threshold_breach",
                    pod_name=pod_name,
                    namespace=namespace,
                    cluster_id=cluster_id,
                    metric="cpu",
                    value=cpu,
                    threshold=cpu_threshold,
                    severity="warning",
                    message=(
                        f"CPU threshold breach on {namespace}/{pod_name} "
                        f"({cpu:.1f}% > {cpu_threshold:.1f}%)"
                    ),
                )
                recorded += 1

            if memory > memory_threshold:
                record_incident_event(
                    db_path,
                    tenant_id=tenant_id,
                    timestamp=timestamp,
                    event_type="threshold_breach",
                    pod_name=pod_name,
                    namespace=namespace,
                    cluster_id=cluster_id,
                    metric="memory",
                    value=memory,
                    threshold=memory_threshold,
                    severity="warning",
                    message=(
                        f"Memory threshold breach on {namespace}/{pod_name} "
                        f"({memory:.1f}% > {memory_threshold:.1f}%)"
                    ),
                )
                recorded += 1

            if cpu >= CRITICAL_CPU_PCT or memory >= CRITICAL_MEMORY_PCT:
                record_incident_event(
                    db_path,
                    tenant_id=tenant_id,
                    timestamp=timestamp,
                    event_type="error_log",
                    pod_name=pod_name,
                    namespace=namespace,
                    cluster_id=cluster_id,
                    metric="cpu" if cpu >= CRITICAL_CPU_PCT else "memory",
                    value=cpu if cpu >= CRITICAL_CPU_PCT else memory,
                    threshold=CRITICAL_CPU_PCT if cpu >= CRITICAL_CPU_PCT else CRITICAL_MEMORY_PCT,
                    severity="critical",
                    message=(
                        f"Critical saturation on workload {workload} in {namespace}: "
                        f"CPU={cpu:.1f}% Memory={memory:.1f}%"
                    ),
                )
                recorded += 1

    except sqlite3.Error as exc:
        logger.error("Incident scan failed: %s", exc)

    if recorded:
        logger.info("Analytics engine recorded %d incident event(s)", recorded)
    return recorded, max_metric_id


def scan_unoptimized_cost_alerts(
    db_path: str,
    *,
    tenant_id: str = DEFAULT_TENANT_ID,
    last_alert_at: float | None = None,
) -> tuple[bool, float | None]:
    """Alert when unoptimized monthly infrastructure exposure exceeds the configured limit."""
    if not db_path or not os.path.exists(db_path):
        return False, last_alert_at

    limit_usd = get_cost_alert_limit_usd(db_path)
    report = build_cost_optimization_report(db_path, tenant_id)
    recommendations = report.get("recommendations") or []
    current_exposure = sum(float(item.get("current_monthly_usd", 0)) for item in recommendations)
    potential_savings = float(report.get("total_monthly_savings_usd", 0))

    if current_exposure <= limit_usd:
        return False, last_alert_at

    now = time.time()
    if last_alert_at is not None and (now - last_alert_at) < COST_ALERT_COOLDOWN_SEC:
        logger.info(
            "Cost alert suppressed by cooldown (exposure=%.2f limit=%.2f)",
            current_exposure,
            limit_usd,
        )
        return False, last_alert_at

    summary = (
        f"Unoptimized infrastructure exposure ${current_exposure:,.0f}/mo exceeds "
        f"limit ${limit_usd:,.0f}/mo"
    )
    detail = (
        f"{summary}\n"
        f"Idle workloads: {report.get('idle_workload_count', 0)}\n"
        f"Potential savings if optimized: ${potential_savings:,.0f}/mo\n"
        f"Provider model: {str(report.get('provider', 'aws')).upper()}"
    )
    remediation_context = build_cost_remediation_context(report)
    dispatch_cost_optimization_alert(
        db_path,
        summary,
        detail=detail,
        context=remediation_context,
    )
    record_incident_event(
        db_path,
        tenant_id=tenant_id,
        timestamp=time.strftime(TIMESTAMP_FORMAT, time.gmtime()),
        event_type="cost_exposure",
        cluster_id="omnikube-cluster",
        metric="cost_usd",
        value=current_exposure,
        threshold=limit_usd,
        severity="warning",
        message=summary,
    )
    logger.warning("[Analytics Engine] Cost exposure alert fired: %s", summary)
    return True, now


class AnalyticsEngine:
    """Background worker that continuously indexes telemetry into incident timelines."""

    def __init__(
        self,
        db_path: str,
        scan_interval_sec: int = SCAN_INTERVAL_SEC,
    ) -> None:
        self.db_path = db_path
        self.scan_interval_sec = scan_interval_sec
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_metric_id = 0
        self._last_cost_alert_at: float | None = None
        self._watermark_lock = threading.Lock()

    def start(self) -> None:
        init_incident_events(self.db_path)
        self._bootstrap_watermark()
        if self._thread is not None and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name="omnikube-analytics-engine",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Analytics engine started (incident scan interval=%ss)",
            self.scan_interval_sec,
        )

    def stop(self) -> None:
        self._running = False

    def _bootstrap_watermark(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute("SELECT MAX(id) FROM cluster_metrics").fetchone()
            if row and row[0] is not None:
                self._last_metric_id = int(row[0])
        except sqlite3.Error:
            self._last_metric_id = 0

    def run_once(self) -> int:
        with self._watermark_lock:
            since_id = self._last_metric_id
        recorded, max_id = scan_recent_metrics_for_incidents(
            self.db_path,
            since_metric_id=since_id,
        )
        with self._watermark_lock:
            self._last_metric_id = max(self._last_metric_id, max_id)

        fired, alert_at = scan_unoptimized_cost_alerts(
            self.db_path,
            last_alert_at=self._last_cost_alert_at,
        )
        if fired and alert_at is not None:
            self._last_cost_alert_at = alert_at

        predictive = scan_predictive_scale_up(self.db_path)
        if predictive:
            logger.info(
                "[Analytics Engine] Predictive scale-up evaluation generated %d recommendation(s)",
                len(predictive),
            )

        return recorded

    def _run_loop(self) -> None:
        while self._running:
            try:
                self.run_once()
            except Exception as exc:
                logger.error("Analytics engine loop error: %s", exc)
            time.sleep(self.scan_interval_sec)
