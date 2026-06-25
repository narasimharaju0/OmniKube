import json
import logging
import mimetypes
import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlparse

import stripe

# Server-side core modules (package: /app/core — not the standalone agent/ collector)
from core.auth import (
    ALL_ROLES,
    DEFAULT_TENANT_ID,
    EDITOR_ROLES,
    ROLE_ADMIN,
    SESSION_COOKIE_NAME,
    SESSION_TTL_SEC,
    authenticate_user,
    create_session,
    destroy_session,
    get_session_user,
    init_users,
    is_global_admin,
    login_required,
    provision_sso_user,
    register_user,
    resolve_query_tenant_id,
    resolve_session_organization_id,
    role_is_allowed,
)
from core.oauth2 import (
    OAuth2Config,
    build_authorization_url,
    consume_oauth_state,
    create_oauth_state,
    exchange_authorization_code,
    issue_simulated_authorization_code,
    render_simulated_login_page,
)
from core.system_config import (
    get_integrations,
    get_notification_config,
    get_thresholds,
    init_system_configs,
    sync_config_store_thresholds,
    update_integrations,
    update_notification_config,
    update_thresholds,
)
from core.tenant_branding import (
    get_tenant_branding,
    init_tenant_branding,
    upsert_tenant_branding,
)
from core.analytics_engine import AnalyticsEngine
from core.cloud_migration import execute_cross_cloud_migration
from core.compliance_ledger import (
    append_compliance_ledger_record,
    fetch_compliance_ledger,
    format_initiated_by,
    init_compliance_ledger,
)
from core.cost_optimizer import build_cost_optimization_report, calculate_all_cluster_savings
from core.billing import (
    BillingError,
    create_checkout_session,
    process_stripe_webhook_event,
)
from core.optimizer import analyze_rightsizing_recommendations, consume_override_token
from core.chatops_interactions import process_chatops_interaction
from core.remediation import apply_optimization_remediation
from core.telemetry_seed import seed_demo_telemetry
from core.incident_rca import build_incident_timeline
from core.alert_manager import AlertBuffer, AlertEvent, MultiChannelAlertDispatcher, resolve_webhook_channels
from core.discovery import DiscoveryService
from core.database import (
    CONFIG_PATH,
    Cluster,
    ClusterMetrics,
    CostRecommendations,
    DB_PATH,
    DEFAULT_ORGANIZATION_ID,
    ORM_DB_PATH,
    SQLALCHEMY_DATABASE_URI,
    ensure_data_directory,
    fetch_cluster_metrics_analytics,
    fetch_cluster_metrics_history,
    fetch_latest_cluster_metric,
    get_db,
    init_core_data_schemas,
    init_orm_tables,
    User,
)
from core.metrics_collector import MetricsCollector, ingest_metric_records, start_collection_loop

from sqlalchemy import desc

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def _configure_logging() -> None:
    """Configure structured production logging for the management gateway."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


logger = logging.getLogger("omnikube.server")

HOST = "0.0.0.0"
PORT = 5000
API_TOKEN = "premium_secret_2026"
METRICS_LIMIT = 30

TIMEFRAME_SQL_OFFSETS: dict[str, str] = {
    "1h": "-1 hour",
    "6h": "-6 hours",
    "12h": "-12 hours",
    "24h": "-24 hours",
}

DEFAULT_CONFIG: dict[str, Any] = {
    "slack_webhook_url": "",
    "discord_webhook_url": "",
    "cpu_alert_threshold": 80,
    "memory_alert_threshold": 80,
}

# POST routes that skip session/cookie auth (local load-test scripts, seeding).
PUBLIC_POST_ROUTES = frozenset({
    "/api/v1/telemetry/seed",
    "/api/v1/chatops/interactions",
    "/api/v1/billing/stripe/webhook",
})

MOCK_METRICS = [
    {
        "id": i,
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=(30 - i) * 5)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
        "cpu": round(18 + (i * 3.7) % 42, 1),
        "memory": round(52 + (i * 2.3) % 28, 1),
    }
    for i in range(1, 31)
]

_config_lock = threading.Lock()


class ConfigStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._data = dict(DEFAULT_CONFIG)

    def load(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self.save()
            return

        try:
            with open(self.path, encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                self._data = {**DEFAULT_CONFIG, **payload}
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Config load failed, using defaults: %s", exc)

    def save(self) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            with open(self.path, "w", encoding="utf-8") as handle:
                json.dump(self._data, handle, indent=2)
            return True
        except OSError as exc:
            logger.error("Config save failed: %s", exc)
            return False

    def get(self) -> dict[str, Any]:
        with _config_lock:
            return dict(self._data)

    def update(self, updates: dict[str, Any]) -> dict[str, Any]:
        with _config_lock:
            if "slack_webhook_url" in updates:
                self._data["slack_webhook_url"] = str(updates["slack_webhook_url"]).strip()
            if "discord_webhook_url" in updates:
                self._data["discord_webhook_url"] = str(updates["discord_webhook_url"]).strip()
            if "cpu_alert_threshold" in updates:
                self._data["cpu_alert_threshold"] = float(updates["cpu_alert_threshold"])
            if "memory_alert_threshold" in updates:
                self._data["memory_alert_threshold"] = float(updates["memory_alert_threshold"])
            self.save()
            return dict(self._data)


config_store = ConfigStore(CONFIG_PATH)
discovery = DiscoveryService()
alert_buffer = AlertBuffer(db_path=DB_PATH, config_getter=config_store.get)
metrics_collector = MetricsCollector(
    discovery,
    DB_PATH,
    config_getter=config_store.get,
    alert_buffer=alert_buffer,
)
analytics_engine = AnalyticsEngine(DB_PATH)

SAVINGS_ANALYSIS_INTERVAL_SEC = int(os.environ.get("OMNIKUBE_SAVINGS_INTERVAL_SEC", "120"))
_savings_analysis_running = False


def _serialize_orm_metric(metric: ClusterMetrics, cluster_name: str) -> dict[str, Any]:
    ts = metric.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    timestamp = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    cpu = round(float(metric.cpu_utilization), 2)
    memory = round(float(metric.memory_utilization), 2)
    return {
        "id": metric.id,
        "timestamp": timestamp,
        "cpu": cpu,
        "memory": memory,
        "cpu_utilization": cpu,
        "memory_utilization": memory,
        "node_count": int(metric.node_count),
        "active_pods": int(metric.active_pods),
        "cluster_id": metric.cluster_id,
        "cluster_name": cluster_name,
        "labels": {
            "source": "orm_cluster_metrics",
            "cluster_name": cluster_name,
            "cpu_utilization": cpu,
            "memory_utilization": memory,
            "running_pods": int(metric.active_pods),
            "ready_nodes": int(metric.node_count),
        },
        "granularity": "orm",
    }


def fetch_orm_cluster_metrics(
    *,
    limit: int = METRICS_LIMIT,
    timeframe: str | None = None,
) -> list[dict[str, Any]]:
    """Return dashboard-compatible metric rows from ORM ClusterMetrics."""
    sql_offset = TIMEFRAME_SQL_OFFSETS.get(timeframe or "")
    cutoff: datetime | None = None
    if sql_offset:
        cutoff = datetime.now(timezone.utc) + _parse_sqlite_offset(sql_offset)

    try:
        with get_db() as db:
            query = (
                db.query(ClusterMetrics, Cluster.cluster_name)
                .join(Cluster, ClusterMetrics.cluster_id == Cluster.id)
                .order_by(desc(ClusterMetrics.timestamp))
            )
            if cutoff is not None:
                query = query.filter(ClusterMetrics.timestamp >= cutoff)
            rows = query.limit(limit).all()
            return [
                _serialize_orm_metric(metric, cluster_name)
                for metric, cluster_name in rows
            ]
    except Exception as exc:
        logger.warning("ORM metrics query failed: %s", exc)
        return []


def fetch_orm_analytics() -> dict[str, Any] | None:
    """Aggregate analytics from ORM ClusterMetrics samples."""
    try:
        with get_db() as db:
            metrics = db.query(ClusterMetrics).all()
            if not metrics:
                return None
            cpus = [float(row.cpu_utilization) for row in metrics]
            memories = [float(row.memory_utilization) for row in metrics]
            return {
                "max_cpu": round(max(cpus), 1),
                "avg_cpu": round(sum(cpus) / len(cpus), 1),
                "max_memory": round(max(memories), 1),
                "avg_memory": round(sum(memories) / len(memories), 1),
                "sample_count": len(metrics),
                "source": "orm_cluster_metrics",
            }
    except Exception as exc:
        logger.warning("ORM analytics query failed: %s", exc)
        return None


def fetch_orm_latest_metric() -> dict[str, Any] | None:
    """Return the newest ORM ClusterMetrics row in dashboard format."""
    rows = fetch_orm_cluster_metrics(limit=1)
    return rows[0] if rows else None


def fetch_orm_cost_recommendations(*, status: str = "Active") -> list[dict[str, Any]]:
    """Return active CostRecommendations rows for the optimization feed."""
    try:
        with get_db() as db:
            rows = (
                db.query(CostRecommendations, Cluster.cluster_name, Cluster.provider)
                .join(Cluster, CostRecommendations.cluster_id == Cluster.id)
                .filter(CostRecommendations.status == status)
                .order_by(desc(CostRecommendations.created_at))
                .all()
            )
            recommendations: list[dict[str, Any]] = []
            for rec, cluster_name, provider in rows:
                recommendations.append(
                    {
                        "type": rec.type.lower().replace("-", "_").replace(" ", "_"),
                        "recommendation_type": rec.type,
                        "cluster_id": rec.cluster_id,
                        "cluster_name": cluster_name,
                        "provider": provider,
                        "monthly_savings_usd": round(float(rec.potential_savings), 2),
                        "current_monthly_cost": round(float(rec.current_monthly_cost), 2),
                        "projected_monthly_cost": round(float(rec.projected_monthly_cost), 2),
                        "potential_savings": round(float(rec.potential_savings), 2),
                        "status": rec.status,
                        "source": "orm_cost_optimizer",
                        "rationale": (
                            f"{rec.type} detected for cluster '{cluster_name}': "
                            f"project monthly spend from ${rec.current_monthly_cost:.2f} "
                            f"to ${rec.projected_monthly_cost:.2f}."
                        ),
                    }
                )
            return recommendations
    except Exception as exc:
        logger.warning("ORM cost recommendations query failed: %s", exc)
        return []


def _merge_orm_recommendations(report: dict[str, Any]) -> dict[str, Any]:
    """Merge ORM cost recommendations into an optimization API report payload."""
    orm_recs = fetch_orm_cost_recommendations()
    if not orm_recs:
        return report

    merged = dict(report)
    existing = list(merged.get("recommendations") or [])
    merged["recommendations"] = orm_recs + existing
    orm_savings = sum(float(item.get("monthly_savings_usd") or 0) for item in orm_recs)
    merged["total_monthly_savings_usd"] = round(
        float(merged.get("total_monthly_savings_usd") or 0) + orm_savings,
        2,
    )
    merged["orm_recommendation_count"] = len(orm_recs)
    return merged


def start_savings_analysis_loop(interval_seconds: int = SAVINGS_ANALYSIS_INTERVAL_SEC) -> None:
    """Background FinOps calculator — persists CostRecommendations from ORM metrics."""
    global _savings_analysis_running

    if _savings_analysis_running:
        return

    _savings_analysis_running = True

    def _loop() -> None:
        while _savings_analysis_running:
            try:
                saved = calculate_all_cluster_savings()
                if saved:
                    logger.info("Savings analysis complete: %s recommendation(s) updated", len(saved))
            except Exception as exc:
                logger.warning("Savings analysis cycle failed: %s", exc)
            time.sleep(max(30, int(interval_seconds)))

    thread = threading.Thread(
        target=_loop,
        name="omnikube-savings-analysis",
        daemon=True,
    )
    thread.start()
    logger.info("Savings analysis loop started (interval=%ss)", interval_seconds)


def seed_mock_metrics_if_empty() -> None:
    try:
        init_core_data_schemas(DB_PATH)
        with sqlite3.connect(DB_PATH) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM cluster_metrics WHERE organization_id = ?",
                (DEFAULT_ORGANIZATION_ID,),
            ).fetchone()[0]
            if count == 0:
                conn.executemany(
                    """
                    INSERT INTO cluster_metrics (
                        timestamp, cpu, memory, labels, granularity, tenant_id, organization_id
                    )
                    VALUES (?, ?, ?, ?, 'raw', ?, ?)
                    """,
                    [
                        (
                            row["timestamp"],
                            row["cpu"],
                            row["memory"],
                            "{}",
                            DEFAULT_TENANT_ID,
                            DEFAULT_ORGANIZATION_ID,
                        )
                        for row in MOCK_METRICS
                    ],
                )
                conn.commit()
    except sqlite3.Error as exc:
        logger.warning("Database seed failed: %s", exc)


def _filter_mock_metrics_by_timeframe(timeframe: str | None, limit: int) -> list[dict[str, Any]]:
    rows = list(MOCK_METRICS)
    sql_offset = TIMEFRAME_SQL_OFFSETS.get(timeframe or "")
    if not sql_offset:
        return rows[:limit]

    cutoff = datetime.now(timezone.utc) + _parse_sqlite_offset(sql_offset)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        try:
            ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts >= cutoff:
            filtered.append(row)

    filtered.sort(key=lambda item: int(item["id"]), reverse=True)
    return filtered[:limit] if limit else filtered


def _parse_sqlite_offset(offset: str) -> timedelta:
    parts = offset.strip().split()
    if len(parts) != 2:
        return timedelta()

    amount = int(parts[0].lstrip("-"))
    unit = parts[1].lower().rstrip("s")
    if unit == "hour":
        return timedelta(hours=-amount)
    if unit == "minute":
        return timedelta(minutes=-amount)
    if unit == "day":
        return timedelta(days=-amount)
    return timedelta()


def fetch_metrics(
    organization_id: str,
    *,
    limit: int = METRICS_LIMIT,
    timeframe: str | None = None,
) -> list[dict[str, Any]]:
    orm_rows = fetch_orm_cluster_metrics(limit=limit, timeframe=timeframe)
    if orm_rows:
        return orm_rows

    sql_offset = TIMEFRAME_SQL_OFFSETS.get(timeframe or "")
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID

    try:
        if not os.path.exists(DB_PATH):
            return _filter_mock_metrics_by_timeframe(timeframe, limit)

        rows = fetch_cluster_metrics_history(
            DB_PATH,
            org_id,
            limit=limit,
            timeframe_sql_offset=sql_offset or None,
        )

        if not rows:
            return _filter_mock_metrics_by_timeframe(timeframe, limit)

        return rows
    except (sqlite3.Error, OSError) as exc:
        logger.warning("Metrics query failed, using fallback data: %s", exc)
        return _filter_mock_metrics_by_timeframe(timeframe, limit)


def fetch_analytics(organization_id: str) -> dict[str, Any]:
    orm_analytics = fetch_orm_analytics()
    if orm_analytics is not None:
        return orm_analytics

    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID

    try:
        if not os.path.exists(DB_PATH):
            cpus = [float(row["cpu"]) for row in MOCK_METRICS]
            memories = [float(row["memory"]) for row in MOCK_METRICS]
            return {
                "max_cpu": round(max(cpus), 1),
                "avg_cpu": round(sum(cpus) / len(cpus), 1),
                "max_memory": round(max(memories), 1),
                "avg_memory": round(sum(memories) / len(memories), 1),
                "sample_count": len(MOCK_METRICS),
            }

        analytics = fetch_cluster_metrics_analytics(DB_PATH, org_id)
        if analytics["sample_count"] == 0:
            cpus = [float(m["cpu"]) for m in MOCK_METRICS]
            memories = [float(m["memory"]) for m in MOCK_METRICS]
            return {
                "max_cpu": round(max(cpus), 1),
                "avg_cpu": round(sum(cpus) / len(cpus), 1),
                "max_memory": round(max(memories), 1),
                "avg_memory": round(sum(memories) / len(memories), 1),
                "sample_count": len(MOCK_METRICS),
            }
        return analytics
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        logger.warning("Analytics query failed, using fallback data: %s", exc)
        cpus = [float(m["cpu"]) for m in MOCK_METRICS]
        memories = [float(m["memory"]) for m in MOCK_METRICS]
        return {
            "max_cpu": round(max(cpus), 1),
            "avg_cpu": round(sum(cpus) / len(cpus), 1),
            "max_memory": round(max(memories), 1),
            "avg_memory": round(sum(memories) / len(memories), 1),
            "sample_count": len(MOCK_METRICS),
        }


def fetch_latest_metric_row(organization_id: str) -> dict[str, Any] | None:
    orm_latest = fetch_orm_latest_metric()
    if orm_latest is not None:
        return orm_latest

    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID

    try:
        if not os.path.exists(DB_PATH):
            return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None

        row = fetch_latest_cluster_metric(DB_PATH, org_id)
        if row is None:
            return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None
        return row
    except (sqlite3.Error, OSError) as exc:
        logger.warning("Latest metric query failed, using fallback row: %s", exc)
        return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None


def _post_json(url: str, payload: dict[str, Any]) -> bool:
    if not url:
        return False

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
        logger.info("Webhook alert delivered to %s...", url[:48])
        return True
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.error("Webhook delivery failed: %s", exc)
        return False


MOCK_WEBHOOK_PAYLOAD: dict[str, Any] = {
    "status": "TEST",
    "message": "OmniKube webhook connectivity test",
    "cpu": 42.5,
}


def dispatch_test_webhooks() -> dict[str, Any]:
    channels = resolve_webhook_channels(config_store.get)
    dispatcher = MultiChannelAlertDispatcher(DB_PATH, config_store.get)
    payload = {
        **MOCK_WEBHOOK_PAYLOAD,
        "summary": MOCK_WEBHOOK_PAYLOAD["message"],
        "cluster_id": "omnikube-cluster",
        "workload_count": 1,
        "workloads": [],
    }
    dispatcher.dispatch_grouped_alert_async(payload["summary"], payload)

    results = {
        channel: "queued" if url else "not_configured"
        for channel, url in channels.items()
    }
    logger.info("Test webhook dispatch queued via MultiChannelAlertDispatcher")
    return {"status": "ok", "payload": MOCK_WEBHOOK_PAYLOAD, "results": results}


def _resolve_organization_id(
    user: dict[str, Any] | None,
    query_params: dict[str, list[str]] | None = None,
) -> str:
    """Resolve corporate organization scope for strict data isolation."""
    if user is None:
        return DEFAULT_ORGANIZATION_ID
    params = query_params or {}
    if is_global_admin(user):
        requested = params.get("organization_id", [None])[0]
        if requested:
            return str(requested).strip()
    return resolve_session_organization_id(user)


def _record_optimization_audit(
    *,
    organization_id: str,
    tenant_id: str,
    user: dict[str, Any],
    action_taken: str,
    original_cost_usd: float,
    optimized_cost_usd: float,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pipe a synchronous compliance vault entry for an optimization action."""
    return append_compliance_ledger_record(
        DB_PATH,
        organization_id=organization_id,
        tenant_id=tenant_id,
        action_taken=action_taken,
        initiated_by=format_initiated_by(user),
        original_cost_usd=original_cost_usd,
        optimized_cost_usd=optimized_cost_usd,
        metadata=metadata,
    )


def inject_session_bootstrap(html: str, user: dict[str, Any]) -> str:
    """Inject authenticated session context for protected SPA pages."""
    bootstrap = {
        "username": user.get("username"),
        "role": user.get("role"),
        "tenant_id": user.get("tenant_id"),
        "organization_id": resolve_session_organization_id(user),
        "email": user.get("email"),
        "display_name": user.get("display_name"),
        "auth_provider": user.get("auth_provider"),
    }
    script = (
        '<script id="omnikube-session-bootstrap">'
        f"window.__OMNIKUBE_SESSION__={json.dumps(bootstrap)};"
        "</script>"
    )
    if "</head>" in html:
        return html.replace("</head>", f"{script}\n</head>", 1)
    return f"{script}\n{html}"


def _resolve_effective_tenant_id(
    user: dict[str, Any],
    query_params: dict[str, list[str]] | None = None,
) -> str:
    params = query_params or {}
    if is_global_admin(user):
        requested = params.get("tenant_id", [None])[0]
        if requested:
            return str(requested).strip()
        return str(user.get("tenant_id", DEFAULT_TENANT_ID))
    scoped = resolve_query_tenant_id(user)
    return scoped or str(user.get("tenant_id", DEFAULT_TENANT_ID))


def _extract_tenant_id_from_request(
    headers: Any,
    payload: dict[str, Any] | None,
) -> str | None:
    for header_name in ("Tenant-ID", "X-Tenant-ID", "X-Tenant-Id"):
        header_value = headers.get(header_name)
        if header_value:
            return str(header_value).strip()
    if payload and payload.get("tenant_id"):
        return str(payload["tenant_id"]).strip()
    return None


def _resolve_ingest_tenant_id(
    user: dict[str, Any],
    requested_tenant_id: str | None,
) -> str:
    if is_global_admin(user):
        return requested_tenant_id or str(user.get("tenant_id", DEFAULT_TENANT_ID))

    user_tenant = str(user.get("tenant_id", DEFAULT_TENANT_ID))
    if requested_tenant_id and requested_tenant_id != user_tenant:
        raise PermissionError("Cannot ingest metrics for another tenant.")
    return user_tenant


def _normalize_ingest_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("metrics"), list):
        return [record for record in payload["metrics"] if isinstance(record, dict)]

    if "cpu" in payload or "memory" in payload:
        return [payload]

    raise ValueError("Payload must include a 'metrics' array or top-level cpu/memory fields.")


def maybe_trigger_cpu_alert(
    organization_id: str,
    _metrics: list[dict[str, Any]] | None = None,
) -> None:
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    latest = fetch_latest_metric_row(org_id)
    if latest is None:
        return

    cpu_utilization = float(latest.get("cpu", 0))
    memory = float(latest.get("memory", 0))
    thresholds = get_thresholds(DB_PATH, org_id)
    cpu_threshold = thresholds["cpu"]
    memory_threshold = thresholds["memory"]

    labels_raw = latest.get("labels", "{}")
    try:
        labels_payload = json.loads(labels_raw) if isinstance(labels_raw, str) else dict(labels_raw or {})
    except json.JSONDecodeError:
        labels_payload = {}

    cluster_id = str(labels_payload.get("cluster_id", "omnikube-cluster"))
    node_id = str(labels_payload.get("node_id", labels_payload.get("ip", "unknown")))
    pod_name = str(labels_payload.get("pod_name", "unknown"))
    namespace = str(labels_payload.get("namespace", "default"))

    if cpu_utilization > cpu_threshold:
        alert_buffer.capture(
            AlertEvent(
                cluster_id=cluster_id,
                node_id=node_id,
                pod_name=pod_name,
                namespace=namespace,
                metric="cpu",
                value=cpu_utilization,
                threshold=cpu_threshold,
                tenant_id=str(latest.get("tenant_id", DEFAULT_TENANT_ID)),
            )
        )

    if memory > memory_threshold:
        alert_buffer.capture(
            AlertEvent(
                cluster_id=cluster_id,
                node_id=node_id,
                pod_name=pod_name,
                namespace=namespace,
                metric="memory",
                value=memory,
                threshold=memory_threshold,
                tenant_id=str(latest.get("tenant_id", DEFAULT_TENANT_ID)),
            )
        )


DASHBOARD_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "dashboard.html")
OPTIMIZATION_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "optimization.html")
ADMIN_SETTINGS_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "admin_settings.html")
INDEX_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "index.html")
LOGIN_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "login.html")
PUBLIC_PAGE_ROUTES: dict[str, str] = {
    "/": INDEX_TEMPLATE_PATH,
    "/login": LOGIN_TEMPLATE_PATH,
}
PAGE_TEMPLATE_ROUTES: dict[str, str] = {
    "/dashboard": DASHBOARD_TEMPLATE_PATH,
    "/cost-optimization": OPTIMIZATION_TEMPLATE_PATH,
    "/admin/settings": ADMIN_SETTINGS_TEMPLATE_PATH,
}
PROTECTED_PAGE_PATHS = frozenset({"/dashboard", "/cost-optimization", "/admin/settings"})
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
STATIC_CATCHALL_EXTENSIONS = (".js", ".css", ".png", ".jpg", ".jpeg", ".ico", ".svg", ".json")

DEFAULT_WIDGET_LAYOUT: list[dict[str, Any]] = [
    {"id": "cpu-chart", "x": 0, "y": 0, "w": 6, "h": 4},
    {"id": "memory-chart", "x": 6, "y": 0, "w": 6, "h": 4},
    {"id": "alert-buffer", "x": 0, "y": 4, "w": 12, "h": 3},
    {"id": "notification-channels", "x": 0, "y": 7, "w": 12, "h": 4},
]


def init_widget_layout_schema() -> None:
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS widget_layouts (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    layout_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        logger.info("Widget layout schema ready")
    except sqlite3.Error as exc:
        logger.error("Widget layout schema failed: %s", exc)


def load_dashboard_html() -> str:
    try:
        with open(DASHBOARD_TEMPLATE_PATH, encoding="utf-8") as handle:
            return handle.read()
    except OSError as exc:
        logger.error("Dashboard template load failed: %s", exc)
        return "<h1>Dashboard unavailable</h1>"


def load_page_html(path: str) -> str | None:
    template_path = PAGE_TEMPLATE_ROUTES.get(path) or PUBLIC_PAGE_ROUTES.get(path)
    if template_path is None:
        return None
    try:
        with open(template_path, encoding="utf-8") as handle:
            return handle.read()
    except OSError as exc:
        logger.error("Page template load failed (%s): %s", path, exc)
        return None


def load_widget_layout() -> list[dict[str, Any]]:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT layout_json FROM widget_layouts WHERE id = 1"
            ).fetchone()
        if not row:
            return list(DEFAULT_WIDGET_LAYOUT)
        layout = json.loads(row[0])
        return layout if isinstance(layout, list) else list(DEFAULT_WIDGET_LAYOUT)
    except (sqlite3.Error, json.JSONDecodeError, OSError) as exc:
        logger.warning("Widget layout load failed, using defaults: %s", exc)
        return list(DEFAULT_WIDGET_LAYOUT)


def save_widget_layout(layout: list[dict[str, Any]]) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    layout_json = json.dumps(layout, indent=2)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO widget_layouts (id, layout_json, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                layout_json = excluded.layout_json,
                updated_at = excluded.updated_at
            """,
            (layout_json, timestamp),
        )
        conn.commit()
    logger.info("Widget layout persisted (%s widgets) at %s", len(layout), timestamp)
    for widget in layout:
        logger.debug(
            "  widget=%s x=%s y=%s w=%s h=%s",
            widget.get("id"), widget.get("x"), widget.get("y"), widget.get("w"), widget.get("h"),
        )




class ManagementHandler(BaseHTTPRequestHandler):
    def _read_json_body(self) -> dict[str, Any] | None:
        raw = self._read_raw_body()
        if not raw:
            return {}

        try:
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _read_raw_body(self) -> bytes:
        try:
            length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return b""
        if length <= 0:
            return b""
        try:
            return self.rfile.read(length)
        except OSError:
            return b""

    def _send_json(
        self,
        status: int,
        payload: object,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        try:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            if extra_headers:
                for header_name, header_value in extra_headers.items():
                    self.send_header(header_name, header_value)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _cookie_value(self, name: str) -> str | None:
        raw = self.headers.get("Cookie", "")
        if not raw:
            return None
        prefix = f"{name}="
        for part in raw.split(";"):
            part = part.strip()
            if part.startswith(prefix):
                return part[len(prefix) :]
        return None

    def _session_cookie_header(self, token: str) -> str:
        return (
            f"{SESSION_COOKIE_NAME}={token}; "
            f"Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SEC}"
        )

    def _clear_session_cookie_header(self) -> str:
        return (
            f"{SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; "
            "Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
        )

    def _resolve_session_token(self) -> str | None:
        cookie_token = self._cookie_value(SESSION_COOKIE_NAME)
        if cookie_token:
            return cookie_token

        auth_header = self.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            bearer_token = auth_header[7:].strip()
            if bearer_token:
                return bearer_token
        return None

    def _resolve_current_user(self) -> dict[str, Any] | None:
        session_user = get_session_user(DB_PATH, self._resolve_session_token())
        if session_user:
            return session_user
        if self.headers.get("X-OmniKube-Token") == API_TOKEN:
            return {
                "id": 0,
                "username": "api_token",
                "role": ROLE_ADMIN,
                "tenant_id": "system",
                "organization_id": DEFAULT_ORGANIZATION_ID,
                "email": "",
                "display_name": "API Token",
                "auth_provider": "token",
            }
        return None

    def _require_user(self, allowed_roles: set[str] | None = None) -> dict[str, Any] | None:
        user = self._resolve_current_user()
        if user is None:
            self._send_json(401, {"error": "Unauthorized. Please sign in."})
            return None
        if allowed_roles is not None and not role_is_allowed(str(user["role"]), allowed_roles):
            self._send_json(
                403,
                {
                    "error": "Forbidden. Your role does not have permission for this action.",
                    "role": user["role"],
                    "required_roles": sorted(allowed_roles),
                },
            )
            return None
        return user

    def _handle_auth_login(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        username = str(payload.get("username") or payload.get("email") or "").strip()
        password = str(payload.get("password", ""))
        if not username or not password:
            self._send_json(400, {"error": "Username or email and password are required."})
            return

        auth_result = authenticate_user(DB_PATH, username, password)
        if auth_result is None:
            self._send_json(401, {"error": "Invalid username or password."})
            return

        user = auth_result["user"]
        token = auth_result["token"]
        logger.info("User signed in: %s (%s)", user["username"], user["role"])
        self._send_json(
            200,
            {
                "success": True,
                "status": "success",
                "token": token,
                "user": user,
            },
            extra_headers={
                "Set-Cookie": self._session_cookie_header(token),
                "Cache-Control": "no-store, no-cache, must-revalidate",
            },
        )

    def _handle_auth_logout(self) -> None:
        token = self._resolve_session_token()
        destroy_session(DB_PATH, token)
        self._send_json(
            200,
            {"status": "ok"},
            extra_headers={"Set-Cookie": self._clear_session_cookie_header()},
        )

    def _handle_auth_me(self) -> None:
        user = self._resolve_current_user()
        if user is None:
            self._send_json(401, {"error": "Unauthorized. Please sign in."})
            return
        self._send_json(
            200,
            {
                "success": True,
                "status": "success",
                "user": user,
            },
            extra_headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    def _resolve_orm_user_id(self, session_user: dict[str, Any]) -> int | None:
        """Map the active session to an ORM User.id (email-based accounts only)."""
        email = str(session_user.get("email") or session_user.get("username") or "").strip().lower()
        if not email or "@" not in email:
            return None
        try:
            with get_db() as db:
                orm_user = db.query(User).filter(User.email == email).one_or_none()
                return int(orm_user.id) if orm_user is not None else None
        except Exception as exc:
            logger.warning("ORM user lookup failed for %s: %s", email, exc)
            return None

    def _handle_billing_checkout(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        plan_type = str(payload.get("plan_type") or payload.get("plan") or "").strip()
        if not plan_type:
            self._send_json(400, {"error": "plan_type is required (developer, growth, or enterprise)."})
            return

        user_id = payload.get("user_id")
        if user_id is not None:
            try:
                resolved_user_id = int(user_id)
            except (TypeError, ValueError):
                self._send_json(400, {"error": "user_id must be an integer."})
                return
        else:
            resolved_user_id = self._resolve_orm_user_id(user)
            if resolved_user_id is None:
                self._send_json(
                    400,
                    {
                        "error": (
                            "Billing checkout requires an ORM-registered account. "
                            "Sign in with your email or pass a valid user_id."
                        ),
                    },
                )
                return

        try:
            checkout_url = create_checkout_session(resolved_user_id, plan_type)
        except BillingError as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except Exception as exc:
            logger.error("Stripe checkout failed: %s", exc)
            self._send_json(500, {"error": "Unable to create Stripe checkout session."})
            return

        self._send_json(
            200,
            {
                "status": "ok",
                "checkout_url": checkout_url,
                "plan_type": plan_type,
                "user_id": resolved_user_id,
            },
        )

    def _handle_billing_stripe_webhook(self) -> None:
        payload = self._read_raw_body()
        sig_header = self.headers.get("Stripe-Signature", "")

        if not payload:
            self._send_json(400, {"error": "Webhook payload is empty."})
            return
        if not sig_header:
            self._send_json(400, {"error": "Missing Stripe-Signature header."})
            return

        webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
        if not webhook_secret:
            logger.error("STRIPE_WEBHOOK_SECRET is not configured")
            self._send_json(503, {"error": "Webhook verification is not configured."})
            return

        stripe_api_key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
        if stripe_api_key:
            stripe.api_key = stripe_api_key

        try:
            event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
        except ValueError as exc:
            logger.warning("Rejected Stripe webhook with invalid payload: %s", exc)
            self._send_json(400, {"error": "Invalid webhook payload."})
            return
        except stripe.error.SignatureVerificationError:
            logger.warning("Rejected Stripe webhook with invalid signature")
            self._send_json(400, {"error": "Invalid Stripe webhook signature."})
            return

        try:
            result = process_stripe_webhook_event(event)
        except BillingError as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except Exception as exc:
            logger.error("Stripe webhook processing failed: %s", exc, exc_info=True)
            self._send_json(500, {"error": "Webhook processing failed."})
            return

        self._send_json(200, result)

    def _handle_metrics(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        timeframe = params.get("timeframe", [None])[0]
        organization_id = _resolve_organization_id(user, params)
        metrics = fetch_metrics(organization_id, limit=METRICS_LIMIT, timeframe=timeframe)
        maybe_trigger_cpu_alert(organization_id, metrics)
        self._send_json(200, metrics)

    def _handle_metrics_ingest(self) -> None:
        user = self._require_user(EDITOR_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        try:
            params = parse_qs(urlparse(self.path).query)
            requested_tenant = _extract_tenant_id_from_request(self.headers, payload)
            tenant_id = _resolve_ingest_tenant_id(user, requested_tenant)
            organization_id = _resolve_organization_id(user, params)
            if payload.get("organization_id") and is_global_admin(user):
                organization_id = str(payload["organization_id"]).strip()
            records = _normalize_ingest_records(payload)
            if not records:
                raise ValueError("At least one metric record is required.")

            inserted = ingest_metric_records(
                DB_PATH,
                tenant_id,
                records,
                organization_id=organization_id,
            )
            logger.info(
                "Ingested %s metric row(s) for tenant=%s organization=%s",
                inserted, tenant_id, organization_id,
            )
            self._send_json(
                200,
                {
                    "status": "ok",
                    "tenant_id": tenant_id,
                    "organization_id": organization_id,
                    "inserted": inserted,
                },
            )
        except PermissionError as exc:
            self._send_json(403, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _send_html(self, html: str) -> None:
        try:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _redirect(self, location: str, *, status: int = 302) -> None:
        try:
            self.send_response(status)
            self.send_header("Location", location)
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _request_base_url(self) -> str:
        host = self.headers.get("Host", f"localhost:{PORT}")
        scheme = self.headers.get("X-Forwarded-Proto", "http")
        return f"{scheme}://{host}"

    def _safe_return_path(self, raw_path: str | None) -> str:
        if not raw_path:
            return "/dashboard"
        path = str(raw_path).strip()
        if not path.startswith("/") or path.startswith("//"):
            return "/dashboard"
        if "api" in path:
            return "/dashboard"
        return path

    def _require_page_session(self, full_path: str) -> dict[str, Any] | None:
        user = self._resolve_current_user()
        if user is not None:
            return user
        params = parse_qs(urlparse(self.path).query)
        return_to = params.get("return_to", [full_path])[0]
        redirect_target = (
            f"/auth/login?return_to={quote(self._safe_return_path(return_to))}"
        )
        self._redirect(redirect_target)
        return None

    def _send_protected_html(self, html: str, user: dict[str, Any]) -> None:
        self._send_html(inject_session_bootstrap(html, user))

    def _handle_oauth_login(self) -> None:
        """Start the OAuth2/OIDC authorization code flow."""
        params = parse_qs(urlparse(self.path).query)
        return_to = self._safe_return_path(params.get("return_to", ["/dashboard"])[0])
        config = OAuth2Config.from_request_base(self._request_base_url())
        state, nonce = create_oauth_state(return_to=return_to)
        authorization_url = build_authorization_url(config, state=state, nonce=nonce)
        logger.info(
            "OAuth login redirect (simulated=%s) -> %s...",
            config.simulated, authorization_url[:96],
        )
        self._redirect(authorization_url)

    def _handle_oauth_callback(self) -> None:
        """Complete OAuth2/OIDC login and bind organization scope to the session cookie."""
        params = parse_qs(urlparse(self.path).query)
        error = (params.get("error") or [None])[0]
        if error:
            description = (params.get("error_description") or ["Authentication failed."])[0]
            self._send_json(400, {"error": str(description), "oauth_error": str(error)})
            return

        code = (params.get("code") or [None])[0]
        state = (params.get("state") or [None])[0]
        if not code or not state:
            self._send_json(400, {"error": "Missing OAuth authorization code or state."})
            return

        state_entry = consume_oauth_state(state)
        if state_entry is None:
            self._send_json(400, {"error": "OAuth state is invalid or expired."})
            return

        config = OAuth2Config.from_request_base(self._request_base_url())
        try:
            profile = exchange_authorization_code(config, code=str(code), state=str(state))
            user = provision_sso_user(
                DB_PATH,
                email=str(profile["email"]),
                name=str(profile.get("name") or profile["email"]),
                organization_id=str(profile["organization_id"]),
            )
            user["auth_provider"] = str(profile.get("auth_provider") or "sso")
            init_system_configs(DB_PATH, user["organization_id"])
            token = create_session(DB_PATH, user)
        except (ValueError, urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
            logger.error("OAuth callback failed: %s", exc)
            self._send_json(400, {"error": f"OAuth sign-in failed: {exc}"})
            return

        return_to = self._safe_return_path(str(state_entry.get("return_to", "/dashboard")))
        logger.info(
            "OAuth SSO session created for %s (organization_id=%s)",
            user["email"],
            user["organization_id"],
        )
        try:
            self.send_response(302)
            self.send_header("Location", return_to)
            self.send_header("Set-Cookie", self._session_cookie_header(token))
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _handle_sso_authorize_get(self) -> None:
        """Simulated enterprise IdP login screen."""
        query = urlparse(self.path).query
        params = parse_qs(urlparse(self.path).query)
        error = (params.get("error") or [""])[0]
        html = render_simulated_login_page(
            authorize_query=f"?{query}" if query else "",
            error_message=str(error),
        )
        self._send_html(html)

    def _handle_sso_authorize_post(self) -> None:
        """Simulated enterprise IdP credential submission."""
        raw = self._read_raw_body().decode("utf-8")
        form = urllib.parse.parse_qs(raw, keep_blank_values=True)
        email = (form.get("email") or [""])[0].strip()
        name = (form.get("name") or [""])[0].strip()
        state = (form.get("state") or [""])[0]
        redirect_uri = (form.get("redirect_uri") or [""])[0]
        nonce = (form.get("nonce") or [""])[0]

        if not email or "@" not in email:
            html = render_simulated_login_page(
                authorize_query=f"?{urlencode({'state': state, 'redirect_uri': redirect_uri, 'nonce': nonce})}",
                error_message="A valid work email address is required.",
            )
            self._send_html(html)
            return

        code = issue_simulated_authorization_code(
            email=email,
            name=name,
            redirect_uri=redirect_uri,
            state=state,
            nonce=nonce,
        )
        callback = f"{redirect_uri}?{urlencode({'code': code, 'state': state})}"
        self._redirect(callback)

    def _handle_settings_get(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        settings = config_store.get()
        thresholds = get_thresholds(DB_PATH, organization_id)
        notifications = get_notification_config(DB_PATH, organization_id)
        settings["cpu_alert_threshold"] = thresholds["cpu"]
        settings["memory_alert_threshold"] = thresholds["memory"]
        settings.update(notifications)
        self._send_json(200, settings)

    def _validate_threshold(self, value: object, label: str) -> float:
        threshold = float(value)  # type: ignore[arg-type]
        if not 1 <= threshold <= 100:
            raise ValueError(f"{label} threshold must be between 1 and 100.")
        return threshold

    def _handle_thresholds_get(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        self._send_json(200, get_thresholds(DB_PATH, organization_id))

    def _handle_thresholds_post(self) -> None:
        user = self._require_user({ROLE_ADMIN})
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)

        try:
            current = get_thresholds(DB_PATH, organization_id)
            cpu = self._validate_threshold(
                payload.get("cpu", current["cpu"]),
                "CPU",
            )
            memory = self._validate_threshold(
                payload.get("memory", current["memory"]),
                "Memory",
            )
            updated = update_thresholds(DB_PATH, cpu, memory, organization_id)
            sync_config_store_thresholds(config_store, updated)
            logger.info(
                "Thresholds hot-reloaded: cpu=%.1f memory=%.1f",
                updated["cpu"], updated["memory"],
            )
            self._send_json(200, {"status": "ok", "cpu": updated["cpu"], "memory": updated["memory"]})
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_integrations_get(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        self._send_json(200, get_integrations(DB_PATH, organization_id))

    def _handle_integrations_post(self) -> None:
        user = self._require_user({ROLE_ADMIN})
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)

        try:
            slack = payload.get("slack") or {}
            discord = payload.get("discord") or {}
            if isinstance(slack, dict) and slack.get("enabled") and not str(slack.get("webhook_url", "")).strip():
                raise ValueError("Slack is enabled but no webhook URL was provided.")
            if isinstance(discord, dict) and discord.get("enabled") and not str(discord.get("webhook_url", "")).strip():
                raise ValueError("Discord is enabled but no webhook URL was provided.")
            updated = update_integrations(DB_PATH, payload, organization_id)
            config_store.update(
                {
                    "slack_webhook_url": updated["slack"]["webhook_url"],
                    "discord_webhook_url": updated["discord"]["webhook_url"],
                }
            )
            logger.info("Notification integrations hot-reloaded from dashboard")
            self._send_json(200, {"status": "ok", **updated})
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_notifications_settings_get(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return
        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        self._send_json(
            200,
            {"status": "ok", "notifications": get_integrations(DB_PATH, organization_id)},
        )

    def _handle_notifications_settings_post(self) -> None:
        user = self._require_user({ROLE_ADMIN})
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)

        try:
            body = payload.get("notifications") if isinstance(payload.get("notifications"), dict) else payload
            slack = body.get("slack") or {}
            discord = body.get("discord") or {}
            if isinstance(slack, dict) and slack.get("enabled") and not str(slack.get("webhook_url", "")).strip():
                raise ValueError("Slack is enabled but no webhook URL was provided.")
            if isinstance(discord, dict) and discord.get("enabled") and not str(discord.get("webhook_url", "")).strip():
                raise ValueError("Discord is enabled but no webhook URL was provided.")
            updated = update_integrations(DB_PATH, body, organization_id)
            config_store.update(
                {
                    "slack_webhook_url": updated["slack"]["webhook_url"],
                    "discord_webhook_url": updated["discord"]["webhook_url"],
                }
            )
            logger.info("Notification settings hot-reloaded via /api/v1/settings/notifications")
            self._send_json(200, {"status": "ok", "notifications": updated})
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_settings_post(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)

        try:
            if "cpu_alert_threshold" in payload:
                threshold = float(payload["cpu_alert_threshold"])
                if not 1 <= threshold <= 100:
                    raise ValueError("CPU threshold must be between 1 and 100.")
            if "memory_alert_threshold" in payload:
                threshold = float(payload["memory_alert_threshold"])
                if not 1 <= threshold <= 100:
                    raise ValueError("Memory threshold must be between 1 and 100.")
            updated = config_store.update(payload)
            notification_updates = {
                key: updated[key]
                for key in (
                    "slack_webhook_url",
                    "discord_webhook_url",
                )
                if key in payload
            }
            if notification_updates:
                update_notification_config(DB_PATH, notification_updates, organization_id)
            if "cpu_alert_threshold" in payload or "memory_alert_threshold" in payload:
                thresholds = get_thresholds(DB_PATH, organization_id)
                cpu = float(updated.get("cpu_alert_threshold", thresholds["cpu"]))
                memory = float(updated.get("memory_alert_threshold", thresholds["memory"]))
                update_thresholds(DB_PATH, cpu, memory, organization_id)
            self._send_json(200, updated)
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_targets(self) -> None:
        if self._require_user(ALL_ROLES) is None:
            return

        self._send_json(200, discovery.get_active_targets())

    def _handle_analytics(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        self._send_json(200, fetch_analytics(organization_id))

    def _handle_test_webhook(self) -> None:
        if self._require_user(ALL_ROLES) is None:
            return

        result = dispatch_test_webhooks()
        if "error" in result and not result.get("results"):
            self._send_json(400, result)
            return

        self._send_json(200, result)

    def _handle_widget_layout_get(self) -> None:
        if self._require_user(ALL_ROLES) is None:
            return

        layout = load_widget_layout()
        logger.info("GET /api/widgets/layout -> %s widget(s)", len(layout))
        self._send_json(200, {"layout": layout})

    def _handle_widget_layout_post(self) -> None:
        user = self._require_user(EDITOR_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        layout = payload.get("layout")
        if not isinstance(layout, list):
            self._send_json(400, {"error": "Payload must include a 'layout' array."})
            return

        normalized: list[dict[str, Any]] = []
        for item in layout:
            if not isinstance(item, dict) or "id" not in item:
                continue
            normalized.append(
                {
                    "id": str(item["id"]),
                    "x": int(item.get("x", 0)),
                    "y": int(item.get("y", 0)),
                    "w": int(item.get("w", 4)),
                    "h": int(item.get("h", 3)),
                }
            )

        if not normalized:
            self._send_json(400, {"error": "No valid widget layout entries provided."})
            return

        logger.info("POST /api/widgets/layout received %s widget(s)", len(normalized))
        save_widget_layout(normalized)
        self._send_json(200, {"status": "ok", "layout": normalized})

    def _handle_cost_optimize(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        organization_id = _resolve_organization_id(user, params)
        provider = params.get("provider", ["aws"])[0] or "aws"

        report = build_cost_optimization_report(
            DB_PATH,
            tenant_id,
            organization_id=organization_id,
            provider=provider,
        )
        self._send_json(200, _merge_orm_recommendations(report))

    def _handle_optimization_recommendations(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        organization_id = _resolve_organization_id(user, params)
        provider = params.get("provider", ["aws"])[0] or "aws"
        hours_raw = params.get("hours", [str(24)])[0]
        try:
            analysis_hours = max(1, min(168, int(hours_raw)))
        except (TypeError, ValueError):
            self._send_json(400, {"error": "Query parameter 'hours' must be an integer between 1 and 168."})
            return

        report = analyze_rightsizing_recommendations(
            DB_PATH,
            organization_id,
            tenant_id=tenant_id,
            provider=provider,
            analysis_hours=analysis_hours,
        )
        self._send_json(200, _merge_orm_recommendations(report))

    def _handle_cost_calculate(self) -> None:
        """POST alias for cost optimization — accepts JSON body or query params."""
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        body = self._read_json_body() or {}

        tenant_id = _resolve_effective_tenant_id(user, params)
        organization_id = _resolve_organization_id(user, params)
        if body.get("tenant_id") and is_global_admin(user):
            tenant_id = str(body["tenant_id"]).strip()
        if body.get("organization_id") and is_global_admin(user):
            organization_id = str(body["organization_id"]).strip()

        provider = (
            str(body.get("provider") or params.get("provider", ["aws"])[0] or "aws")
        )

        report = build_cost_optimization_report(
            DB_PATH,
            tenant_id,
            organization_id=organization_id,
            provider=provider,
            history_limit=int(body.get("history_limit", 500)),
        )
        merged = _merge_orm_recommendations(report)
        self._send_json(
            200,
            {
                "status": "success",
                "calculation": merged,
                **merged,
            },
        )

    def _handle_telemetry_seed(self) -> None:
        """Seed demo telemetry — listed in PUBLIC_POST_ROUTES (no session auth)."""
        payload = self._read_json_body() or {}
        params = parse_qs(urlparse(self.path).query)
        tenant_id = str(
            payload.get("tenant_id")
            or params.get("tenant_id", [DEFAULT_TENANT_ID])[0]
            or DEFAULT_TENANT_ID
        ).strip()
        organization_id = str(
            payload.get("organization_id")
            or params.get("organization_id", [DEFAULT_ORGANIZATION_ID])[0]
            or DEFAULT_ORGANIZATION_ID
        ).strip()

        try:
            result = seed_demo_telemetry(
                DB_PATH,
                tenant_id,
                organization_id=organization_id,
                idle_nodes=int(payload.get("idle_nodes", 4)),
                total_cores=int(payload.get("total_cores", 32)),
                memory_gb=int(payload.get("memory_gb", 128)),
                provider=str(payload.get("provider", "aws")),
                simulated_days=int(payload.get("simulated_days", 7)),
            )
            optimization = build_cost_optimization_report(
                DB_PATH,
                tenant_id,
                organization_id=organization_id,
                provider=str(payload.get("provider", "aws")),
            )
            self._send_json(
                200,
                {
                    **result,
                    "optimization_preview": {
                        "idle_workload_count": optimization["idle_workload_count"],
                        "total_monthly_savings_usd": optimization["total_monthly_savings_usd"],
                    },
                },
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_chatops_interactions(self) -> None:
        """Public ChatOps receiver for Slack/Discord interactive button payloads."""
        raw_body = self._read_raw_body()
        content_type = self.headers.get("Content-Type", "")
        try:
            result = process_chatops_interaction(
                DB_PATH,
                raw_body=raw_body,
                content_type=content_type,
            )
            self._send_json(int(result["status"]), result["body"])
        except Exception as exc:
            logger.error("ChatOps interaction failed: %s", exc)
            self._send_json(500, {"error": "ChatOps interaction processing failed."})

    def _handle_optimization_apply(self) -> None:
        """ChatOps actuator — apply cluster right-sizing remediation actions."""
        user = self._require_user(EDITOR_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        action = payload.get("action")
        if not action:
            self._send_json(400, {"error": "Field 'action' is required."})
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        if payload.get("tenant_id") and is_global_admin(user):
            tenant_id = str(payload["tenant_id"]).strip()

        try:
            result = apply_optimization_remediation(
                DB_PATH,
                tenant_id=tenant_id,
                action=str(action),
                payload=payload,
            )
            self._send_json(
                200,
                {
                    "status": result["status"],
                    "message": result["message"],
                    "action": result.get("action"),
                    "target": result.get("target"),
                    "execution": result.get("execution"),
                    "incident_logged": result.get("incident_logged"),
                },
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:
            logger.error("Optimization apply failed: %s", exc)
            self._send_json(500, {"error": "Cluster remediation failed."})

    def _handle_optimization_override(self) -> None:
        """Admin-only budget guardrail override for blocked predictive scale-up actions."""
        user = self._require_user({ROLE_ADMIN})
        if user is None:
            return

        payload = self._read_json_body()
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        override_token = str(payload.get("override_token") or "").strip()
        if not override_token:
            self._send_json(400, {"error": "Field 'override_token' is required."})
            return

        token_entry = consume_override_token(override_token)
        if token_entry is None:
            self._send_json(403, {"error": "Override token is invalid, expired, or already used."})
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        token_org = str(token_entry.get("organization_id") or DEFAULT_ORGANIZATION_ID)
        if not is_global_admin(user) and organization_id != token_org:
            self._send_json(
                403,
                {"error": "Override token does not match your organization scope."},
            )
            return

        recommendation = token_entry.get("recommendation") or {}
        action = str(payload.get("action") or recommendation.get("action") or "upscale_nodes")
        apply_payload = {
            "action": action,
            "target": payload.get("target") or recommendation.get("recommended_node_spec"),
            "cluster_id": payload.get("cluster_id") or recommendation.get("cluster_id"),
            "guardrail_override": True,
            "override_token": override_token,
        }

        tenant_id = _resolve_effective_tenant_id(user, params)
        if payload.get("tenant_id") and is_global_admin(user):
            tenant_id = str(payload["tenant_id"]).strip()
        elif token_org:
            tenant_id = token_org

        try:
            result = apply_optimization_remediation(
                DB_PATH,
                tenant_id=tenant_id,
                action=action,
                payload=apply_payload,
            )
            guardrails = recommendation.get("budget_guardrails") or {}
            original_cost = float(
                recommendation.get("projected_monthly_cost_usd")
                or guardrails.get("monthly_ceiling_usd")
                or 0
            )
            optimized_cost = float(
                recommendation.get("projected_monthly_cost_usd")
                or original_cost
            )
            audit_record = _record_optimization_audit(
                organization_id=token_org,
                tenant_id=tenant_id,
                user=user,
                action_taken=f"optimization_override:{action}",
                original_cost_usd=original_cost,
                optimized_cost_usd=optimized_cost,
                metadata={
                    "endpoint": "/api/v1/optimization/override",
                    "override_token": override_token,
                    "guardrail_override": True,
                    "recommendation_type": recommendation.get("type"),
                    "execution_mode": (result.get("execution") or {}).get("mode"),
                },
            )
            self._send_json(
                200,
                {
                    "status": result["status"],
                    "message": "Budget guardrail override applied. Infrastructure action forced.",
                    "action": result.get("action"),
                    "target": result.get("target"),
                    "organization_id": token_org,
                    "guardrail_override": True,
                    "execution": result.get("execution"),
                    "incident_logged": result.get("incident_logged"),
                    "recommendation": recommendation,
                    "compliance_ledger_record": audit_record,
                },
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:
            logger.error("Optimization override failed: %s", exc)
            self._send_json(500, {"error": "Guardrail override execution failed."})

    def _handle_optimization_migrate(self) -> None:
        """Execute a cross-cloud migration transfer for an arbitrage recommendation."""
        user = self._require_user(EDITOR_ROLES)
        if user is None:
            return

        payload = self._read_json_body()
        if not isinstance(payload, dict):
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        organization_id = _resolve_organization_id(user, params)
        if payload.get("tenant_id") and is_global_admin(user):
            tenant_id = str(payload["tenant_id"]).strip()

        migration_path = str(payload.get("migration_path") or "").strip()
        if not migration_path and not payload.get("workload"):
            self._send_json(
                400,
                {"error": "Provide 'migration_path' or 'workload' for cross-cloud migration."},
            )
            return

        migrate_payload = dict(payload)
        migrate_payload.setdefault("organization_id", organization_id)

        try:
            result = execute_cross_cloud_migration(
                DB_PATH,
                tenant_id=tenant_id,
                payload=migrate_payload,
            )
            original_cost = float(
                migrate_payload.get("current_monthly_usd")
                or payload.get("current_monthly_usd")
                or 0
            )
            optimized_cost = float(
                migrate_payload.get("target_monthly_usd")
                or payload.get("target_monthly_usd")
                or max(
                    0.0,
                    original_cost - float(result.get("arbitrage_monthly_savings_usd") or 0),
                )
            )
            audit_record = _record_optimization_audit(
                organization_id=organization_id,
                tenant_id=tenant_id,
                user=user,
                action_taken="cross_cloud_migration",
                original_cost_usd=original_cost,
                optimized_cost_usd=optimized_cost,
                metadata={
                    "endpoint": "/api/v1/optimization/migrate",
                    "migration_path": result.get("migration_path"),
                    "source_platform": result.get("source_platform"),
                    "target_platform": result.get("target_platform"),
                    "workload": result.get("workload"),
                    "arbitrage_monthly_savings_usd": result.get("arbitrage_monthly_savings_usd"),
                    "steps_completed": result.get("steps_completed"),
                },
            )
            self._send_json(
                200,
                {
                    "status": result["status"],
                    "message": result["message"],
                    "action": result.get("action"),
                    "workload": result.get("workload"),
                    "migration_path": result.get("migration_path"),
                    "source_platform": result.get("source_platform"),
                    "target_platform": result.get("target_platform"),
                    "arbitrage_monthly_savings_usd": result.get("arbitrage_monthly_savings_usd"),
                    "transfer_sequence": result.get("transfer_sequence"),
                    "steps_completed": result.get("steps_completed"),
                    "incident_logged": result.get("incident_logged"),
                    "organization_id": organization_id,
                    "compliance_ledger_record": audit_record,
                },
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:
            logger.error("Cross-cloud migration failed: %s", exc)
            self._send_json(500, {"error": "Cross-cloud migration failed."})

    def _handle_compliance_ledger(self) -> None:
        """Return chronological signed audit trails from the compliance vault."""
        user = self._require_user({ROLE_ADMIN})
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        organization_id = _resolve_organization_id(user, params)
        if is_global_admin(user):
            requested = params.get("organization_id", [None])[0]
            if requested:
                organization_id = str(requested).strip()

        limit_raw = params.get("limit", ["200"])[0]
        try:
            limit = max(1, min(1000, int(limit_raw)))
        except (TypeError, ValueError):
            self._send_json(400, {"error": "Query parameter 'limit' must be an integer."})
            return

        records = fetch_compliance_ledger(
            DB_PATH,
            organization_id,
            limit=limit,
        )
        self._send_json(
            200,
            {
                "status": "ok",
                "organization_id": organization_id,
                "record_count": len(records),
                "ledger": records,
            },
        )

    def _handle_incidents_rca(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        event_timestamp = params.get("timestamp", [None])[0]
        pod_name = params.get("pod", [None])[0]
        if not event_timestamp or not pod_name:
            self._send_json(
                400,
                {"error": "Query parameters 'timestamp' and 'pod' are required."},
            )
            return

        tenant_scope = resolve_query_tenant_id(user)
        organization_id = _resolve_organization_id(user, params)
        try:
            timeline = build_incident_timeline(
                DB_PATH,
                organization_id=organization_id,
                tenant_id=tenant_scope,
                event_timestamp=event_timestamp,
                pod_name=pod_name,
            )
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
            return

        self._send_json(200, timeline)

    def _handle_tenant_branding_get(self) -> None:
        user = self._require_user(ALL_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        branding = get_tenant_branding(DB_PATH, tenant_id)
        if branding is None:
            self._send_json(404, {"error": f"Branding not found for tenant '{tenant_id}'."})
            return
        self._send_json(200, {"branding": branding})

    def _handle_tenant_branding_put(self) -> None:
        user = self._require_user(EDITOR_ROLES)
        if user is None:
            return

        params = parse_qs(urlparse(self.path).query)
        tenant_id = _resolve_effective_tenant_id(user, params)
        if not is_global_admin(user):
            user_tenant = str(user.get("tenant_id", DEFAULT_TENANT_ID))
            if tenant_id != user_tenant:
                self._send_json(403, {"error": "Cannot update branding for another tenant."})
                return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        company_name = str(payload.get("company_name", "")).strip()
        if not company_name:
            self._send_json(400, {"error": "company_name is required."})
            return

        try:
            branding = upsert_tenant_branding(
                DB_PATH,
                tenant_id,
                company_name=company_name,
                logo_url=str(payload.get("logo_url", "")).strip(),
                primary_color=str(payload.get("primary_color", "#2563eb")).strip(),
                secondary_color=str(payload.get("secondary_color", "#1e40af")).strip(),
            )
        except (RuntimeError, sqlite3.Error) as exc:
            self._send_json(500, {"error": f"Failed to update branding: {exc}"})
            return

        self._send_json(200, {"status": "ok", "branding": branding})

    def _handle_alert_buffer(self) -> None:
        if self._require_user(ALL_ROLES) is None:
            return

        self._send_json(200, alert_buffer.get_queue_status())

    def _handle_mock_webhook(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        summary = payload.get("summary", payload.get("message", "no summary"))
        logger.info(
            "Mock webhook received grouped alert bundle: summary=%r cluster=%s workloads=%s",
            summary, payload.get("cluster_id"), payload.get("workload_count"),
        )
        self._send_json(200, {"status": "received", "echo": payload})

    def _parse_request_path(self) -> str:
        path = urlparse(self.path).path
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        return path

    def _relative_catch_all_path(self, full_path: str) -> str:
        return full_path.lstrip("/")

    def _is_api_path(self, full_path: str) -> bool:
        path = self._relative_catch_all_path(full_path)
        return "api" in path or path.startswith("api")

    def _send_api_route_not_found(self) -> None:
        self._send_json(404, {"error": "API Route Not Found"})

    def _serve_spa_index(self, *, user: dict[str, Any] | None = None) -> None:
        """Serve the SPA index frame (templates/dashboard.html)."""
        html = load_dashboard_html()
        if user is not None:
            self._send_protected_html(html, user)
        else:
            self._send_html(html)

    def _serve_public_page(self, full_path: str) -> bool:
        """Serve public marketing and login pages without authentication."""
        if full_path not in PUBLIC_PAGE_ROUTES:
            return False
        html = load_page_html(full_path)
        if html is None:
            return False
        self._send_html(html)
        return True

    def _serve_page_template(self, full_path: str, *, user: dict[str, Any] | None = None) -> bool:
        """Serve dedicated page templates for non-SPA routes."""
        html = load_page_html(full_path)
        if html is None:
            return False
        if user is not None:
            self._send_protected_html(html, user)
        else:
            self._send_html(html)
        return True

    def _serve_protected_page(self, full_path: str) -> bool:
        if full_path not in PROTECTED_PAGE_PATHS:
            return False
        user = self._require_page_session(full_path)
        if user is None:
            return True
        if full_path in PAGE_TEMPLATE_ROUTES:
            return self._serve_page_template(full_path, user=user)
        self._serve_spa_index(user=user)
        return True

    def _serve_static_file(self, path: str) -> None:
        safe_path = os.path.normpath(path).lstrip(os.path.sep)
        if safe_path.startswith(".."):
            self.send_error(404, "Not Found")
            return

        if safe_path.startswith("static/"):
            file_path = os.path.join(os.path.dirname(__file__), safe_path)
        else:
            file_path = os.path.join(STATIC_DIR, safe_path)

        if not os.path.isfile(file_path):
            self.send_error(404, "Not Found")
            return

        content_type, _ = mimetypes.guess_type(file_path)
        if content_type is None:
            content_type = "application/octet-stream"

        try:
            with open(file_path, "rb") as handle:
                body = handle.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            self.send_error(404, "Not Found")

    def catch_all(self, path: str) -> None:
        """
        SPA catch-all fallback (Flask-equivalent).

        Equivalent to:
            @app.route('/', defaults={'path': ''})
            @app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
        """
        # If 'api' is anywhere in the URL path, do NOT return index.html.
        if "api" in path or path.startswith("api"):
            self._send_api_route_not_found()
            return

        if path.startswith("static/") or any(
            path.endswith(ext) for ext in STATIC_CATCHALL_EXTENSIONS
        ):
            self._serve_static_file(path)
            return

        self.send_error(404, "Not Found")

    def _dispatch_get_api(self, path: str) -> bool:
        routes: dict[str, Any] = {
            "/api/metrics": self._handle_metrics,
            "/api/analytics": self._handle_analytics,
            "/api/v1/cost/optimize": self._handle_cost_optimize,
            "/api/v1/optimization/recommendations": self._handle_optimization_recommendations,
            "/api/v1/incidents/rca": self._handle_incidents_rca,
            "/api/v1/tenant/branding": self._handle_tenant_branding_get,
            "/api/v1/settings/notifications": self._handle_notifications_settings_get,
            "/api/settings": self._handle_settings_get,
            "/api/targets": self._handle_targets,
            "/api/widgets/layout": self._handle_widget_layout_get,
            "/api/alerts/buffer": self._handle_alert_buffer,
            "/api/config/thresholds": self._handle_thresholds_get,
            "/api/config/integrations": self._handle_integrations_get,
            "/api/auth/me": self._handle_auth_me,
            "/api/v1/auth/me": self._handle_auth_me,
        }
        handler = routes.get(path)
        if handler is None:
            return False
        handler()
        return True

    def _dispatch_post_api(self, path: str) -> bool:
        routes: dict[str, Any] = {
            "/api/settings": self._handle_settings_post,
            "/api/test-webhook": self._handle_test_webhook,
            "/api/widgets/layout": self._handle_widget_layout_post,
            "/api/alerts/mock-webhook": self._handle_mock_webhook,
            "/api/config/thresholds": self._handle_thresholds_post,
            "/api/config/integrations": self._handle_integrations_post,
            "/api/auth/login": self._handle_auth_login,
            "/api/v1/auth/login": self._handle_auth_login,
            "/api/auth/logout": self._handle_auth_logout,
            "/api/v1/auth/logout": self._handle_auth_logout,
            "/api/metrics/ingest": self._handle_metrics_ingest,
            "/api/v1/telemetry/seed": self._handle_telemetry_seed,
            "/api/v1/chatops/interactions": self._handle_chatops_interactions,
            "/api/v1/cost/calculate": self._handle_cost_calculate,
            "/api/v1/optimization/apply": self._handle_optimization_apply,
            "/api/v1/optimization/override": self._handle_optimization_override,
            "/api/v1/optimization/migrate": self._handle_optimization_migrate,
            "/api/v1/compliance/ledger": self._handle_compliance_ledger,
            "/api/v1/settings/notifications": self._handle_notifications_settings_post,
            "/api/v1/billing/checkout": self._handle_billing_checkout,
            "/api/v1/billing/stripe/webhook": self._handle_billing_stripe_webhook,
        }
        handler = routes.get(path)
        if handler is None:
            return False
        handler()
        return True

    def _dispatch_put_api(self, path: str) -> bool:
        routes: dict[str, Any] = {
            "/api/v1/tenant/branding": self._handle_tenant_branding_put,
        }
        handler = routes.get(path)
        if handler is None:
            return False
        handler()
        return True

    def _dispatch_delete_api(self, path: str) -> bool:
        routes: dict[str, Any] = {}
        handler = routes.get(path)
        if handler is None:
            return False
        handler()
        return True

    def do_GET(self) -> None:
        try:
            full_path = self._parse_request_path()
            relative = self._relative_catch_all_path(full_path)

            auth_routes: dict[str, Any] = {
                "/auth/login": self._handle_oauth_login,
                "/auth/callback": self._handle_oauth_callback,
                "/auth/sso/authorize": self._handle_sso_authorize_get,
            }
            auth_handler = auth_routes.get(full_path)
            if auth_handler is not None:
                auth_handler()
                return

            if self._is_api_path(full_path) and self._dispatch_get_api(full_path):
                return

            if self._serve_public_page(full_path):
                return

            if self._serve_protected_page(full_path):
                return

            if self._serve_page_template(full_path):
                return

            self.catch_all(relative)
        except Exception as exc:
            logger.error("GET %s failed: %s", self.path, exc)
            self._send_json(500, {"error": "Internal server error."})

    def do_POST(self) -> None:
        try:
            full_path = self._parse_request_path()
            relative = self._relative_catch_all_path(full_path)

            if full_path == "/auth/sso/authorize":
                self._handle_sso_authorize_post()
                return

            if self._is_api_path(full_path):
                if self._dispatch_post_api(full_path):
                    return
                self.catch_all(relative)
                return

            self.send_error(404, "Not Found")
        except Exception as exc:
            logger.error("POST %s failed: %s", self.path, exc)
            self._send_json(500, {"error": "Internal server error."})

    def do_PUT(self) -> None:
        try:
            full_path = self._parse_request_path()
            relative = self._relative_catch_all_path(full_path)

            if self._is_api_path(full_path):
                if self._dispatch_put_api(full_path):
                    return
                self.catch_all(relative)
                return

            self.send_error(404, "Not Found")
        except Exception as exc:
            logger.error("PUT %s failed: %s", self.path, exc)
            self._send_json(500, {"error": "Internal server error."})

    def do_DELETE(self) -> None:
        try:
            full_path = self._parse_request_path()
            relative = self._relative_catch_all_path(full_path)

            if self._is_api_path(full_path):
                if self._dispatch_delete_api(full_path):
                    return
                self.catch_all(relative)
                return

            self.send_error(404, "Not Found")
        except Exception as exc:
            logger.error("DELETE %s failed: %s", self.path, exc)
            self._send_json(500, {"error": "Internal server error."})

    def log_message(self, format: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), format % args)


def _start_background_services() -> None:
    """Start background daemons without taking down the HTTP gateway on transient failures."""
    try:
        discovery.start_discovery()
    except Exception as exc:
        logger.error(
            "Discovery service failed to start; continuing without cluster discovery: %s",
            exc,
            exc_info=True,
        )

    try:
        metrics_collector.start()
    except Exception as exc:
        logger.error(
            "Metrics collector failed to start; continuing without scrape daemon: %s",
            exc,
            exc_info=True,
        )

    try:
        start_collection_loop(interval_seconds=60)
    except Exception as exc:
        logger.error(
            "ORM metrics collection loop failed to start; continuing without K8s polling: %s",
            exc,
            exc_info=True,
        )

    try:
        start_savings_analysis_loop(interval_seconds=SAVINGS_ANALYSIS_INTERVAL_SEC)
    except Exception as exc:
        logger.error(
            "Savings analysis loop failed to start; continuing without FinOps background jobs: %s",
            exc,
            exc_info=True,
        )

    try:
        analytics_engine.start()
    except Exception as exc:
        logger.error(
            "Analytics engine failed to start; continuing without analytics daemon: %s",
            exc,
            exc_info=True,
        )


def main() -> None:
    _configure_logging()
    ensure_data_directory()
    init_orm_tables()
    logger.info("ORM database path: %s", ORM_DB_PATH)
    logger.info("Legacy database URI: %s", SQLALCHEMY_DATABASE_URI)
    logger.info("Legacy database path: %s", DB_PATH)

    config_store.load()
    init_core_data_schemas(DB_PATH)
    init_system_configs(DB_PATH)
    init_compliance_ledger(DB_PATH)
    init_users(DB_PATH)
    init_tenant_branding(DB_PATH)
    init_widget_layout_schema()
    seed_mock_metrics_if_empty()
    _start_background_services()

    server = HTTPServer((HOST, PORT), ManagementHandler)
    logger.info("Management gateway listening on http://%s:%s", HOST, PORT)
    logger.info(
        "Routes: /, /dashboard, /admin/settings, /api/metrics, /api/metrics/ingest, /api/analytics, "
        "/api/v1/optimization/recommendations, /api/v1/optimization/apply, /api/v1/optimization/override, "
        "/api/v1/optimization/migrate, /api/v1/compliance/ledger, /api/v1/chatops/interactions, "
        "/cost-optimization, /api/v1/cost/calculate, /api/v1/telemetry/seed, /api/v1/incidents/rca, "
        "/api/v1/tenant/branding, /api/v1/settings/notifications, /api/v1/billing/checkout, "
        "/api/v1/billing/stripe/webhook, /api/settings, /api/config/thresholds, /api/config/integrations, "
        "/api/auth/login, /api/v1/auth/login, /auth/login, /auth/callback, /api/auth/me, /api/v1/auth/me, "
        "/api/test-webhook, /api/targets, /api/widgets/layout, /api/alerts/buffer"
    )
    logger.info("Discovery, ORM metrics collection, savings analysis, and K8s telemetry running in background")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped.")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
