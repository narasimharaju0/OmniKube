"""Root-cause analysis timeline builder for incident investigations."""

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from core.database import DEFAULT_ORGANIZATION_ID

logger = logging.getLogger(__name__)

RCA_WINDOW_MINUTES = 5
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def parse_event_timestamp(raw: str) -> datetime:
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in (TIMESTAMP_FORMAT, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {raw!r}")


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


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


def _pod_matches(labels: dict[str, Any], pod_filter: str) -> bool:
    pod_name = str(labels.get("pod_name") or "")
    if not pod_filter:
        return True
    needle = pod_filter.lower()
    return needle in pod_name.lower() or needle in str(labels.get("workload_name", "")).lower()


def fetch_metrics_in_window(
    db_path: str,
    *,
    organization_id: str,
    tenant_id: str | None = None,
    center: datetime,
    pod_name: str,
    window_minutes: int = RCA_WINDOW_MINUTES,
) -> list[dict[str, Any]]:
    start = center - timedelta(minutes=window_minutes)
    end = center + timedelta(minutes=window_minutes)
    start_ts = _format_timestamp(start)
    end_ts = _format_timestamp(end)

    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    params: list[Any] = [org_id, start_ts, end_ts]
    tenant_clause = ""
    if tenant_id is not None:
        tenant_clause = " AND tenant_id = ?"
        params.append(tenant_id)

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, timestamp, cpu, memory, labels, tenant_id, organization_id
                FROM cluster_metrics
                WHERE organization_id = ?
                  AND datetime(substr(timestamp, 1, 19)) >= datetime(substr(?, 1, 19))
                  AND datetime(substr(timestamp, 1, 19)) <= datetime(substr(?, 1, 19))
                {tenant_clause}
                ORDER BY timestamp ASC
                """,
                params,
            ).fetchall()
    except sqlite3.Error as exc:
        logger.error("RCA metrics query failed: %s", exc)
        return []

    results: list[dict[str, Any]] = []
    for row in rows:
        labels = _parse_labels(row["labels"])
        if not _pod_matches(labels, pod_name):
            continue
        results.append(dict(row))
    return results


def fetch_incident_events_in_window(
    db_path: str,
    *,
    tenant_id: str | None,
    center: datetime,
    pod_name: str,
    window_minutes: int = RCA_WINDOW_MINUTES,
) -> list[dict[str, Any]]:
    start = center - timedelta(minutes=window_minutes)
    end = center + timedelta(minutes=window_minutes)
    start_ts = _format_timestamp(start)
    end_ts = _format_timestamp(end)

    tenant_clause = ""
    params: list[Any] = [start_ts, end_ts]
    if tenant_id is not None:
        tenant_clause = " AND tenant_id = ?"
        params.append(tenant_id)

    pod_clause = ""
    if pod_name:
        pod_clause = " AND pod_name LIKE ?"
        params.append(f"%{pod_name}%")

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT
                    id, tenant_id, timestamp, event_type, pod_name, namespace, cluster_id,
                    metric, value, threshold, severity, message
                FROM incident_events
                WHERE datetime(substr(timestamp, 1, 19)) >= datetime(substr(?, 1, 19))
                  AND datetime(substr(timestamp, 1, 19)) <= datetime(substr(?, 1, 19))
                {tenant_clause}
                {pod_clause}
                ORDER BY timestamp ASC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.Error as exc:
        logger.error("RCA incident query failed: %s", exc)
        return []


def _infer_deployment_events(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Surface probable rollout/restart events from abrupt utilization shifts."""
    deployments: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None

    for row in metrics:
        labels = _parse_labels(row.get("labels"))
        cpu = float(row.get("cpu", 0))
        memory = float(row.get("memory", 0))
        pod_name = str(labels.get("pod_name") or "")
        namespace = str(labels.get("namespace") or "default")

        if previous is not None:
            cpu_delta = abs(cpu - float(previous.get("cpu", 0)))
            memory_delta = abs(memory - float(previous.get("memory", 0)))
            if cpu_delta >= 25 or memory_delta >= 25:
                deployments.append(
                    {
                        "timestamp": row["timestamp"],
                        "type": "deployment",
                        "severity": "info",
                        "pod_name": pod_name,
                        "namespace": namespace,
                        "summary": (
                            f"Probable deployment or restart on {namespace}/{pod_name} "
                            f"(CPU Δ{cpu_delta:.1f}%, Memory Δ{memory_delta:.1f}%)"
                        ),
                        "details": {
                            "cpu_delta": round(cpu_delta, 1),
                            "memory_delta": round(memory_delta, 1),
                            "cluster_id": labels.get("cluster_id"),
                        },
                    }
                )
        previous = {"cpu": cpu, "memory": memory, "pod_name": pod_name}

    return deployments


def build_incident_timeline(
    db_path: str,
    *,
    organization_id: str,
    tenant_id: str | None,
    event_timestamp: str,
    pod_name: str,
) -> dict[str, Any]:
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    center = parse_event_timestamp(event_timestamp)
    window_start = center - timedelta(minutes=RCA_WINDOW_MINUTES)
    window_end = center + timedelta(minutes=RCA_WINDOW_MINUTES)

    metrics = fetch_metrics_in_window(
        db_path,
        organization_id=org_id,
        tenant_id=tenant_id,
        center=center,
        pod_name=pod_name,
    )
    incidents = fetch_incident_events_in_window(
        db_path,
        tenant_id=tenant_id,
        center=center,
        pod_name=pod_name,
    )

    timeline: list[dict[str, Any]] = []

    for row in metrics:
        labels = _parse_labels(row.get("labels"))
        cpu = float(row.get("cpu", 0))
        memory = float(row.get("memory", 0))
        timeline.append(
            {
                "timestamp": row["timestamp"],
                "type": "metric_sample",
                "severity": "info",
                "pod_name": str(labels.get("pod_name") or pod_name),
                "namespace": str(labels.get("namespace") or "default"),
                "summary": f"CPU {cpu:.1f}% | Memory {memory:.1f}%",
                "details": {
                    "cpu": cpu,
                    "memory": memory,
                    "cluster_id": labels.get("cluster_id"),
                },
            }
        )

    for event in incidents:
        timeline.append(
            {
                "timestamp": event["timestamp"],
                "type": event["event_type"],
                "severity": event["severity"],
                "pod_name": event["pod_name"],
                "namespace": event["namespace"],
                "summary": event["message"] or f"{event['event_type']} on {event['pod_name']}",
                "details": {
                    "metric": event.get("metric"),
                    "value": event.get("value"),
                    "threshold": event.get("threshold"),
                    "cluster_id": event.get("cluster_id"),
                },
            }
        )

    timeline.extend(_infer_deployment_events(metrics))

    timeline.sort(key=lambda item: str(item["timestamp"]))

    breach_count = sum(1 for item in timeline if item["type"] == "threshold_breach")
    error_count = sum(1 for item in timeline if item["type"] == "error_log")
    deployment_count = sum(1 for item in timeline if item["type"] == "deployment")

    return {
        "tenant_id": tenant_id,
        "organization_id": org_id,
        "pod": pod_name,
        "anchor_timestamp": _format_timestamp(center),
        "window": {
            "start": _format_timestamp(window_start),
            "end": _format_timestamp(window_end),
            "minutes": RCA_WINDOW_MINUTES * 2,
        },
        "summary": {
            "metric_samples": len(metrics),
            "threshold_breaches": breach_count,
            "error_logs": error_count,
            "deployment_events": deployment_count,
            "total_events": len(timeline),
        },
        "timeline": timeline,
    }
