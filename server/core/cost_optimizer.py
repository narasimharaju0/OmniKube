"""Idle resource detection and cost optimization recommendations."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from core.alert_manager import derive_workload_name
from core.cloud_pricing_matrix import (
    SUPPORTED_PROVIDERS,
    compute_downscale_savings,
    infer_provisioned_size,
)
from core.database import (
    DEFAULT_ORGANIZATION_ID,
    Cluster,
    ClusterMetrics,
    CostRecommendations,
    fetch_cluster_metrics_history,
    get_db,
)

logger = logging.getLogger(__name__)
IDLE_CPU_MAX_PCT = 5.0
IDLE_MEMORY_MAX_PCT = 10.0
DEFAULT_HISTORY_LIMIT = 500
DEFAULT_PROVIDER = "aws"

# ORM FinOps baseline assumptions
BASELINE_COST_PER_NODE_USD = 50.0
IDLE_UTILIZATION_THRESHOLD_PCT = 15.0
RIGHTSIZING_UTILIZATION_MIN_PCT = 15.0
RIGHTSIZING_UTILIZATION_MAX_PCT = 40.0
IDLE_SAVINGS_RATE = 0.60
RIGHTSIZING_SAVINGS_RATE = 0.35
DEFAULT_METRICS_LOOKBACK = 48
RECOMMENDATION_STATUS_ACTIVE = "Active"
RECOMMENDATION_STATUS_RESOLVED = "Resolved"
RECOMMENDATION_TYPE_IDLE_NODE = "Idle Node"
RECOMMENDATION_TYPE_RIGHTSIZING = "Right-sizing"


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


def _average_utilization(metrics: list[ClusterMetrics]) -> tuple[float, float, int]:
    """Return average CPU %, average memory %, and representative node count."""
    if not metrics:
        return 0.0, 0.0, 0

    avg_cpu = sum(float(m.cpu_utilization) for m in metrics) / len(metrics)
    avg_memory = sum(float(m.memory_utilization) for m in metrics) / len(metrics)
    latest = metrics[0]
    node_count = int(latest.node_count or 0)
    return round(avg_cpu, 2), round(avg_memory, 2), node_count


def _resolve_active_recommendations(db: Session, cluster_id: int) -> None:
    """Mark prior Active recommendations as Resolved before writing fresh analysis."""
    active_rows = (
        db.query(CostRecommendations)
        .filter(
            CostRecommendations.cluster_id == cluster_id,
            CostRecommendations.status == RECOMMENDATION_STATUS_ACTIVE,
        )
        .all()
    )
    for row in active_rows:
        row.status = RECOMMENDATION_STATUS_RESOLVED


def _build_recommendation_payload(
    *,
    rec_type: str,
    node_count: int,
    savings_rate: float,
    avg_cpu: float,
    avg_memory: float,
) -> dict[str, float | str]:
    """Compute monthly cost fields from node baseline and savings rate."""
    billable_nodes = max(node_count, 1)
    current_monthly_cost = round(billable_nodes * BASELINE_COST_PER_NODE_USD, 2)
    potential_savings = round(current_monthly_cost * savings_rate, 2)
    projected_monthly_cost = round(current_monthly_cost - potential_savings, 2)
    return {
        "type": rec_type,
        "current_monthly_cost": current_monthly_cost,
        "projected_monthly_cost": projected_monthly_cost,
        "potential_savings": potential_savings,
        "avg_cpu": avg_cpu,
        "avg_memory": avg_memory,
    }


def calculate_cluster_savings(cluster_id: int) -> list[CostRecommendations]:
    """
    Analyze recent ClusterMetrics for a cluster and persist FinOps recommendations.

    Rules:
    - Idle Node: average CPU and memory utilization both below 15% → 60% savings.
    - Right-sizing: average utilization between 15% and 40% → 35% savings.
    - Baseline cost: $50/month per node.
    """
    saved: list[CostRecommendations] = []

    with get_db() as db:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).one_or_none()
        if cluster is None:
            logger.warning("calculate_cluster_savings: cluster id=%s not found", cluster_id)
            return []

        metric_rows = (
            db.query(ClusterMetrics)
            .filter(ClusterMetrics.cluster_id == cluster_id)
            .order_by(desc(ClusterMetrics.timestamp))
            .limit(DEFAULT_METRICS_LOOKBACK)
            .all()
        )

        if not metric_rows:
            logger.info(
                "calculate_cluster_savings: no metrics for cluster id=%s (%s)",
                cluster_id,
                cluster.cluster_name,
            )
            return []

        avg_cpu, avg_memory, node_count = _average_utilization(metric_rows)
        avg_utilization = (avg_cpu + avg_memory) / 2.0

        recommendation_payload: dict[str, float | str] | None = None
        if avg_cpu < IDLE_UTILIZATION_THRESHOLD_PCT and avg_memory < IDLE_UTILIZATION_THRESHOLD_PCT:
            recommendation_payload = _build_recommendation_payload(
                rec_type=RECOMMENDATION_TYPE_IDLE_NODE,
                node_count=node_count,
                savings_rate=IDLE_SAVINGS_RATE,
                avg_cpu=avg_cpu,
                avg_memory=avg_memory,
            )
        elif (
            RIGHTSIZING_UTILIZATION_MIN_PCT <= avg_utilization <= RIGHTSIZING_UTILIZATION_MAX_PCT
        ):
            recommendation_payload = _build_recommendation_payload(
                rec_type=RECOMMENDATION_TYPE_RIGHTSIZING,
                node_count=node_count,
                savings_rate=RIGHTSIZING_SAVINGS_RATE,
                avg_cpu=avg_cpu,
                avg_memory=avg_memory,
            )

        if recommendation_payload is None:
            logger.info(
                "calculate_cluster_savings: no rule matched for cluster=%s "
                "(avg_cpu=%.1f%% avg_memory=%.1f%%)",
                cluster.cluster_name,
                avg_cpu,
                avg_memory,
            )
            return []

        _resolve_active_recommendations(db, cluster_id)

        record = CostRecommendations(
            cluster_id=cluster_id,
            type=str(recommendation_payload["type"]),
            current_monthly_cost=float(recommendation_payload["current_monthly_cost"]),
            projected_monthly_cost=float(recommendation_payload["projected_monthly_cost"]),
            potential_savings=float(recommendation_payload["potential_savings"]),
            status=RECOMMENDATION_STATUS_ACTIVE,
            created_at=datetime.now(timezone.utc),
        )
        db.add(record)
        db.flush()
        saved.append(record)

        logger.info(
            "Cost recommendation saved: cluster=%s type=%s savings=$%.2f/mo "
            "(avg_cpu=%.1f%% avg_memory=%.1f%% nodes=%d)",
            cluster.cluster_name,
            record.type,
            record.potential_savings,
            avg_cpu,
            avg_memory,
            node_count,
        )

    return saved


def calculate_all_cluster_savings() -> list[CostRecommendations]:
    """Run savings analysis for every registered cluster."""
    results: list[CostRecommendations] = []
    with get_db() as db:
        cluster_ids = [row.id for row in db.query(Cluster.id).all()]

    for cluster_id in cluster_ids:
        results.extend(calculate_cluster_savings(cluster_id))
    return results


def _workload_key(labels: dict[str, Any]) -> str:
    pod_name = str(labels.get("pod_name") or "unknown")
    namespace = str(labels.get("namespace") or "default")
    kubernetes_labels = labels.get("kubernetes_labels")
    workload = derive_workload_name(
        pod_name,
        kubernetes_labels if isinstance(kubernetes_labels, dict) else None,
    )
    return f"{namespace}/{workload}"


def fetch_tenant_telemetry_history(
    db_path: str,
    organization_id: str,
    *,
    limit: int = DEFAULT_HISTORY_LIMIT,
) -> list[dict[str, Any]]:
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    try:
        return fetch_cluster_metrics_history(db_path, org_id, limit=limit)
    except Exception as exc:
        logger.error(
            "Failed to fetch telemetry for organization %s: %s",
            org_id,
            exc,
        )
        return []


def find_idle_resources(
    metrics: list[dict[str, Any]],
    *,
    max_cpu_pct: float = IDLE_CPU_MAX_PCT,
    max_memory_pct: float = IDLE_MEMORY_MAX_PCT,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in metrics:
        labels = _parse_labels(row.get("labels"))
        key = _workload_key(labels)
        entry = grouped.setdefault(
            key,
            {
                "workload": key,
                "pod_name": str(labels.get("pod_name") or "unknown"),
                "namespace": str(labels.get("namespace") or "default"),
                "cluster_id": str(labels.get("cluster_id") or "omnikube-cluster"),
                "max_cpu": 0.0,
                "max_memory": 0.0,
                "sample_count": 0,
                "provider": str(labels.get("cloud_provider") or labels.get("provider") or ""),
                "instance_size": str(labels.get("instance_size") or ""),
            },
        )
        cpu = float(row.get("cpu", 0))
        memory = float(row.get("memory", 0))
        entry["max_cpu"] = max(entry["max_cpu"], cpu)
        entry["max_memory"] = max(entry["max_memory"], memory)
        entry["sample_count"] += 1

    idle: list[dict[str, Any]] = []
    for entry in grouped.values():
        if entry["max_cpu"] < max_cpu_pct and entry["max_memory"] < max_memory_pct:
            idle.append(entry)
    return idle


def build_cost_optimization_report(
    db_path: str,
    tenant_id: str,
    *,
    organization_id: str | None = None,
    provider: str = DEFAULT_PROVIDER,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
) -> dict[str, Any]:
    provider_key = provider.lower()
    if provider_key not in SUPPORTED_PROVIDERS:
        provider_key = DEFAULT_PROVIDER

    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    metrics = fetch_tenant_telemetry_history(db_path, org_id, limit=history_limit)
    idle_workloads = find_idle_resources(metrics)

    recommendations: list[dict[str, Any]] = []
    total_monthly_savings = 0.0

    for workload in idle_workloads:
        current_size = workload["instance_size"] or infer_provisioned_size(
            workload["max_cpu"], workload["max_memory"]
        )
        workload_provider = (workload["provider"] or provider_key).lower()
        if workload_provider not in SUPPORTED_PROVIDERS:
            workload_provider = provider_key

        savings = compute_downscale_savings(workload_provider, current_size)
        if savings is None:
            continue

        monthly_savings = float(savings["monthly_savings_usd"])
        total_monthly_savings += monthly_savings
        recommendations.append(
            {
                "workload": workload["workload"],
                "pod_name": workload["pod_name"],
                "namespace": workload["namespace"],
                "cluster_id": workload["cluster_id"],
                "max_cpu_pct": round(workload["max_cpu"], 2),
                "max_memory_pct": round(workload["max_memory"], 2),
                "sample_count": workload["sample_count"],
                "provider": savings["provider"],
                "current_size": savings["current_size"],
                "recommended_size": savings["recommended_size"],
                "current_monthly_usd": savings["current_monthly_usd"],
                "recommended_monthly_usd": savings["recommended_monthly_usd"],
                "monthly_savings_usd": monthly_savings,
                "recommendation": (
                    f"Downscale {workload['workload']} from "
                    f"{savings['current_size']} to {savings['recommended_size']} "
                    f"to save ${monthly_savings:.2f}/month"
                    if savings["recommended_size"]
                    else "Already at smallest instance tier"
                ),
            }
        )

    recommendations.sort(key=lambda item: item["monthly_savings_usd"], reverse=True)

    return {
        "tenant_id": tenant_id,
        "organization_id": org_id,
        "provider": provider_key,
        "history_samples": len(metrics),
        "idle_thresholds": {
            "max_cpu_pct": IDLE_CPU_MAX_PCT,
            "max_memory_pct": IDLE_MEMORY_MAX_PCT,
        },
        "idle_workload_count": len(recommendations),
        "total_monthly_savings_usd": round(total_monthly_savings, 2),
        "recommendations": recommendations,
    }
