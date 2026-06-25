"""Automated right-sizing analysis and predictive utilization forecasting."""

from __future__ import annotations

import json
import logging
import secrets
import threading
import time
from typing import Any

from core.alert_manager import derive_workload_name
from core.cloud_pricing_matrix import (
    SUPPORTED_PROVIDERS,
    compute_downscale_savings,
    compute_upscale_cost,
    estimate_monthly_cost,
    infer_provisioned_size,
    recommend_downscale_size,
    recommend_upscale_size,
)
from core.database import DEFAULT_ORGANIZATION_ID, fetch_cluster_metrics_history, fetch_cluster_snapshots
from core.system_config import get_budget_guardrails

logger = logging.getLogger(__name__)

LOW_UTILIZATION_THRESHOLD_PCT = 30.0
PREDICTIVE_UPPER_THRESHOLD_PCT = 75.0
DEFAULT_ANALYSIS_HOURS = 24
DEFAULT_FORECAST_HISTORY_HOURS = 24
DEFAULT_PROVIDER = "aws"
PODS_PER_NODE_BASELINE = 10
DEFAULT_NODE_INSTANCE_SIZE = "medium"
OVERRIDE_TOKEN_TTL_SEC = 3600
EMA_ALPHA = 0.35
HOURS_PER_MONTH = 730.0
MIN_ARBITRAGE_SAVINGS_USD = 10.0
HEAVY_UTILIZATION_THRESHOLD_PCT = 35.0
EXPENSIVE_SOURCE_PROVIDERS = frozenset({"aws", "azure"})

# Simulated real-world comparative hourly rates for standard compute units (USD/hr).
CROSS_CLOUD_PRICING_MATRIX: dict[str, dict[str, Any]] = {
    "aws": {
        "platform": "Amazon EKS",
        "billing_model": "on_demand",
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0416},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0832},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1664},
    },
    "azure": {
        "platform": "Azure AKS",
        "billing_model": "on_demand",
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0440},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0880},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1760},
    },
    "gcp": {
        "platform": "GCP GKE",
        "billing_model": "standard",
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0380},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0760},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1520},
    },
    "gcp_spot": {
        "platform": "GCP GKE Spot",
        "billing_model": "spot",
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0114},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0228},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.0456},
    },
}

CROSS_CLOUD_PROVIDER_KEYS: tuple[str, ...] = tuple(CROSS_CLOUD_PRICING_MATRIX.keys())
ARBITRAGE_TARGET_PRIORITY: tuple[str, ...] = ("gcp_spot", "gcp", "aws", "azure")

_predictive_lock = threading.Lock()
_predictive_recommendations: dict[str, dict[str, Any]] = {}
_override_tokens: dict[str, dict[str, Any]] = {}


def _average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _clamp_pct(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def _ema_series(values: list[float], alpha: float = EMA_ALPHA) -> list[float]:
    if not values:
        return []
    ema_values = [float(values[0])]
    for value in values[1:]:
        ema_values.append(alpha * float(value) + (1.0 - alpha) * ema_values[-1])
    return ema_values


def _linear_regression_forecast(
    values: list[float],
    *,
    steps_ahead: float,
) -> tuple[float, float]:
    """Return forecasted value and slope for equally spaced hourly samples."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    if n == 1:
        return float(values[0]), 0.0

    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    numerator = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    slope = numerator / denominator if denominator else 0.0
    intercept = mean_y - slope * mean_x
    forecast_x = (n - 1) + steps_ahead
    forecast = intercept + slope * forecast_x
    return _clamp_pct(forecast), slope


def predict_next_window_utilization(
    db_path: str,
    organization_id: str,
    *,
    hours_ahead: int = 1,
    history_hours: int = DEFAULT_FORECAST_HISTORY_HOURS,
) -> dict[str, Any]:
    """
    Forecast CPU and memory utilization for an organization using accelerated
    EMA trend lines blended with linear regression over cluster_snapshots history.
    """
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    snapshots = fetch_cluster_snapshots(db_path, org_id, hours=history_hours)
    chronological = list(reversed(snapshots))

    if not chronological:
        return {
            "organization_id": org_id,
            "hours_ahead": int(hours_ahead),
            "snapshot_count": 0,
            "forecast": None,
            "message": "Insufficient snapshot history for predictive forecasting.",
        }

    cpu_series = [float(row["cpu_utilization"]) for row in chronological]
    memory_series = [float(row["memory_utilization"]) for row in chronological]

    cpu_ema = _ema_series(cpu_series)
    memory_ema = _ema_series(memory_series)

    cpu_regression, cpu_slope = _linear_regression_forecast(
        cpu_series,
        steps_ahead=float(hours_ahead),
    )
    memory_regression, memory_slope = _linear_regression_forecast(
        memory_series,
        steps_ahead=float(hours_ahead),
    )

    cpu_ema_momentum = 0.0
    memory_ema_momentum = 0.0
    if len(cpu_ema) >= 2:
        cpu_ema_momentum = cpu_ema[-1] - cpu_ema[-2]
        memory_ema_momentum = memory_ema[-1] - memory_ema[-2]

    cpu_ema_forecast = _clamp_pct(cpu_ema[-1] + cpu_ema_momentum * float(hours_ahead))
    memory_ema_forecast = _clamp_pct(memory_ema[-1] + memory_ema_momentum * float(hours_ahead))

    predicted_cpu = round(_clamp_pct((cpu_regression * 0.55) + (cpu_ema_forecast * 0.45)), 2)
    predicted_memory = round(
        _clamp_pct((memory_regression * 0.55) + (memory_ema_forecast * 0.45)),
        2,
    )

    latest = chronological[-1]
    return {
        "organization_id": org_id,
        "hours_ahead": int(hours_ahead),
        "snapshot_count": len(chronological),
        "history_hours": history_hours,
        "latest": {
            "timestamp": latest.get("timestamp"),
            "cpu_utilization_pct": round(float(latest.get("cpu_utilization", 0)), 2),
            "memory_utilization_pct": round(float(latest.get("memory_utilization", 0)), 2),
            "node_count": int(latest.get("node_count", 0)),
            "pod_count": int(latest.get("pod_count", 0)),
        },
        "forecast": {
            "predicted_cpu_utilization_pct": predicted_cpu,
            "predicted_memory_utilization_pct": predicted_memory,
            "predicted_peak_utilization_pct": round(max(predicted_cpu, predicted_memory), 2),
            "cpu_regression_slope": round(cpu_slope, 4),
            "memory_regression_slope": round(memory_slope, 4),
            "cpu_ema_forecast_pct": round(cpu_ema_forecast, 2),
            "memory_ema_forecast_pct": round(memory_ema_forecast, 2),
            "method": "accelerated_ema_regression_blend",
        },
    }


def _purge_expired_override_tokens(now: float | None = None) -> None:
    current = now if now is not None else time.time()
    expired = [
        token
        for token, entry in _override_tokens.items()
        if float(entry.get("expires_at", 0)) <= current or entry.get("consumed")
    ]
    for token in expired:
        _override_tokens.pop(token, None)


def _issue_budget_override_token(
    organization_id: str,
    recommendation: dict[str, Any],
) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + OVERRIDE_TOKEN_TTL_SEC
    with _predictive_lock:
        _purge_expired_override_tokens(expires_at)
        _override_tokens[token] = {
            "organization_id": organization_id,
            "recommendation": recommendation,
            "created_at": time.time(),
            "expires_at": expires_at,
            "consumed": False,
        }
    return token


def get_override_token_entry(token: str) -> dict[str, Any] | None:
    with _predictive_lock:
        _purge_expired_override_tokens()
        entry = _override_tokens.get(token)
        if entry is None or entry.get("consumed"):
            return None
        if time.time() > float(entry.get("expires_at", 0)):
            _override_tokens.pop(token, None)
            return None
        return dict(entry)


def consume_override_token(token: str) -> dict[str, Any] | None:
    with _predictive_lock:
        entry = _override_tokens.get(token)
        if entry is None or entry.get("consumed"):
            return None
        if time.time() > float(entry.get("expires_at", 0)):
            _override_tokens.pop(token, None)
            return None
        entry["consumed"] = True
        return dict(entry)


def get_predictive_recommendation(organization_id: str) -> dict[str, Any] | None:
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    with _predictive_lock:
        entry = _predictive_recommendations.get(org_id)
        return dict(entry) if entry else None


def evaluate_predictive_scale_up(
    db_path: str,
    organization_id: str,
    *,
    provider: str = DEFAULT_PROVIDER,
    hours_ahead: int = 1,
) -> dict[str, Any] | None:
    """
    Evaluate forecasted utilization and emit a predictive scale-up recommendation
    when the upcoming window is expected to exceed the upper threshold.
    """
    provider_key = provider.lower()
    if provider_key not in SUPPORTED_PROVIDERS:
        provider_key = DEFAULT_PROVIDER

    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    forecast_report = predict_next_window_utilization(
        db_path,
        org_id,
        hours_ahead=hours_ahead,
    )
    forecast = forecast_report.get("forecast")
    if not forecast:
        return None

    predicted_peak = float(forecast["predicted_peak_utilization_pct"])
    if predicted_peak <= PREDICTIVE_UPPER_THRESHOLD_PCT:
        with _predictive_lock:
            _predictive_recommendations.pop(org_id, None)
        return None

    latest = forecast_report.get("latest") or {}
    node_count = max(1, int(latest.get("node_count") or 1))
    current_size = infer_provisioned_size(
        float(forecast["predicted_cpu_utilization_pct"]),
        float(forecast["predicted_memory_utilization_pct"]),
    )
    target_size = recommend_upscale_size(current_size)
    scale_out_only = target_size is None
    if scale_out_only:
        target_size = current_size
        monthly_node_cost = estimate_monthly_cost(provider_key, current_size)
        projected_cost = round(float(monthly_node_cost or 0), 2)
        upscale_cost = {
            "provider": provider_key,
            "current_size": current_size,
            "recommended_size": current_size,
            "node_count": node_count + 1,
            "per_node_monthly_usd": projected_cost,
            "projected_monthly_usd": projected_cost,
        }
    else:
        upscale_cost = compute_upscale_cost(
            provider_key,
            current_size,
            target_size=target_size,
            node_count=node_count,
        )
        projected_cost = float(upscale_cost["projected_monthly_usd"]) if upscale_cost else 0.0

    guardrails = get_budget_guardrails(db_path, org_id)
    remaining_budget = float(guardrails["remaining_usd"])
    blocked_by_guardrail = projected_cost > remaining_budget

    recommendation: dict[str, Any] = {
        "type": "predictive_scale_up",
        "action": "upscale_nodes",
        "status": "Blocked by Guardrail" if blocked_by_guardrail else "Ready",
        "predictive": True,
        "forecast_window_hours": hours_ahead,
        "predicted_cpu_utilization_pct": forecast["predicted_cpu_utilization_pct"],
        "predicted_memory_utilization_pct": forecast["predicted_memory_utilization_pct"],
        "predicted_peak_utilization_pct": predicted_peak,
        "upper_threshold_pct": PREDICTIVE_UPPER_THRESHOLD_PCT,
        "current_node_count": node_count,
        "recommended_node_count": node_count + 1,
        "current_node_spec": current_size,
        "recommended_node_spec": target_size,
        "scale_out_only": scale_out_only,
        "projected_monthly_cost_usd": projected_cost,
        "budget_guardrails": guardrails,
        "rationale": (
            f"Predicted peak utilization of {predicted_peak:.1f}% in the next "
            f"{hours_ahead} hour(s) exceeds the {PREDICTIVE_UPPER_THRESHOLD_PCT:.0f}% "
            f"auto-throttle threshold. Recommend "
            + (
                f"adding a {target_size} node (horizontal scale-out)."
                if scale_out_only
                else f"scaling cluster nodes from {current_size} to {target_size}."
            )
        ),
    }

    if blocked_by_guardrail:
        recommendation["override_token"] = _issue_budget_override_token(org_id, recommendation)
        recommendation["guardrail_message"] = (
            f"Projected scale-up cost ${projected_cost:,.2f}/mo exceeds remaining "
            f"monthly budget ${remaining_budget:,.2f}. Admin override required."
        )
        logger.warning(
            "[Optimizer] Predictive scale-up blocked by budget guardrail org=%s "
            "projected=%.2f remaining=%.2f",
            org_id,
            projected_cost,
            remaining_budget,
        )
    else:
        logger.info(
            "[Optimizer] Predictive scale-up recommendation generated org=%s peak=%.1f%%",
            org_id,
            predicted_peak,
        )

    stored = {
        "organization_id": org_id,
        "provider": provider_key,
        "generated_at": time.time(),
        "forecast_report": forecast_report,
        "recommendation": recommendation,
    }
    with _predictive_lock:
        _predictive_recommendations[org_id] = stored
    return stored


def list_active_organization_ids(db_path: str) -> list[str]:
    """Return organization IDs with recent cluster snapshot telemetry."""
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT organization_id
                FROM cluster_snapshots
                WHERE datetime(substr(timestamp, 1, 19)) >= datetime('now', '-48 hours')
                ORDER BY organization_id ASC
                """
            ).fetchall()
        org_ids = [str(row[0]) for row in rows if row and row[0]]
        return org_ids or [DEFAULT_ORGANIZATION_ID]
    except sqlite3.Error as exc:
        logger.error("Failed to list active organizations: %s", exc)
        return [DEFAULT_ORGANIZATION_ID]


def scan_predictive_scale_up(
    db_path: str,
    *,
    provider: str = DEFAULT_PROVIDER,
    hours_ahead: int = 1,
) -> list[dict[str, Any]]:
    """Background sweep: evaluate predictive scale-up for all active organizations."""
    generated: list[dict[str, Any]] = []
    for org_id in list_active_organization_ids(db_path):
        result = evaluate_predictive_scale_up(
            db_path,
            org_id,
            provider=provider,
            hours_ahead=hours_ahead,
        )
        if result is not None:
            generated.append(result)
    return generated


def _round_replica_target(current_replicas: int, utilization_pct: float) -> int:
    if current_replicas <= 1:
        return 1
    headroom_ratio = max(utilization_pct, 1.0) / LOW_UTILIZATION_THRESHOLD_PCT
    target = max(1, round(current_replicas * min(1.0, headroom_ratio)))
    return min(target, current_replicas - 1) if target >= current_replicas else target


def _parse_metric_labels(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _normalize_cloud_provider(raw: str) -> str:
    value = str(raw or "").strip().lower()
    aliases = {
        "eks": "aws",
        "amazon": "aws",
        "amazon_eks": "aws",
        "aks": "azure",
        "microsoft": "azure",
        "gke": "gcp",
        "google": "gcp",
        "gcp_gke": "gcp",
        "gke_spot": "gcp_spot",
        "spot": "gcp_spot",
    }
    return aliases.get(value, value)


def _cross_cloud_platform_name(provider_key: str) -> str:
    entry = CROSS_CLOUD_PRICING_MATRIX.get(provider_key, {})
    return str(entry.get("platform") or provider_key.upper())


def _cross_cloud_monthly_cost(provider_key: str, instance_size: str) -> float | None:
    provider = _normalize_cloud_provider(provider_key)
    matrix = CROSS_CLOUD_PRICING_MATRIX.get(provider)
    if matrix is None:
        return None
    tier = matrix.get(instance_size.lower())
    if not isinstance(tier, dict):
        return None
    hourly = float(tier.get("hourly_usd", 0))
    return round(hourly * HOURS_PER_MONTH, 2)


def _workload_identity(labels: dict[str, Any]) -> dict[str, str]:
    pod_name = str(labels.get("pod_name") or "unknown")
    namespace = str(labels.get("namespace") or "default")
    kubernetes_labels = labels.get("kubernetes_labels")
    workload = derive_workload_name(
        pod_name,
        kubernetes_labels if isinstance(kubernetes_labels, dict) else None,
    )
    return {
        "workload": f"{namespace}/{workload}",
        "pod_name": pod_name,
        "namespace": namespace,
    }


def _is_heavy_workload(entry: dict[str, Any]) -> bool:
    instance_size = str(entry.get("instance_size") or "").lower()
    peak_util = max(float(entry.get("max_cpu", 0)), float(entry.get("max_memory", 0)))
    on_expensive_tier = (
        entry.get("provider") in EXPENSIVE_SOURCE_PROVIDERS
        and instance_size in {"medium", "large"}
    )
    return on_expensive_tier or peak_util >= HEAVY_UTILIZATION_THRESHOLD_PCT


def _find_cross_cloud_arbitrage_target(
    source_provider: str,
    instance_size: str,
) -> dict[str, Any] | None:
    source_key = _normalize_cloud_provider(source_provider)
    if source_key not in CROSS_CLOUD_PRICING_MATRIX:
        return None

    current_monthly = _cross_cloud_monthly_cost(source_key, instance_size)
    if current_monthly is None:
        return None

    best: dict[str, Any] | None = None
    for target_key in ARBITRAGE_TARGET_PRIORITY:
        if target_key == source_key:
            continue
        target_monthly = _cross_cloud_monthly_cost(target_key, instance_size)
        if target_monthly is None or target_monthly >= current_monthly:
            continue
        savings = round(current_monthly - target_monthly, 2)
        if savings < MIN_ARBITRAGE_SAVINGS_USD:
            continue
        candidate = {
            "target_provider": target_key,
            "target_platform": _cross_cloud_platform_name(target_key),
            "target_billing_model": CROSS_CLOUD_PRICING_MATRIX[target_key].get("billing_model"),
            "target_monthly_usd": target_monthly,
            "arbitrage_monthly_savings_usd": savings,
        }
        if best is None or savings > float(best["arbitrage_monthly_savings_usd"]):
            best = candidate
    return best


def _group_active_workloads(
    metrics: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in metrics:
        labels = _parse_metric_labels(row.get("labels"))
        identity = _workload_identity(labels)
        key = identity["workload"]
        provider = _normalize_cloud_provider(
            str(labels.get("cloud_provider") or labels.get("provider") or "")
        )
        entry = grouped.setdefault(
            key,
            {
                **identity,
                "cluster_id": str(labels.get("cluster_id") or "omnikube-cluster"),
                "max_cpu": 0.0,
                "max_memory": 0.0,
                "sample_count": 0,
                "provider": provider,
                "instance_size": str(labels.get("instance_size") or ""),
            },
        )
        if provider:
            entry["provider"] = provider
        cpu = float(row.get("cpu", 0))
        memory = float(row.get("memory", 0))
        entry["max_cpu"] = max(entry["max_cpu"], cpu)
        entry["max_memory"] = max(entry["max_memory"], memory)
        entry["sample_count"] += 1
    return grouped


def analyze_cross_cloud_arbitrage(
    db_path: str,
    organization_id: str,
    *,
    history_limit: int = 500,
    min_savings_usd: float = MIN_ARBITRAGE_SAVINGS_USD,
) -> dict[str, Any]:
    """
    Review active workloads for cross-cloud cost arbitrage opportunities.
    Flags heavy footprints on expensive providers when an identical tier is
    cheaper on another platform (e.g. Azure AKS -> GCP GKE Spot).
    """
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    metrics = fetch_cluster_metrics_history(db_path, org_id, limit=history_limit)
    workloads = _group_active_workloads(metrics)

    recommendations: list[dict[str, Any]] = []
    total_savings = 0.0

    for entry in workloads.values():
        source_provider = entry.get("provider") or DEFAULT_PROVIDER
        source_key = _normalize_cloud_provider(source_provider)
        if source_key not in EXPENSIVE_SOURCE_PROVIDERS:
            continue
        if not _is_heavy_workload(entry):
            continue

        instance_size = entry["instance_size"] or infer_provisioned_size(
            entry["max_cpu"],
            entry["max_memory"],
        )
        arbitrage = _find_cross_cloud_arbitrage_target(source_key, instance_size)
        if arbitrage is None:
            continue
        if float(arbitrage["arbitrage_monthly_savings_usd"]) < min_savings_usd:
            continue

        source_platform = _cross_cloud_platform_name(source_key)
        target_platform = str(arbitrage["target_platform"])
        migration_path = (
            f"Move {entry['workload']} from {source_platform} to {target_platform}"
        )
        current_monthly = _cross_cloud_monthly_cost(source_key, instance_size) or 0.0

        recommendations.append(
            {
                "type": "cross_cloud_arbitrage",
                "action": "cross_cloud_migration",
                "status": "Ready",
                "workload": entry["workload"],
                "namespace": entry["namespace"],
                "pod_name": entry["pod_name"],
                "cluster_id": entry["cluster_id"],
                "source_provider": source_key,
                "source_platform": source_platform,
                "target_provider": arbitrage["target_provider"],
                "target_platform": target_platform,
                "target_billing_model": arbitrage.get("target_billing_model"),
                "instance_size": instance_size,
                "migration_path": migration_path,
                "current_monthly_usd": current_monthly,
                "target_monthly_usd": arbitrage["target_monthly_usd"],
                "arbitrage_monthly_savings_usd": arbitrage["arbitrage_monthly_savings_usd"],
                "max_cpu_pct": round(entry["max_cpu"], 2),
                "max_memory_pct": round(entry["max_memory"], 2),
                "sample_count": entry["sample_count"],
                "rationale": (
                    f"Heavy workload {entry['workload']} on {source_platform} "
                    f"({instance_size}) costs ${current_monthly:,.2f}/mo vs "
                    f"${arbitrage['target_monthly_usd']:,.2f}/mo on {target_platform}. "
                    f"Net arbitrage: ${arbitrage['arbitrage_monthly_savings_usd']:,.2f}/mo."
                ),
            }
        )
        total_savings += float(arbitrage["arbitrage_monthly_savings_usd"])

    recommendations.sort(
        key=lambda item: float(item.get("arbitrage_monthly_savings_usd", 0)),
        reverse=True,
    )

    if recommendations:
        logger.info(
            "[Optimizer] Cross-cloud arbitrage: %d opportunity(ies) for org=%s "
            "(total savings $%.2f/mo)",
            len(recommendations),
            org_id,
            total_savings,
        )

    return {
        "organization_id": org_id,
        "workloads_analyzed": len(workloads),
        "cross_cloud_recommendations": recommendations,
        "total_arbitrage_monthly_savings_usd": round(total_savings, 2),
        "pricing_matrix_providers": list(CROSS_CLOUD_PROVIDER_KEYS),
    }


def analyze_rightsizing_recommendations(
    db_path: str,
    organization_id: str,
    *,
    tenant_id: str | None = None,
    provider: str = DEFAULT_PROVIDER,
    analysis_hours: int = DEFAULT_ANALYSIS_HOURS,
) -> dict[str, Any]:
    """
    Evaluate the last 24 hours of cluster snapshots and recommend node or replica
    downscaling when average CPU or memory utilization stays below 30%.
    Includes any active predictive scale-up recommendation for the organization.
    """
    provider_key = provider.lower()
    if provider_key not in SUPPORTED_PROVIDERS:
        provider_key = DEFAULT_PROVIDER

    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    tenant = str(tenant_id or org_id).strip() or org_id

    snapshots = fetch_cluster_snapshots(
        db_path,
        org_id,
        hours=analysis_hours,
    )

    forecast_report = predict_next_window_utilization(db_path, org_id, hours_ahead=1)
    predictive_entry = get_predictive_recommendation(org_id)
    arbitrage_report = analyze_cross_cloud_arbitrage(db_path, org_id)
    cross_cloud_recs = list(arbitrage_report.get("cross_cloud_recommendations") or [])

    if not snapshots:
        predictive_recs = (
            [predictive_entry["recommendation"]]
            if predictive_entry and predictive_entry.get("recommendation")
            else []
        )
        combined = predictive_recs + cross_cloud_recs
        return {
            "status": "ok",
            "tenant_id": tenant,
            "organization_id": org_id,
            "provider": provider_key,
            "analysis_window_hours": analysis_hours,
            "snapshot_count": 0,
            "underutilization_threshold_pct": LOW_UTILIZATION_THRESHOLD_PCT,
            "predictive_upper_threshold_pct": PREDICTIVE_UPPER_THRESHOLD_PCT,
            "underutilized": False,
            "cluster_summary": None,
            "utilization_forecast": forecast_report.get("forecast"),
            "recommendations": combined,
            "predictive_recommendations": predictive_recs,
            "cross_cloud_recommendations": cross_cloud_recs,
            "total_arbitrage_monthly_savings_usd": arbitrage_report.get(
                "total_arbitrage_monthly_savings_usd", 0.0
            ),
            "total_monthly_savings_usd": round(
                float(arbitrage_report.get("total_arbitrage_monthly_savings_usd") or 0),
                2,
            ),
            "message": (
                f"No cluster snapshots found in the last {analysis_hours} hours. "
                "Ensure the metrics collector telemetry daemon is running."
            ),
        }

    cpu_values = [float(row["cpu_utilization"]) for row in snapshots]
    memory_values = [float(row["memory_utilization"]) for row in snapshots]
    node_values = [int(row["node_count"]) for row in snapshots]
    pod_values = [int(row["pod_count"]) for row in snapshots]

    avg_cpu = round(_average(cpu_values), 2)
    avg_memory = round(_average(memory_values), 2)
    avg_nodes = round(_average([float(v) for v in node_values]), 1)
    avg_pods = round(_average([float(v) for v in pod_values]), 1)
    latest = snapshots[0]

    underutilized = (
        avg_cpu < LOW_UTILIZATION_THRESHOLD_PCT
        or avg_memory < LOW_UTILIZATION_THRESHOLD_PCT
    )

    cluster_summary = {
        "cluster_id": str(latest.get("cluster_id") or "omnikube-cluster"),
        "avg_cpu_utilization_pct": avg_cpu,
        "avg_memory_utilization_pct": avg_memory,
        "avg_node_count": avg_nodes,
        "avg_pod_count": avg_pods,
        "latest_snapshot": {
            "timestamp": latest.get("timestamp"),
            "node_count": int(latest.get("node_count", 0)),
            "pod_count": int(latest.get("pod_count", 0)),
            "cpu_utilization_pct": float(latest.get("cpu_utilization", 0)),
            "memory_utilization_pct": float(latest.get("memory_utilization", 0)),
        },
    }

    recommendations: list[dict[str, Any]] = []
    total_monthly_savings = 0.0

    if predictive_entry and predictive_entry.get("recommendation"):
        recommendations.append(dict(predictive_entry["recommendation"]))

    recommendations.extend(cross_cloud_recs)
    total_monthly_savings += float(
        arbitrage_report.get("total_arbitrage_monthly_savings_usd") or 0
    )

    if underutilized:
        peak_utilization = max(avg_cpu, avg_memory)
        current_node_size = infer_provisioned_size(peak_utilization, peak_utilization)
        if peak_utilization < 5.0:
            current_node_size = DEFAULT_NODE_INSTANCE_SIZE

        recommended_node_size = recommend_downscale_size(current_node_size)
        node_count = max(1, round(avg_nodes))

        if recommended_node_size:
            per_node_savings = compute_downscale_savings(
                provider_key,
                current_node_size,
                target_size=recommended_node_size,
            )
            if per_node_savings:
                monthly_savings = round(
                    float(per_node_savings["monthly_savings_usd"]) * node_count,
                    2,
                )
                if monthly_savings > 0:
                    recommendations.append(
                        {
                            "type": "node_rightsizing",
                            "action": "downscale_instance",
                            "status": "Ready",
                            "current_node_spec": per_node_savings["current_size"],
                            "recommended_node_spec": per_node_savings["recommended_size"],
                            "current_node_count": node_count,
                            "recommended_node_count": node_count,
                            "current_monthly_usd": round(
                                float(per_node_savings["current_monthly_usd"]) * node_count,
                                2,
                            ),
                            "recommended_monthly_usd": round(
                                float(per_node_savings["recommended_monthly_usd"]) * node_count,
                                2,
                            ),
                            "monthly_savings_usd": monthly_savings,
                            "rationale": (
                                f"Average cluster utilization is {peak_utilization:.1f}% "
                                f"(below {LOW_UTILIZATION_THRESHOLD_PCT:.0f}% threshold). "
                                f"Downscale {node_count} node(s) from "
                                f"{per_node_savings['current_size']} to "
                                f"{per_node_savings['recommended_size']}."
                            ),
                        }
                    )
                    total_monthly_savings += monthly_savings

        current_replicas = max(1, round(avg_pods))
        utilization_for_replicas = max(avg_cpu, avg_memory)
        recommended_replicas = _round_replica_target(
            current_replicas,
            utilization_for_replicas,
        )

        if recommended_replicas < current_replicas:
            baseline_node_cost = estimate_monthly_cost(provider_key, "small")
            per_replica_cost = (
                float(baseline_node_cost) / PODS_PER_NODE_BASELINE
                if baseline_node_cost is not None
                else 0.0
            )
            replicas_removed = current_replicas - recommended_replicas
            replica_savings = round(replicas_removed * per_replica_cost, 2)

            if replica_savings > 0:
                recommendations.append(
                    {
                        "type": "replica_rightsizing",
                        "action": "reduce_replica_count",
                        "status": "Ready",
                        "current_replica_count": current_replicas,
                        "recommended_replica_count": recommended_replicas,
                        "replicas_to_remove": replicas_removed,
                        "estimated_per_replica_monthly_usd": round(per_replica_cost, 2),
                        "monthly_savings_usd": replica_savings,
                        "rationale": (
                            f"Average utilization ({utilization_for_replicas:.1f}%) suggests "
                            f"{current_replicas} replicas exceed demand; "
                            f"scale to {recommended_replicas} replicas."
                        ),
                    }
                )
                total_monthly_savings += replica_savings

    recommendations.sort(
        key=lambda item: (
            0 if item.get("type") == "predictive_scale_up" else 1,
            -float(
                item.get("arbitrage_monthly_savings_usd")
                or item.get("monthly_savings_usd", 0)
                or 0
            ),
        )
    )

    predictive_recs = [rec for rec in recommendations if rec.get("predictive")]
    cross_cloud_only = [rec for rec in recommendations if rec.get("type") == "cross_cloud_arbitrage"]

    return {
        "status": "ok",
        "tenant_id": tenant,
        "organization_id": org_id,
        "provider": provider_key,
        "analysis_window_hours": analysis_hours,
        "snapshot_count": len(snapshots),
        "underutilization_threshold_pct": LOW_UTILIZATION_THRESHOLD_PCT,
        "predictive_upper_threshold_pct": PREDICTIVE_UPPER_THRESHOLD_PCT,
        "underutilized": underutilized,
        "cluster_summary": cluster_summary,
        "utilization_forecast": forecast_report.get("forecast"),
        "recommendations": recommendations,
        "predictive_recommendations": predictive_recs,
        "cross_cloud_recommendations": cross_cloud_only,
        "total_arbitrage_monthly_savings_usd": round(
            float(arbitrage_report.get("total_arbitrage_monthly_savings_usd") or 0),
            2,
        ),
        "total_monthly_savings_usd": round(total_monthly_savings, 2),
    }
