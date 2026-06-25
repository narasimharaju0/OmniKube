"""Mock hourly infrastructure pricing for standard cloud machine types."""

from typing import Any, TypedDict


class InstancePricing(TypedDict):
    vcpu: int
    memory_gb: int
    hourly_usd: float


class ProviderPricing(TypedDict):
    small: InstancePricing
    medium: InstancePricing
    large: InstancePricing


CLOUD_PRICING_MATRIX: dict[str, ProviderPricing] = {
    "aws": {
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0416},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0832},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1664},
    },
    "gcp": {
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0380},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0760},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1520},
    },
    "azure": {
        "small": {"vcpu": 2, "memory_gb": 4, "hourly_usd": 0.0396},
        "medium": {"vcpu": 4, "memory_gb": 8, "hourly_usd": 0.0792},
        "large": {"vcpu": 8, "memory_gb": 16, "hourly_usd": 0.1584},
    },
}

INSTANCE_SIZES: tuple[str, ...] = ("small", "medium", "large")
SUPPORTED_PROVIDERS: tuple[str, ...] = tuple(CLOUD_PRICING_MATRIX.keys())
INSTANCE_SIZE_ORDER: dict[str, int] = {size: index for index, size in enumerate(INSTANCE_SIZES)}


def get_instance_pricing(
    provider: str,
    size: str,
) -> InstancePricing | None:
    provider_key = provider.lower()
    size_key = size.lower()
    provider_pricing = CLOUD_PRICING_MATRIX.get(provider_key)
    if provider_pricing is None:
        return None
    return provider_pricing.get(size_key)  # type: ignore[return-value]


def estimate_monthly_cost(
    provider: str,
    size: str,
    *,
    hours_per_month: float = 730.0,
) -> float | None:
    pricing = get_instance_pricing(provider, size)
    if pricing is None:
        return None
    return round(pricing["hourly_usd"] * hours_per_month, 2)


def list_provider_pricing(provider: str) -> dict[str, Any] | None:
    provider_key = provider.lower()
    pricing = CLOUD_PRICING_MATRIX.get(provider_key)
    if pricing is None:
        return None
    return dict(pricing)


def recommend_downscale_size(current_size: str) -> str | None:
    size_key = current_size.lower()
    current_rank = INSTANCE_SIZE_ORDER.get(size_key)
    if current_rank is None or current_rank == 0:
        return None
    return INSTANCE_SIZES[current_rank - 1]


def recommend_upscale_size(current_size: str) -> str | None:
    size_key = current_size.lower()
    current_rank = INSTANCE_SIZE_ORDER.get(size_key)
    if current_rank is None or current_rank >= len(INSTANCE_SIZES) - 1:
        return None
    return INSTANCE_SIZES[current_rank + 1]


def infer_provisioned_size(max_cpu_pct: float, max_memory_pct: float) -> str:
    """Estimate provisioned instance tier from observed utilization peaks."""
    peak = max(max_cpu_pct, max_memory_pct)
    if peak < 15:
        return "medium"
    if peak < 50:
        return "medium"
    if peak < 80:
        return "large"
    return "large"


def compute_downscale_savings(
    provider: str,
    current_size: str,
    *,
    target_size: str | None = None,
    hours_per_month: float = 730.0,
) -> dict[str, Any] | None:
    provider_key = provider.lower()
    current_key = current_size.lower()
    target_key = (target_size or recommend_downscale_size(current_key) or "").lower()

    current_cost = estimate_monthly_cost(provider_key, current_key, hours_per_month=hours_per_month)
    if current_cost is None:
        return None

    if not target_key:
        return {
            "provider": provider_key,
            "current_size": current_key,
            "recommended_size": None,
            "current_monthly_usd": current_cost,
            "recommended_monthly_usd": current_cost,
            "monthly_savings_usd": 0.0,
        }

    recommended_cost = estimate_monthly_cost(
        provider_key, target_key, hours_per_month=hours_per_month
    )
    if recommended_cost is None:
        return None

    savings = round(max(0.0, current_cost - recommended_cost), 2)
    return {
        "provider": provider_key,
        "current_size": current_key,
        "recommended_size": target_key,
        "current_monthly_usd": current_cost,
        "recommended_monthly_usd": recommended_cost,
        "monthly_savings_usd": savings,
    }


def compute_upscale_cost(
    provider: str,
    current_size: str,
    *,
    target_size: str | None = None,
    node_count: int = 1,
    hours_per_month: float = 730.0,
) -> dict[str, Any] | None:
    """Estimate incremental monthly cost for scaling infrastructure up one tier."""
    provider_key = provider.lower()
    current_key = current_size.lower()
    target_key = (target_size or recommend_upscale_size(current_key) or "").lower()
    if not target_key:
        return None

    current_cost = estimate_monthly_cost(provider_key, current_key, hours_per_month=hours_per_month)
    target_cost = estimate_monthly_cost(provider_key, target_key, hours_per_month=hours_per_month)
    if current_cost is None or target_cost is None:
        return None

    per_node_delta = round(max(0.0, target_cost - current_cost), 2)
    total_delta = round(per_node_delta * max(1, int(node_count)), 2)
    return {
        "provider": provider_key,
        "current_size": current_key,
        "recommended_size": target_key,
        "node_count": max(1, int(node_count)),
        "per_node_monthly_usd": per_node_delta,
        "projected_monthly_usd": total_delta,
    }
