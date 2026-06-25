"""Demo telemetry seeding for cost optimization and RCA dashboards."""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from core.database import DEFAULT_ORGANIZATION_ID, insert_cluster_snapshot
from core.metrics_collector import insert_metric

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"

IDLE_WORKLOAD_TEMPLATES: tuple[dict[str, str], ...] = (
    {"pod_name": "cache-redis-0", "namespace": "data", "instance_size": "medium"},
    {"pod_name": "worker-batch-7", "namespace": "jobs", "instance_size": "large"},
    {"pod_name": "api-staging-2", "namespace": "staging", "instance_size": "medium"},
    {"pod_name": "metrics-sidecar-1", "namespace": "observability", "instance_size": "small"},
    {"pod_name": "archive-worker-3", "namespace": "jobs", "instance_size": "medium"},
    {"pod_name": "legacy-svc-0", "namespace": "legacy", "instance_size": "large"},
)

SPIKE_WORKLOAD_TEMPLATES: tuple[dict[str, str], ...] = (
    {"pod_name": "api-server-7f8d9", "namespace": "prod", "instance_size": "large"},
    {"pod_name": "checkout-api-4b2c1", "namespace": "prod", "instance_size": "medium"},
)


def _timestamp_at(day_offset: int, hour: int) -> str:
    moment = datetime.now(timezone.utc) - timedelta(days=day_offset, hours=hour)
    return moment.strftime(TIMESTAMP_FORMAT)


def seed_demo_telemetry(
    db_path: str,
    tenant_id: str,
    *,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
    idle_nodes: int = 4,
    total_cores: int = 32,
    memory_gb: int = 128,
    provider: str = "aws",
    simulated_days: int = 7,
    samples_per_day: int = 4,
) -> dict[str, Any]:
    """Insert realistic idle, steady, and spike workload metrics for a tenant."""
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    idle_nodes = max(1, min(int(idle_nodes), len(IDLE_WORKLOAD_TEMPLATES)))
    simulated_days = max(1, min(int(simulated_days), 30))
    samples_per_day = max(1, min(int(samples_per_day), 12))
    provider_key = provider.lower()

    inserted = 0
    idle_workloads: list[str] = []
    spike_workloads: list[str] = []

    for index in range(idle_nodes):
        template = IDLE_WORKLOAD_TEMPLATES[index % len(IDLE_WORKLOAD_TEMPLATES)]
        workload_name = f"{template['namespace']}/{template['pod_name']}"
        idle_workloads.append(workload_name)

        for day in range(simulated_days):
            for sample in range(samples_per_day):
                cpu = round(1.0 + (index * 0.4) + (sample * 0.2), 1)
                memory = round(2.0 + (index * 0.8) + (sample * 0.3), 1)
                labels = {
                    "pod_name": template["pod_name"],
                    "namespace": template["namespace"],
                    "cluster_id": "omnikube-cluster",
                    "cloud_provider": provider_key,
                    "provider": provider_key,
                    "instance_size": template["instance_size"],
                    "total_cores": total_cores,
                    "memory_gb": memory_gb,
                    "workload_type": "idle",
                }
                insert_metric(
                    db_path,
                    cpu,
                    memory,
                    labels,
                    tenant_id=tenant_id,
                    organization_id=org_id,
                    timestamp=_timestamp_at(day, sample * 2),
                )
                inserted += 1

    for index, template in enumerate(SPIKE_WORKLOAD_TEMPLATES):
        workload_name = f"{template['namespace']}/{template['pod_name']}"
        spike_workloads.append(workload_name)
        for day in range(min(3, simulated_days)):
            base_hour = 10 + index
            labels = {
                "pod_name": template["pod_name"],
                "namespace": template["namespace"],
                "cluster_id": "omnikube-cluster",
                "cloud_provider": provider_key,
                "provider": provider_key,
                "instance_size": template["instance_size"],
                "workload_type": "spike",
            }
            insert_metric(
                db_path,
                42.0 + index * 5,
                55.0 + index * 4,
                labels,
                tenant_id=tenant_id,
                organization_id=org_id,
                timestamp=_timestamp_at(day, base_hour),
            )
            inserted += 1
            insert_metric(
                db_path,
                88.0 + index * 3,
                82.0 + index * 2,
                labels,
                tenant_id=tenant_id,
                organization_id=org_id,
                timestamp=_timestamp_at(day, base_hour + 1),
            )
            inserted += 1

    snapshot_hours = min(simulated_days * 24, 48)
    snapshots_inserted = 0
    for hour in range(snapshot_hours):
        insert_cluster_snapshot(
            db_path,
            node_count=3,
            pod_count=24 - (hour % 4),
            cpu_utilization=round(18.0 + (hour % 5), 1),
            memory_utilization=round(22.0 + (hour % 4), 1),
            organization_id=org_id,
            tenant_id=tenant_id,
            cluster_id="omnikube-cluster",
            timestamp=_timestamp_at(0, hour),
        )
        snapshots_inserted += 1

    logger.info(
        "Seeded %d demo metric rows and %d cluster snapshots for tenant=%s "
        "organization=%s (idle_nodes=%d provider=%s)",
        inserted,
        snapshots_inserted,
        tenant_id,
        org_id,
        idle_nodes,
        provider_key,
    )

    return {
        "status": "success",
        "tenant_id": tenant_id,
        "organization_id": org_id,
        "inserted": inserted,
        "cluster_snapshots_inserted": snapshots_inserted,
        "idle_nodes": idle_nodes,
        "idle_workloads": idle_workloads,
        "spike_workloads": spike_workloads,
        "provider": provider_key,
        "simulated_days": simulated_days,
        "total_cores": total_cores,
        "memory_gb": memory_gb,
    }


def clear_tenant_telemetry(
    db_path: str,
    tenant_id: str,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> int:
    org_id = str(organization_id or tenant_id or DEFAULT_ORGANIZATION_ID).strip()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM cluster_metrics
            WHERE tenant_id = ? AND organization_id = ?
            """,
            (tenant_id, org_id),
        )
        conn.commit()
        return int(cursor.rowcount)
