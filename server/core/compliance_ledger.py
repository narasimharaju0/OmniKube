"""Immutable compliance vault ledger for regulatory optimization audit trails."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from core.database import DEFAULT_ORGANIZATION_ID

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S UTC"
GENESIS_BLOCK_HASH = "0" * 64


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)


def init_compliance_ledger(db_path: str) -> None:
    """Ensure the compliance vault table exists."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_ledger_vault (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    organization_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    action_taken TEXT NOT NULL,
                    initiated_by TEXT NOT NULL,
                    original_cost_usd REAL NOT NULL DEFAULT 0,
                    optimized_cost_usd REAL NOT NULL DEFAULT 0,
                    immutable_block_sha256 TEXT NOT NULL,
                    previous_block_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_compliance_ledger_org_ts
                ON compliance_ledger_vault(organization_id, id ASC)
                """
            )
            conn.commit()
        logger.info("Compliance ledger vault initialized")
    except sqlite3.Error as exc:
        logger.error("Compliance ledger initialization failed: %s", exc)


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def compute_immutable_block_sha256(
    record_payload: dict[str, Any],
    *,
    previous_block_sha256: str,
) -> str:
    """Hash the canonical record payload chained to the prior vault block."""
    chained = {
        **record_payload,
        "previous_block_sha256": previous_block_sha256,
    }
    digest = hashlib.sha256(_canonical_json(chained).encode("utf-8")).hexdigest()
    return digest


def _fetch_latest_block_hash(db_path: str, organization_id: str) -> str:
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT immutable_block_sha256
                FROM compliance_ledger_vault
                WHERE organization_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (org_id,),
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    except sqlite3.Error as exc:
        logger.error("Compliance ledger head lookup failed: %s", exc)
    return GENESIS_BLOCK_HASH


def append_compliance_ledger_record(
    db_path: str,
    *,
    organization_id: str,
    tenant_id: str,
    action_taken: str,
    initiated_by: str,
    original_cost_usd: float,
    optimized_cost_usd: float,
    metadata: dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """
    Synchronously append a signed audit record to the immutable compliance vault.
    """
    init_compliance_ledger(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    tenant = str(tenant_id).strip() or org_id
    ts = timestamp or utc_timestamp()
    previous_hash = _fetch_latest_block_hash(db_path, org_id)

    record_payload = {
        "timestamp": ts,
        "organization_id": org_id,
        "tenant_id": tenant,
        "action_taken": str(action_taken),
        "initiated_by": str(initiated_by),
        "original_cost_usd": round(float(original_cost_usd), 2),
        "optimized_cost_usd": round(float(optimized_cost_usd), 2),
        "metadata": metadata or {},
    }
    block_hash = compute_immutable_block_sha256(
        record_payload,
        previous_block_sha256=previous_hash,
    )

    payload_json = _canonical_json(record_payload)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO compliance_ledger_vault (
                timestamp, organization_id, tenant_id, action_taken, initiated_by,
                original_cost_usd, optimized_cost_usd, immutable_block_sha256,
                previous_block_sha256, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                org_id,
                tenant,
                record_payload["action_taken"],
                record_payload["initiated_by"],
                record_payload["original_cost_usd"],
                record_payload["optimized_cost_usd"],
                block_hash,
                previous_hash,
                payload_json,
            ),
        )
        record_id = int(cursor.lastrowid)
        conn.commit()

    audit_record = {
        "id": record_id,
        **record_payload,
        "immutable_block_sha256": block_hash,
        "previous_block_sha256": previous_hash,
        "signed_audit_trail": {
            "algorithm": "SHA-256",
            "chain_linked": previous_hash != GENESIS_BLOCK_HASH,
            "block_hash": block_hash,
            "previous_block_hash": previous_hash,
        },
    }
    logger.info(
        "[ComplianceVault] Appended ledger record id=%s org=%s action=%s hash=%s…",
        record_id,
        org_id,
        action_taken,
        block_hash[:16],
    )
    return audit_record


def fetch_compliance_ledger(
    db_path: str,
    organization_id: str,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Return chronological signed audit trails for an organization."""
    init_compliance_ledger(db_path)
    org_id = str(organization_id).strip() or DEFAULT_ORGANIZATION_ID
    capped_limit = max(1, min(int(limit), 1000))

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    id, timestamp, organization_id, tenant_id, action_taken,
                    initiated_by, original_cost_usd, optimized_cost_usd,
                    immutable_block_sha256, previous_block_sha256, payload_json
                FROM compliance_ledger_vault
                WHERE organization_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (org_id, capped_limit),
            ).fetchall()
    except sqlite3.Error as exc:
        logger.error("Compliance ledger query failed: %s", exc)
        return []

    records: list[dict[str, Any]] = []
    for row in rows:
        metadata: dict[str, Any] = {}
        try:
            parsed = json.loads(str(row["payload_json"] or "{}"))
            if isinstance(parsed, dict):
                metadata = parsed.get("metadata") or {}
        except json.JSONDecodeError:
            metadata = {}

        block_hash = str(row["immutable_block_sha256"])
        previous_hash = str(row["previous_block_sha256"])
        records.append(
            {
                "id": int(row["id"]),
                "timestamp": str(row["timestamp"]),
                "organization_id": str(row["organization_id"]),
                "tenant_id": str(row["tenant_id"]),
                "action_taken": str(row["action_taken"]),
                "initiated_by": str(row["initiated_by"]),
                "original_cost_usd": round(float(row["original_cost_usd"]), 2),
                "optimized_cost_usd": round(float(row["optimized_cost_usd"]), 2),
                "immutable_block_sha256": block_hash,
                "previous_block_sha256": previous_hash,
                "metadata": metadata,
                "signed_audit_trail": {
                    "algorithm": "SHA-256",
                    "chain_linked": previous_hash != GENESIS_BLOCK_HASH,
                    "block_hash": block_hash,
                    "previous_block_hash": previous_hash,
                },
            }
        )
    return records


def format_initiated_by(user: dict[str, Any]) -> str:
    """Build a stable initiated_by string from an authenticated user context."""
    username = str(user.get("username") or user.get("display_name") or "unknown")
    role = str(user.get("role") or "unknown")
    email = str(user.get("email") or "").strip()
    org_id = str(user.get("organization_id") or user.get("tenant_id") or "").strip()
    parts = [f"{username} ({role})"]
    if email:
        parts.append(f"email={email}")
    if org_id:
        parts.append(f"org={org_id}")
    return " | ".join(parts)
