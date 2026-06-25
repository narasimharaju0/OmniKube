import logging
import sqlite3
from typing import Any

from core.auth import DEFAULT_TENANT_ID

logger = logging.getLogger(__name__)

DEFAULT_PRIMARY_COLOR = "#2563eb"
DEFAULT_SECONDARY_COLOR = "#1e40af"

DEFAULT_BRANDING: dict[str, str] = {
    "company_name": "OmniKube",
    "logo_url": "",
    "primary_color": DEFAULT_PRIMARY_COLOR,
    "secondary_color": DEFAULT_SECONDARY_COLOR,
}

BRANDING_COLUMNS = (
    "tenant_id",
    "company_name",
    "logo_url",
    "primary_color",
    "secondary_color",
)


def init_tenant_branding(db_path: str) -> None:
    """Create tenant_branding table and seed the default tenant row."""
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenants (
                    tenant_id TEXT PRIMARY KEY
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenant_branding (
                    tenant_id TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    logo_url TEXT NOT NULL DEFAULT '',
                    primary_color TEXT NOT NULL,
                    secondary_color TEXT NOT NULL,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
                )
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO tenants (tenant_id) VALUES (?)",
                (DEFAULT_TENANT_ID,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO tenant_branding (
                    tenant_id, company_name, logo_url, primary_color, secondary_color
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    DEFAULT_TENANT_ID,
                    DEFAULT_BRANDING["company_name"],
                    DEFAULT_BRANDING["logo_url"],
                    DEFAULT_BRANDING["primary_color"],
                    DEFAULT_BRANDING["secondary_color"],
                ),
            )
            conn.commit()
        logger.info("Tenant branding table initialized")
    except sqlite3.Error as exc:
        logger.error("Tenant branding initialization failed: %s", exc)


def ensure_tenant_exists(db_path: str, tenant_id: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT OR IGNORE INTO tenants (tenant_id) VALUES (?)",
            (tenant_id,),
        )
        conn.commit()


def get_tenant_branding(db_path: str, tenant_id: str) -> dict[str, Any] | None:
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT tenant_id, company_name, logo_url, primary_color, secondary_color
                FROM tenant_branding
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            ).fetchone()
        if row is None:
            return None
        return {key: row[key] for key in BRANDING_COLUMNS}
    except sqlite3.Error as exc:
        logger.error("Failed to read tenant branding for %s: %s", tenant_id, exc)
        return None


def upsert_tenant_branding(
    db_path: str,
    tenant_id: str,
    *,
    company_name: str,
    logo_url: str = "",
    primary_color: str = DEFAULT_PRIMARY_COLOR,
    secondary_color: str = DEFAULT_SECONDARY_COLOR,
) -> dict[str, Any]:
    ensure_tenant_exists(db_path, tenant_id)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            INSERT INTO tenant_branding (
                tenant_id, company_name, logo_url, primary_color, secondary_color
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id) DO UPDATE SET
                company_name = excluded.company_name,
                logo_url = excluded.logo_url,
                primary_color = excluded.primary_color,
                secondary_color = excluded.secondary_color
            """,
            (tenant_id, company_name, logo_url, primary_color, secondary_color),
        )
        conn.commit()
    branding = get_tenant_branding(db_path, tenant_id)
    if branding is None:
        raise RuntimeError(f"Failed to persist branding for tenant {tenant_id}")
    return branding
