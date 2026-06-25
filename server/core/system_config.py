import logging
import os
import sqlite3
import threading
from typing import Any

from core.database import DEFAULT_ORGANIZATION_ID, init_system_configs_schema

logger = logging.getLogger(__name__)

KEY_CPU_THRESHOLD = "cpu_threshold"
KEY_MEMORY_THRESHOLD = "memory_threshold"

DEFAULT_THRESHOLDS: dict[str, float] = {"cpu": 80.0, "memory": 80.0}

KEY_SLACK_WEBHOOK_URL = "slack_webhook_url"
KEY_DISCORD_WEBHOOK_URL = "discord_webhook_url"
KEY_SLACK_ENABLED = "slack_enabled"
KEY_DISCORD_ENABLED = "discord_enabled"
KEY_COST_ALERT_LIMIT_USD = "cost_alert_limit_usd"
KEY_MONTHLY_BUDGET_CEILING_USD = "monthly_budget_ceiling_usd"
KEY_MONTHLY_SPEND_USD = "monthly_spend_usd"

# Legacy keys may still exist in system_configs from older deployments; they are ignored.
LEGACY_NOTIFICATION_KEYS = frozenset(
    {
        "email_recipient",
        "email_enabled",
        "smtp_server",
        "smtp_port",
        "smtp_user",
        "smtp_password",
    }
)

DEFAULT_NOTIFICATION_CONFIG: dict[str, str] = {
    KEY_SLACK_ENABLED: "false",
    KEY_DISCORD_ENABLED: "false",
    KEY_SLACK_WEBHOOK_URL: "",
    KEY_DISCORD_WEBHOOK_URL: "",
    KEY_COST_ALERT_LIMIT_USD: "5000",
    KEY_MONTHLY_BUDGET_CEILING_USD: "10000",
    KEY_MONTHLY_SPEND_USD: "0",
}

NOTIFICATION_CONFIG_KEYS = tuple(DEFAULT_NOTIFICATION_CONFIG.keys())
BUDGET_CONFIG_KEYS = (
    KEY_MONTHLY_BUDGET_CEILING_USD,
    KEY_MONTHLY_SPEND_USD,
    KEY_COST_ALERT_LIMIT_USD,
)

_lock = threading.Lock()


def _parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("true", "1", "yes", "on")


def _threshold_seed_values() -> tuple[str, str]:
    cpu = os.environ.get("OMNIKUBE_CPU_THRESHOLD", str(DEFAULT_THRESHOLDS["cpu"]))
    memory = os.environ.get("OMNIKUBE_MEMORY_THRESHOLD", str(DEFAULT_THRESHOLDS["memory"]))
    return cpu, memory


def _seed_rows(organization_id: str) -> list[tuple[str, str, str]]:
    cpu, memory = _threshold_seed_values()
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    rows = [
        (org_id, KEY_CPU_THRESHOLD, cpu),
        (org_id, KEY_MEMORY_THRESHOLD, memory),
    ]
    rows.extend(
        (org_id, key, value) for key, value in DEFAULT_NOTIFICATION_CONFIG.items()
    )
    return rows


def init_system_configs(db_path: str, organization_id: str = DEFAULT_ORGANIZATION_ID) -> None:
    try:
        init_system_configs_schema(db_path)
        org_id = organization_id or DEFAULT_ORGANIZATION_ID
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO system_configs (organization_id, config_key, config_value)
                VALUES (?, ?, ?)
                """,
                _seed_rows(org_id),
            )
            conn.commit()
        logger.info(
            "System configs initialized for organization=%s (thresholds + notification routing keys)",
            org_id,
        )
    except sqlite3.Error as exc:
        logger.error("System config initialization failed: %s", exc)


def _read_config_keys(
    db_path: str,
    keys: tuple[str, ...],
    organization_id: str,
) -> dict[str, str]:
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    placeholders = ", ".join("?" for _ in keys)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT config_key, config_value
            FROM system_configs
            WHERE organization_id = ?
              AND config_key IN ({placeholders})
            """,
            (org_id, *keys),
        ).fetchall()
    return {key: value for key, value in rows}


def get_thresholds(
    db_path: str,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, float]:
    try:
        values = _read_config_keys(
            db_path,
            (KEY_CPU_THRESHOLD, KEY_MEMORY_THRESHOLD),
            organization_id,
        )
        return {
            "cpu": float(values.get(KEY_CPU_THRESHOLD, DEFAULT_THRESHOLDS["cpu"])),
            "memory": float(values.get(KEY_MEMORY_THRESHOLD, DEFAULT_THRESHOLDS["memory"])),
        }
    except (sqlite3.Error, TypeError, ValueError) as exc:
        logger.warning("Threshold read failed, using defaults: %s", exc)
        return dict(DEFAULT_THRESHOLDS)


def get_notification_config(
    db_path: str,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, str]:
    try:
        values = _read_config_keys(db_path, NOTIFICATION_CONFIG_KEYS, organization_id)
        return {key: str(values.get(key, default)) for key, default in DEFAULT_NOTIFICATION_CONFIG.items()}
    except sqlite3.Error as exc:
        logger.warning("Notification config read failed, using defaults: %s", exc)
        return dict(DEFAULT_NOTIFICATION_CONFIG)


def update_thresholds(
    db_path: str,
    cpu: float,
    memory: float,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, float]:
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    with _lock:
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO system_configs (organization_id, config_key, config_value)
                VALUES (?, ?, ?)
                ON CONFLICT(organization_id, config_key) DO UPDATE SET
                    config_value = excluded.config_value
                """,
                [
                    (org_id, KEY_CPU_THRESHOLD, str(cpu)),
                    (org_id, KEY_MEMORY_THRESHOLD, str(memory)),
                ],
            )
            conn.commit()

    thresholds = get_thresholds(db_path, org_id)
    logger.info(
        "Thresholds hot-reloaded: cpu=%.1f memory=%.1f",
        thresholds["cpu"],
        thresholds["memory"],
    )
    return thresholds


def update_notification_config(
    db_path: str,
    updates: dict[str, Any],
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, str]:
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    rows: list[tuple[str, str, str]] = []
    for key in NOTIFICATION_CONFIG_KEYS:
        if key in updates:
            rows.append((org_id, key, str(updates[key]).strip()))

    ignored_legacy = [key for key in updates if key in LEGACY_NOTIFICATION_KEYS]
    if ignored_legacy:
        logger.debug("Ignoring legacy notification keys: %s", ", ".join(sorted(ignored_legacy)))

    if not rows:
        return get_notification_config(db_path, org_id)

    with _lock:
        with sqlite3.connect(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO system_configs (organization_id, config_key, config_value)
                VALUES (?, ?, ?)
                ON CONFLICT(organization_id, config_key) DO UPDATE SET
                    config_value = excluded.config_value
                """,
                rows,
            )
            conn.commit()

    config = get_notification_config(db_path, org_id)
    logger.info(
        "Notification routing hot-reloaded: slack=%s discord=%s",
        bool(config[KEY_SLACK_WEBHOOK_URL]),
        bool(config[KEY_DISCORD_WEBHOOK_URL]),
    )
    return config


def sync_config_store_thresholds(
    config_store: Any,
    thresholds: dict[str, float],
) -> None:
    config_store.update(
        {
            "cpu_alert_threshold": thresholds["cpu"],
            "memory_alert_threshold": thresholds["memory"],
        }
    )


def get_integrations(
    db_path: str,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, Any]:
    cfg = get_notification_config(db_path, organization_id)
    slack_enabled = _parse_bool(cfg[KEY_SLACK_ENABLED])
    discord_enabled = _parse_bool(cfg[KEY_DISCORD_ENABLED])
    slack_url = cfg[KEY_SLACK_WEBHOOK_URL]
    discord_url = cfg[KEY_DISCORD_WEBHOOK_URL]

    return {
        "slack": {
            "enabled": slack_enabled,
            "webhook_url": slack_url,
        },
        "discord": {
            "enabled": discord_enabled,
            "webhook_url": discord_url,
        },
        "cost_alert_limit_usd": float(cfg.get(KEY_COST_ALERT_LIMIT_USD, "5000") or 5000),
    }


def update_integrations(
    db_path: str,
    payload: dict[str, Any],
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, Any]:
    updates: dict[str, str] = {}

    slack = payload.get("slack")
    if isinstance(slack, dict):
        if "enabled" in slack:
            updates[KEY_SLACK_ENABLED] = "true" if bool(slack.get("enabled")) else "false"
        if "webhook_url" in slack:
            updates[KEY_SLACK_WEBHOOK_URL] = str(slack.get("webhook_url", "")).strip()

    discord = payload.get("discord")
    if isinstance(discord, dict):
        if "enabled" in discord:
            updates[KEY_DISCORD_ENABLED] = "true" if bool(discord.get("enabled")) else "false"
        if "webhook_url" in discord:
            updates[KEY_DISCORD_WEBHOOK_URL] = str(discord.get("webhook_url", "")).strip()

    if "cost_alert_limit_usd" in payload:
        updates[KEY_COST_ALERT_LIMIT_USD] = str(float(payload["cost_alert_limit_usd"]))

    if updates:
        update_notification_config(db_path, updates, organization_id)

    return get_integrations(db_path, organization_id)


def get_budget_guardrails(
    db_path: str,
    organization_id: str = DEFAULT_ORGANIZATION_ID,
) -> dict[str, float]:
    """Return monthly budget ceiling, recorded spend, and remaining headroom."""
    org_id = organization_id or DEFAULT_ORGANIZATION_ID
    try:
        values = _read_config_keys(db_path, BUDGET_CONFIG_KEYS, org_id)
        ceiling = float(
            values.get(KEY_MONTHLY_BUDGET_CEILING_USD)
            or values.get(KEY_COST_ALERT_LIMIT_USD, "10000")
            or 10000
        )
        spend = float(values.get(KEY_MONTHLY_SPEND_USD, "0") or 0)
        remaining = round(max(0.0, ceiling - spend), 2)
        return {
            "monthly_ceiling_usd": round(ceiling, 2),
            "monthly_spend_usd": round(spend, 2),
            "remaining_usd": remaining,
        }
    except (sqlite3.Error, TypeError, ValueError) as exc:
        logger.warning("Budget guardrail read failed, using defaults: %s", exc)
        return {
            "monthly_ceiling_usd": 10000.0,
            "monthly_spend_usd": 0.0,
            "remaining_usd": 10000.0,
        }
