"""Lightweight live alert dispatchers for Slack and Discord."""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

from core.system_config import (
    KEY_COST_ALERT_LIMIT_USD,
    KEY_DISCORD_ENABLED,
    KEY_DISCORD_WEBHOOK_URL,
    KEY_SLACK_ENABLED,
    KEY_SLACK_WEBHOOK_URL,
    _parse_bool,
    get_notification_config,
)

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SEC = 10
DISCORD_EMBED_COLOR = 0xE11D48
DISCORD_COST_COLOR = 0x10B981
SLACK_DANGER_COLOR = "#E11D48"

DOWNSCALE_ACTION_ID = "downscale_workload"
DOWNSCALE_BUTTON_TEXT = "Scale Down Now"


def _load_settings(db_path: str) -> dict[str, str]:
    if not db_path:
        return {}
    return get_notification_config(db_path)


def build_cost_remediation_context(report: dict[str, Any]) -> dict[str, str]:
    """Derive ChatOps remediation metadata from a cost optimization report."""
    recommendations = report.get("recommendations") or []
    if not recommendations:
        return {
            "namespace": "",
            "deployment": "",
            "workload": "",
            "pod_name": "",
            "target": "medium",
            "current_size": "",
            "cluster_id": "omnikube-cluster",
            "tenant_id": str(report.get("tenant_id", "default")),
            "provider": str(report.get("provider", "aws")),
        }

    top = recommendations[0]
    workload = str(top.get("workload") or "")
    namespace = str(top.get("namespace") or "")
    deployment = workload.split("/", 1)[-1] if "/" in workload else str(top.get("pod_name") or "")

    return {
        "namespace": namespace,
        "deployment": deployment,
        "workload": workload,
        "pod_name": str(top.get("pod_name") or ""),
        "target": str(top.get("recommended_size") or top.get("current_size") or "medium"),
        "current_size": str(top.get("current_size") or ""),
        "cluster_id": str(top.get("cluster_id") or "omnikube-cluster"),
        "tenant_id": str(report.get("tenant_id", "default")),
        "provider": str(report.get("provider", "aws")),
    }


def encode_remediation_metadata(context: dict[str, Any]) -> str:
    """Compact JSON payload embedded in Slack button values and Discord embeds."""
    payload = {
        "action": DOWNSCALE_ACTION_ID,
        "namespace": str(context.get("namespace") or ""),
        "deployment": str(context.get("deployment") or ""),
        "workload": str(context.get("workload") or ""),
        "pod_name": str(context.get("pod_name") or ""),
        "target": str(context.get("target") or "medium"),
        "current_size": str(context.get("current_size") or ""),
        "cluster_id": str(context.get("cluster_id") or "omnikube-cluster"),
        "tenant_id": str(context.get("tenant_id") or "default"),
        "provider": str(context.get("provider") or "aws"),
    }
    return json.dumps(payload, separators=(",", ":"))


def build_cost_slack_payload(
    summary: str,
    detail: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Slack Block Kit payload for cost optimization alerts with a remediation button."""
    metadata = encode_remediation_metadata(context)
    namespace = str(context.get("namespace") or "—")
    deployment = str(context.get("deployment") or "—")
    target = str(context.get("target") or "medium")
    cluster_id = str(context.get("cluster_id") or "unknown")

    return {
        "text": summary,
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Cost Optimization Alert", "emoji": True},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*:moneybag: {summary}*\n{detail}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Namespace*\n`{namespace}`"},
                    {"type": "mrkdwn", "text": f"*Deployment*\n`{deployment}`"},
                    {"type": "mrkdwn", "text": f"*Target Size*\n`{target}`"},
                    {"type": "mrkdwn", "text": f"*Cluster*\n`{cluster_id}`"},
                ],
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Remediation context: `{metadata[:300]}`",
                    }
                ],
            },
            {
                "type": "actions",
                "block_id": "cost_optimization_remediation",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": DOWNSCALE_BUTTON_TEXT,
                            "emoji": True,
                        },
                        "style": "primary",
                        "action_id": DOWNSCALE_ACTION_ID,
                        "value": metadata[:2000],
                    }
                ],
            },
        ],
    }


def build_cost_discord_payload(
    summary: str,
    detail: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Discord webhook payload for cost alerts with an interactive remediation button."""
    metadata = encode_remediation_metadata(context)
    namespace = str(context.get("namespace") or "—")
    deployment = str(context.get("deployment") or "—")
    target = str(context.get("target") or "medium")
    cluster_id = str(context.get("cluster_id") or "unknown")

    return {
        "embeds": [
            {
                "title": "Cost Optimization Alert",
                "description": summary[:4096],
                "color": DISCORD_COST_COLOR,
                "fields": [
                    {"name": "Details", "value": detail[:1024], "inline": False},
                    {"name": "Namespace", "value": namespace, "inline": True},
                    {"name": "Deployment", "value": deployment, "inline": True},
                    {"name": "Target Size", "value": target, "inline": True},
                    {"name": "Cluster", "value": cluster_id, "inline": True},
                    {
                        "name": "Remediation Metadata",
                        "value": f"```json\n{metadata[:900]}\n```",
                        "inline": False,
                    },
                ],
                "footer": {"text": "OmniKube CloudMetrics · ChatOps"},
            }
        ],
        "components": [
            {
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,
                        "label": DOWNSCALE_BUTTON_TEXT,
                        "custom_id": DOWNSCALE_ACTION_ID,
                    }
                ],
            }
        ],
    }


def send_slack_alert(message: str, *, db_path: str = "", webhook_url: str | None = None) -> bool:
    """Send a JSON payload to a Slack incoming webhook."""
    settings = _load_settings(db_path)
    url = (webhook_url or settings.get(KEY_SLACK_WEBHOOK_URL, "")).strip()
    if not url:
        logger.debug("[Notifier] Slack skipped — no webhook URL configured")
        return False
    if db_path and not _parse_bool(settings.get(KEY_SLACK_ENABLED, "false")):
        logger.debug("[Notifier] Slack skipped — channel disabled")
        return False

    payload = {
        "text": f":warning: {message}",
        "attachments": [
            {
                "color": SLACK_DANGER_COLOR,
                "text": message,
                "footer": "OmniKube CloudMetrics",
            }
        ],
    }
    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        logger.info("[Notifier] Slack alert delivered")
        return True
    except requests.RequestException as exc:
        logger.warning("[Notifier] Slack alert failed: %s", exc)
        return False


def send_slack_cost_alert(
    summary: str,
    detail: str,
    context: dict[str, Any],
    *,
    db_path: str = "",
    webhook_url: str | None = None,
) -> bool:
    """Send a Block Kit cost optimization alert with a Scale Down Now button."""
    settings = _load_settings(db_path)
    url = (webhook_url or settings.get(KEY_SLACK_WEBHOOK_URL, "")).strip()
    if not url:
        logger.debug("[Notifier] Slack cost alert skipped — no webhook URL configured")
        return False
    if db_path and not _parse_bool(settings.get(KEY_SLACK_ENABLED, "false")):
        logger.debug("[Notifier] Slack cost alert skipped — channel disabled")
        return False

    payload = build_cost_slack_payload(summary, detail, context)
    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        logger.info("[Notifier] Slack cost optimization alert delivered (ChatOps button attached)")
        return True
    except requests.RequestException as exc:
        logger.warning("[Notifier] Slack cost alert failed: %s", exc)
        return False


def send_discord_alert(message: str, *, db_path: str = "", webhook_url: str | None = None) -> bool:
    """Send a structured embed message to a Discord webhook."""
    settings = _load_settings(db_path)
    url = (webhook_url or settings.get(KEY_DISCORD_WEBHOOK_URL, "")).strip()
    if not url:
        logger.debug("[Notifier] Discord skipped — no webhook URL configured")
        return False
    if db_path and not _parse_bool(settings.get(KEY_DISCORD_ENABLED, "false")):
        logger.debug("[Notifier] Discord skipped — channel disabled")
        return False

    payload = {
        "embeds": [
            {
                "title": "OmniKube Alert",
                "description": message[:4096],
                "color": DISCORD_EMBED_COLOR,
                "footer": {"text": "OmniKube CloudMetrics"},
            }
        ]
    }
    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        logger.info("[Notifier] Discord alert delivered")
        return True
    except requests.RequestException as exc:
        logger.warning("[Notifier] Discord alert failed: %s", exc)
        return False


def send_discord_cost_alert(
    summary: str,
    detail: str,
    context: dict[str, Any],
    *,
    db_path: str = "",
    webhook_url: str | None = None,
) -> bool:
    """Send a Discord cost optimization alert with a Scale Down Now button row."""
    settings = _load_settings(db_path)
    url = (webhook_url or settings.get(KEY_DISCORD_WEBHOOK_URL, "")).strip()
    if not url:
        logger.debug("[Notifier] Discord cost alert skipped — no webhook URL configured")
        return False
    if db_path and not _parse_bool(settings.get(KEY_DISCORD_ENABLED, "false")):
        logger.debug("[Notifier] Discord cost alert skipped — channel disabled")
        return False

    payload = build_cost_discord_payload(summary, detail, context)
    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        logger.info("[Notifier] Discord cost optimization alert delivered (ChatOps button attached)")
        return True
    except requests.RequestException as exc:
        logger.warning("[Notifier] Discord cost alert failed: %s", exc)
        return False


def get_cost_alert_limit_usd(db_path: str) -> float:
    settings = _load_settings(db_path)
    try:
        return float(settings.get(KEY_COST_ALERT_LIMIT_USD, "5000") or 5000)
    except (TypeError, ValueError):
        return 5000.0


def dispatch_live_alert(
    db_path: str,
    summary: str,
    *,
    detail: str = "",
) -> dict[str, bool]:
    """Fan out an alert to every enabled notification channel immediately."""
    body = detail or summary
    results = {
        "slack": send_slack_alert(body, db_path=db_path),
        "discord": send_discord_alert(body, db_path=db_path),
    }
    if any(results.values()):
        logger.info("[Notifier] Live alert dispatched: %s", results)
    else:
        logger.warning("[Notifier] Live alert had no successful channels: %s", results)
    return results


def dispatch_cost_optimization_alert(
    db_path: str,
    summary: str,
    *,
    detail: str = "",
    context: dict[str, Any] | None = None,
) -> dict[str, bool]:
    """Fan out a cost optimization alert with interactive ChatOps remediation buttons."""
    remediation_context = context or {}
    body = detail or summary
    results = {
        "slack": send_slack_cost_alert(
            summary,
            body,
            remediation_context,
            db_path=db_path,
        ),
        "discord": send_discord_cost_alert(
            summary,
            body,
            remediation_context,
            db_path=db_path,
        ),
    }
    if any(results.values()):
        logger.info("[Notifier] Cost optimization alert dispatched: %s", results)
    else:
        logger.warning("[Notifier] Cost optimization alert had no successful channels: %s", results)
    return results


def dispatch_structured_alert(db_path: str, payload: dict[str, Any]) -> dict[str, bool]:
    """Dispatch using rich alert_manager payload shape."""
    summary = str(payload.get("summary", "OmniKube threshold alert"))
    lines = [summary]
    for workload in payload.get("workloads") or []:
        lines.append(
            f"- {workload.get('namespace', 'default')}/{workload.get('pod_name', 'unknown')}: "
            f"{str(workload.get('metric', '')).upper()} {workload.get('value')}% "
            f"(threshold {workload.get('threshold')}%)"
        )
    detail = "\n".join(lines)
    return dispatch_live_alert(db_path, summary, detail=detail)
