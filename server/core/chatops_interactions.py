"""Incoming ChatOps interaction receiver for Slack and Discord button clicks."""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import parse_qs

from core.auth import DEFAULT_TENANT_ID
from core.notifier import DOWNSCALE_ACTION_ID
from core.remediation import apply_optimization_remediation

logger = logging.getLogger(__name__)

MANAGED_SUCCESS_MESSAGE = "[Managed] Infrastructure scaled down successfully!"

DISCORD_INTERACTION_PING = 1
DISCORD_INTERACTION_MESSAGE_COMPONENT = 3
DISCORD_RESPONSE_CHANNEL_MESSAGE = 4
DISCORD_RESPONSE_UPDATE_MESSAGE = 7

_JSON_FENCE_PATTERN = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _parse_json_blob(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _metadata_from_embed_fields(embed: dict[str, Any]) -> dict[str, Any]:
    for field in embed.get("fields") or []:
        if str(field.get("name")) == "Remediation Metadata":
            cleaned = _JSON_FENCE_PATTERN.sub("", str(field.get("value") or "")).strip()
            metadata = _parse_json_blob(cleaned)
            if metadata:
                return metadata

    metadata: dict[str, Any] = {}
    field_map = {
        "Namespace": "namespace",
        "Deployment": "deployment",
        "Target Size": "target",
        "Cluster": "cluster_id",
    }
    for field in embed.get("fields") or []:
        key = field_map.get(str(field.get("name")))
        if key:
            value = str(field.get("value") or "").strip()
            if value and value != "—":
                metadata[key] = value
    return metadata


def extract_discord_metadata(interaction: dict[str, Any]) -> dict[str, Any]:
    message = interaction.get("message") or {}
    for embed in message.get("embeds") or []:
        metadata = _metadata_from_embed_fields(embed)
        if metadata:
            return metadata
    return {}


def parse_discord_interaction(body: dict[str, Any]) -> dict[str, Any] | None:
    interaction_type = body.get("type")
    if interaction_type == DISCORD_INTERACTION_PING:
        return {"platform": "discord", "kind": "ping"}

    if interaction_type != DISCORD_INTERACTION_MESSAGE_COMPONENT:
        return None

    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    if str(data.get("custom_id")) != DOWNSCALE_ACTION_ID:
        return None

    return {
        "platform": "discord",
        "kind": "downscale_workload",
        "metadata": extract_discord_metadata(body),
        "interaction": body,
    }


def parse_slack_interaction(raw_body: bytes) -> dict[str, Any] | None:
    try:
        form = parse_qs(raw_body.decode("utf-8"), keep_blank_values=True)
    except UnicodeDecodeError:
        return None

    payload_raw = (form.get("payload") or [None])[0]
    if not payload_raw:
        return None

    payload = _parse_json_blob(payload_raw)
    if not payload:
        return None

    for action in payload.get("actions") or []:
        if str(action.get("action_id")) != DOWNSCALE_ACTION_ID:
            continue
        metadata = _parse_json_blob(str(action.get("value") or ""))
        return {
            "platform": "slack",
            "kind": "downscale_workload",
            "metadata": metadata,
            "interaction": payload,
        }

    return None


def metadata_to_apply_payload(metadata: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    tenant_id = str(metadata.get("tenant_id") or DEFAULT_TENANT_ID)
    namespace = str(metadata.get("namespace") or "").strip()
    deployment = str(metadata.get("deployment") or "").strip()
    target = str(metadata.get("target") or "medium").strip()
    cluster_id = str(metadata.get("cluster_id") or "").strip()

    apply_payload: dict[str, Any] = {"target": target}
    if cluster_id:
        apply_payload["cluster_id"] = cluster_id
    if namespace:
        apply_payload["namespace"] = namespace
    if deployment:
        apply_payload["deployment"] = deployment

    if namespace and deployment:
        apply_payload["action"] = "reduce_replicas"
    else:
        apply_payload["action"] = "downscale_nodes"

    return tenant_id, apply_payload


def build_slack_interaction_response() -> dict[str, Any]:
    return {
        "response_type": "in_channel",
        "replace_original": True,
        "text": MANAGED_SUCCESS_MESSAGE,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":white_check_mark: *{MANAGED_SUCCESS_MESSAGE}*",
                },
            }
        ],
    }


def build_discord_interaction_response(interaction: dict[str, Any]) -> dict[str, Any]:
    if interaction.get("message"):
        return {
            "type": DISCORD_RESPONSE_UPDATE_MESSAGE,
            "data": {
                "embeds": [
                    {
                        "title": "Cost Optimization Alert",
                        "description": MANAGED_SUCCESS_MESSAGE,
                        "color": 0x10B981,
                        "footer": {"text": "OmniKube CloudMetrics · ChatOps"},
                    }
                ],
                "components": [],
            },
        }

    return {
        "type": DISCORD_RESPONSE_CHANNEL_MESSAGE,
        "data": {
            "content": MANAGED_SUCCESS_MESSAGE,
            "flags": 0,
        },
    }


def process_chatops_interaction(
    db_path: str,
    *,
    raw_body: bytes,
    content_type: str = "",
) -> dict[str, Any]:
    """
    Parse a Slack or Discord interaction payload, trigger remediation, and return
    the platform-specific acknowledgment body plus HTTP status code.
    """
    parsed: dict[str, Any] | None = None
    content_type_lower = (content_type or "").lower()

    if raw_body:
        if "application/json" in content_type_lower:
            body = _parse_json_blob(raw_body.decode("utf-8"))
            if body:
                parsed = parse_discord_interaction(body)
                if parsed and parsed.get("kind") == "ping":
                    return {"status": 200, "body": {"type": DISCORD_INTERACTION_PING}}

        if parsed is None:
            parsed = parse_slack_interaction(raw_body)

        if parsed is None:
            body = _parse_json_blob(raw_body.decode("utf-8"))
            if body:
                parsed = parse_discord_interaction(body)
                if parsed and parsed.get("kind") == "ping":
                    return {"status": 200, "body": {"type": DISCORD_INTERACTION_PING}}

    if parsed is None or parsed.get("kind") != "downscale_workload":
        logger.info("[ChatOps] Interaction ignored — no matching downscale_workload action")
        return {
            "status": 400,
            "body": {"error": "Unsupported interaction payload."},
        }

    metadata = parsed.get("metadata") if isinstance(parsed.get("metadata"), dict) else {}
    tenant_id, apply_payload = metadata_to_apply_payload(metadata)

    try:
        apply_optimization_remediation(
            db_path,
            tenant_id=tenant_id,
            action=str(apply_payload["action"]),
            payload=apply_payload,
        )
        logger.info(
            "[ChatOps] %s downscale_workload accepted tenant=%s action=%s",
            parsed.get("platform"),
            tenant_id,
            apply_payload.get("action"),
        )
    except ValueError as exc:
        logger.warning("[ChatOps] Remediation rejected: %s", exc)
        return {"status": 400, "body": {"error": str(exc)}}
    except Exception as exc:
        logger.error("[ChatOps] Remediation failed: %s", exc)
        return {"status": 500, "body": {"error": "Cluster remediation failed."}}

    platform = str(parsed.get("platform") or "")
    if platform == "slack":
        ack_body: dict[str, Any] = build_slack_interaction_response()
    else:
        interaction = parsed.get("interaction") if isinstance(parsed.get("interaction"), dict) else {}
        ack_body = build_discord_interaction_response(interaction)

    return {"status": 200, "body": ack_body}
