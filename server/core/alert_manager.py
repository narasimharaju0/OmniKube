import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

from core.system_config import (
    KEY_DISCORD_ENABLED,
    KEY_SLACK_ENABLED,
    _parse_bool,
    get_notification_config,
)

logger = logging.getLogger(__name__)

BUFFER_WINDOW_SEC = 60
FLUSH_CHECK_INTERVAL_SEC = 5
DISPATCH_MAX_RETRIES = 4
DISPATCH_INITIAL_BACKOFF_SEC = 2
DISPATCH_REQUEST_TIMEOUT_SEC = 10
HTTP_POST_SPACING_SEC = float(os.environ.get("OMNIKUBE_HTTP_POST_SPACING_SEC", "1.5"))
ALERT_COOLDOWN_INTERVAL_SEC = int(os.environ.get("OMNIKUBE_ALERT_COOLDOWN_SEC", "600"))
DEFAULT_TENANT_ID = "default"

_http_spacing_lock = threading.Lock()
_last_http_post_monotonic = 0.0

ENV_SLACK_WEBHOOK = "OMNIKUBE_SLACK_WEBHOOK_URL"
ENV_DISCORD_WEBHOOK = "OMNIKUBE_DISCORD_WEBHOOK_URL"
ENV_MOCK_WEBHOOK = "OMNIKUBE_ALERT_MOCK_WEBHOOK_URL"
DEFAULT_MOCK_WEBHOOK_PATH = "/api/alerts/mock-webhook"

DISCORD_WARNING_COLOR = 0xE11D48
SLACK_DANGER_COLOR = "#E11D48"

# ReplicaSet / Deployment pod suffix: -{rs-hash}-{pod-id}
_REPLICASET_POD_SUFFIX = re.compile(r"-[a-z0-9]{5,}-[a-z0-9]{5,}$", re.IGNORECASE)
# StatefulSet ordinal suffix: -{ordinal}
_STATEFULSET_ORDINAL_SUFFIX = re.compile(r"-\d+$")

_WORKLOAD_LABEL_KEYS = (
    "app.kubernetes.io/name",
    "app.kubernetes.io/instance",
    "app",
    "k8s-app",
)


@dataclass
class AlertEvent:
    cluster_id: str
    node_id: str
    pod_name: str
    namespace: str
    metric: str
    value: float
    threshold: float
    workload_name: str = ""
    tenant_id: str = DEFAULT_TENANT_ID
    captured_at: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        if not self.workload_name:
            self.workload_name = derive_workload_name(self.pod_name)


def derive_workload_name(
    pod_name: str,
    kubernetes_labels: dict[str, Any] | None = None,
) -> str:
    """Collapse per-pod names into a stable workload identity for cooldown grouping."""
    labels = kubernetes_labels or {}
    for key in _WORKLOAD_LABEL_KEYS:
        label_value = labels.get(key)
        if label_value:
            return str(label_value).strip()

    workload = str(pod_name).strip()
    if not workload:
        return "unknown"

    if _REPLICASET_POD_SUFFIX.search(workload):
        return _REPLICASET_POD_SUFFIX.sub("", workload) or workload

    if _STATEFULSET_ORDINAL_SUFFIX.search(workload):
        base_name = _STATEFULSET_ORDINAL_SUFFIX.sub("", workload)
        if base_name and base_name != workload:
            return base_name

    return workload


def build_cooldown_key(
    tenant_id: str,
    cluster_id: str,
    workload_name: str,
    metric_name: str,
) -> str:
    return f"{tenant_id}:{cluster_id}:{workload_name}:{metric_name}"


def extract_cooldown_keys(message: dict[str, Any]) -> list[str]:
    tenant_id = str(message.get("tenant_id", DEFAULT_TENANT_ID))
    cluster_id = str(message.get("cluster_id", "unknown"))
    workloads = message.get("workloads") or []

    if not workloads:
        return [build_cooldown_key(tenant_id, cluster_id, "unknown", "unknown")]

    keys: set[str] = set()
    for workload in workloads:
        pod_name = str(workload.get("pod_name", "unknown"))
        workload_name = str(
            workload.get("workload_name")
            or derive_workload_name(
                pod_name,
                workload.get("kubernetes_labels"),
            )
        )
        metric_name = str(workload.get("metric", "unknown"))
        keys.add(build_cooldown_key(tenant_id, cluster_id, workload_name, metric_name))

    return sorted(keys)


def _workload_lines(payload: dict[str, Any]) -> list[str]:
    workloads = payload.get("workloads") or []
    if not workloads:
        return ["No workload details available."]
    return [
        (
            f"{item.get('namespace', 'default')}/{item.get('workload_name') or item.get('pod_name', 'unknown')}: "
            f"{str(item.get('metric', '')).upper()} "
            f"{item.get('value')}% (threshold {item.get('threshold')}%)"
        )
        for item in workloads
    ]


def build_slack_payload(message: dict[str, Any]) -> dict[str, Any]:
    summary = str(message.get("summary", "OmniKube threshold alert"))
    workloads = _workload_lines(message)
    cluster_id = str(message.get("cluster_id", "unknown"))

    return {
        "text": f":warning: {summary}",
        "attachments": [
            {
                "color": SLACK_DANGER_COLOR,
                "title": "OmniKube Threshold Breach",
                "text": summary,
                "fields": [
                    {"title": "Cluster", "value": cluster_id, "short": True},
                    {
                        "title": "Workloads",
                        "value": str(message.get("workload_count", len(workloads))),
                        "short": True,
                    },
                    {
                        "title": "Max CPU",
                        "value": f"{message.get('max_cpu', 0)}%",
                        "short": True,
                    },
                    {
                        "title": "Max Memory",
                        "value": f"{message.get('max_memory', 0)}%",
                        "short": True,
                    },
                    {
                        "title": "Affected Workloads",
                        "value": "\n".join(workloads[:8]),
                        "short": False,
                    },
                ],
                "footer": "OmniKube CloudMetrics",
            }
        ],
    }


def build_discord_payload(message: dict[str, Any]) -> dict[str, Any]:
    summary = str(message.get("summary", "OmniKube threshold alert"))
    workloads = _workload_lines(message)
    cluster_id = str(message.get("cluster_id", "unknown"))

    return {
        "content": f"**OmniKube Alert** — {summary}",
        "embeds": [
            {
                "title": "Threshold Breach Detected",
                "description": summary,
                "color": DISCORD_WARNING_COLOR,
                "fields": [
                    {"name": "Cluster", "value": cluster_id, "inline": True},
                    {
                        "name": "Workloads",
                        "value": str(message.get("workload_count", len(workloads))),
                        "inline": True,
                    },
                    {
                        "name": "Max CPU",
                        "value": f"{message.get('max_cpu', 0)}%",
                        "inline": True,
                    },
                    {
                        "name": "Max Memory",
                        "value": f"{message.get('max_memory', 0)}%",
                        "inline": True,
                    },
                    {
                        "name": "Affected Workloads",
                        "value": "\n".join(workloads[:8])[:1024],
                        "inline": False,
                    },
                ],
                "footer": {"text": "OmniKube CloudMetrics"},
            }
        ],
    }


def _wait_for_http_post_spacing() -> None:
    """Serialize third-party HTTP POSTs with a small gap to avoid webhook 429 bursts."""
    global _last_http_post_monotonic

    with _http_spacing_lock:
        now = time.monotonic()
        elapsed = now - _last_http_post_monotonic
        if elapsed < HTTP_POST_SPACING_SEC:
            time.sleep(HTTP_POST_SPACING_SEC - elapsed)
        _last_http_post_monotonic = time.monotonic()


def _post_json_with_retry(channel_name: str, url: str, payload: dict[str, Any]) -> bool:
    body = json.dumps(payload).encode("utf-8")
    backoff_sec = DISPATCH_INITIAL_BACKOFF_SEC

    for attempt in range(1, DISPATCH_MAX_RETRIES + 1):
        _wait_for_http_post_spacing()
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=DISPATCH_REQUEST_TIMEOUT_SEC) as response:
                response.read()
            logger.info(
                "[Alert Dispatcher] %s delivery succeeded (attempt=%d)",
                channel_name,
                attempt,
            )
            return True
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < DISPATCH_MAX_RETRIES:
                retry_after = exc.headers.get("Retry-After")
                wait_sec = (
                    int(retry_after)
                    if retry_after and str(retry_after).isdigit()
                    else backoff_sec
                )
                logger.warning(
                    "[Alert Dispatcher] HTTP 429 from %s (non-fatal) — backing off %ss "
                    "(attempt %d/%d, alert bundle retained)",
                    channel_name,
                    wait_sec,
                    attempt,
                    DISPATCH_MAX_RETRIES,
                )
                time.sleep(wait_sec)
                backoff_sec = min(backoff_sec * 2, 60)
                continue
            if exc.code == 429:
                logger.warning(
                    "[Alert Dispatcher] HTTP 429 from %s after %d attempt(s) — "
                    "alert bundle exhausted retries (non-fatal, collector unaffected): %s",
                    channel_name,
                    attempt,
                    exc.reason,
                )
            else:
                logger.error(
                    "[Alert Dispatcher] HTTP %s from %s after %d attempt(s): %s",
                    exc.code,
                    channel_name,
                    attempt,
                    exc.reason,
                )
            return False
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < DISPATCH_MAX_RETRIES:
                logger.warning(
                    "[Alert Dispatcher] %s delivery failed (%s) — retry in %ss",
                    channel_name,
                    exc,
                    backoff_sec,
                )
                time.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2, 60)
                continue
            logger.warning(
                "[Alert Dispatcher] %s delivery failed after %d attempt(s) "
                "(non-fatal, collector unaffected): %s",
                channel_name,
                attempt,
                exc,
            )
            return False

    return False


def dispatch_slack_alert(message: dict[str, Any], webhook_url: str) -> bool:
    if not webhook_url:
        return False
    payload = build_slack_payload(message)
    return _post_json_with_retry("slack", webhook_url, payload)


def dispatch_discord_alert(message: dict[str, Any], webhook_url: str) -> bool:
    if not webhook_url:
        return False
    payload = build_discord_payload(message)
    return _post_json_with_retry("discord", webhook_url, payload)


def resolve_notification_channels(
    db_path: str,
    config_getter: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = get_notification_config(db_path)
    legacy = config_getter() if config_getter else {}

    slack_url = str(settings.get("slack_webhook_url", "")).strip()
    discord_url = str(settings.get("discord_webhook_url", "")).strip()
    slack_enabled = _parse_bool(settings.get(KEY_SLACK_ENABLED, "false"))
    discord_enabled = _parse_bool(settings.get(KEY_DISCORD_ENABLED, "false"))

    if not slack_url:
        slack_url = str(legacy.get("slack_webhook_url", "")).strip()
    if not discord_url:
        discord_url = str(legacy.get("discord_webhook_url", "")).strip()

    if not slack_url:
        slack_url = os.environ.get(ENV_SLACK_WEBHOOK, "").strip()
    if not discord_url:
        discord_url = os.environ.get(ENV_DISCORD_WEBHOOK, "").strip()

    mock_url = os.environ.get(ENV_MOCK_WEBHOOK, "").strip()
    if not mock_url and not slack_url and not discord_url:
        mock_url = (
            f"http://127.0.0.1:{os.environ.get('OMNIKUBE_SERVER_PORT', '5000')}"
            f"{DEFAULT_MOCK_WEBHOOK_PATH}"
        )

    return {
        "slack": slack_url if slack_enabled else "",
        "discord": discord_url if discord_enabled else "",
        "slack_enabled": slack_enabled,
        "discord_enabled": discord_enabled,
        "mock": mock_url,
    }


def resolve_webhook_channels(config_getter: Callable[[], dict[str, Any]]) -> dict[str, str]:
    """Backward-compatible wrapper for legacy callers without db_path."""
    channels = resolve_notification_channels("", config_getter)
    return {
        "slack": str(channels.get("slack", "")),
        "discord": str(channels.get("discord", "")),
        "mock": str(channels.get("mock", "")),
    }


class MultiChannelAlertDispatcher:
    """Fires grouped alert bundles to Slack and Discord concurrently."""

    def __init__(
        self,
        db_path: str,
        config_getter: Callable[[], dict[str, Any]] | None = None,
        cooldown_interval_sec: int = ALERT_COOLDOWN_INTERVAL_SEC,
    ) -> None:
        self._db_path = db_path
        self._config_getter = config_getter
        self.cooldown_cache: dict[str, float] = {}
        self.cooldown_interval_sec = cooldown_interval_sec
        self._cooldown_lock = threading.Lock()

    def _should_suppress_throttled_alert(self, message: dict[str, Any]) -> bool:
        keys = extract_cooldown_keys(message)
        now = time.time()

        with self._cooldown_lock:
            for key in keys:
                last_sent = self.cooldown_cache.get(key)
                if last_sent is not None and (now - last_sent) < self.cooldown_interval_sec:
                    logger.info("[Alert Manager] Suppressing throttled alert for key=%s", key)
                    return True

            for key in keys:
                self.cooldown_cache[key] = now

        return False

    def dispatch_grouped_alert_async(self, summary: str, payload: dict[str, Any]) -> None:
        """Queue grouped alert dispatch on a background thread."""
        message = {**payload, "summary": summary}
        worker = threading.Thread(
            target=self._dispatch_grouped_alert,
            args=(message,),
            name="omnikube-alert-dispatch",
            daemon=True,
        )
        worker.start()
        logger.info(
            "[Alert Dispatcher] Async dispatch queued for cluster=%s workloads=%d",
            payload.get("cluster_id"),
            payload.get("workload_count"),
        )

    def dispatch_grouped_alert_sync(self, summary: str, payload: dict[str, Any]) -> None:
        """Run dispatch in the caller thread (used by staggered flush worker)."""
        message = {**payload, "summary": summary}
        self._dispatch_grouped_alert(message)

    def dispatch_threshold_breach_async(self, summary: str, payload: dict[str, Any]) -> None:
        """Non-blocking notification fan-out from the scrape evaluation loop."""
        self.dispatch_grouped_alert_async(summary, payload)

    def _dispatch_http_channels_sequential(
        self,
        message: dict[str, Any],
        channels: dict[str, Any],
    ) -> None:
        if channels["slack"]:
            dispatch_slack_alert(message, channels["slack"])

        if channels["discord"]:
            dispatch_discord_alert(message, channels["discord"])

        if not channels["slack"] and not channels["discord"] and channels["mock"]:
            _post_json_with_retry("mock", channels["mock"], dict(message))

    def _dispatch_grouped_alert(self, message: dict[str, Any]) -> None:
        if self._should_suppress_throttled_alert(message):
            return

        channels = resolve_notification_channels(self._db_path, self._config_getter)
        has_http_channel = bool(channels["slack"] or channels["discord"] or channels["mock"])

        if not has_http_channel:
            logger.warning(
                "[Alert Dispatcher] No notification channels configured — alert not sent (cluster=%s)",
                message.get("cluster_id"),
            )
            return

        logger.info(
            "[Alert Dispatcher] Firing HTTP alert channels sequentially "
            "(spacing=%.1fs, cluster=%s)",
            HTTP_POST_SPACING_SEC,
            message.get("cluster_id"),
        )
        self._dispatch_http_channels_sequential(message, channels)

    def _post_with_retry(self, channel_name: str, url: str, payload: dict[str, Any]) -> bool:
        return _post_json_with_retry(channel_name, url, payload)


class AlertBuffer:
    def __init__(
        self,
        db_path: str,
        config_getter: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._db_path = db_path
        self._config_getter = config_getter
        self._dispatcher = MultiChannelAlertDispatcher(db_path, config_getter)
        self.cooldown_cache = self._dispatcher.cooldown_cache
        self._lock = threading.Lock()
        self._events: list[AlertEvent] = []
        self._window_started_at = time.monotonic()
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._flush_loop,
            name="omnikube-alert-buffer",
            daemon=True,
        )
        self._thread.start()
        channels = resolve_notification_channels(self._db_path, self._config_getter)
        logger.info(
            "Alert buffer started (window=%ss, channels=slack:%s discord:%s mock:%s)",
            BUFFER_WINDOW_SEC,
            bool(channels["slack"]),
            bool(channels["discord"]),
            bool(channels["mock"]),
        )

    def stop(self) -> None:
        self._running = False

    def capture(self, event: AlertEvent) -> None:
        with self._lock:
            self._events.append(event)
            pending = len(self._events)

        logger.info(
            "Alert caught: metric=%s value=%.1f threshold=%.1f pod=%s/%s cluster=%s node=%s (buffered=%d)",
            event.metric,
            event.value,
            event.threshold,
            event.namespace,
            event.pod_name,
            event.cluster_id,
            event.node_id,
            pending,
        )
        self.fire_immediate(event)

    def fire_immediate(self, event: AlertEvent) -> None:
        """Dispatch a threshold breach to live notification channels immediately."""
        payload = self._build_summary_payload([event])
        summary = str(payload.get("summary", "OmniKube threshold alert"))
        worker = threading.Thread(
            target=self._dispatch_immediate_alert,
            args=(summary, payload),
            name="omnikube-alert-immediate",
            daemon=True,
        )
        worker.start()

    def _dispatch_immediate_alert(self, summary: str, payload: dict[str, Any]) -> None:
        try:
            from core.notifier import dispatch_structured_alert

            if self._dispatcher._should_suppress_throttled_alert({**payload, "summary": summary}):
                return
            dispatch_structured_alert(self._db_path, {**payload, "summary": summary})
        except Exception as exc:
            logger.error("[Alert Manager] Immediate dispatch failed (non-fatal): %s", exc)

    def _dispatch_flush_groups_staggered(self, groups: list[list[AlertEvent]]) -> None:
        """Dispatch deduplicated tenant/workload groups with spacing between HTTP bursts."""
        for index, group in enumerate(groups):
            if index > 0:
                logger.info(
                    "[Alert Manager] Staggering flush dispatch — sleeping %.1fs before next group",
                    HTTP_POST_SPACING_SEC,
                )
                time.sleep(HTTP_POST_SPACING_SEC)

            payload = self._build_summary_payload(group)
            tenant_id = payload.get("tenant_id", DEFAULT_TENANT_ID)
            cluster_id = payload.get("cluster_id", "unknown")
            workload_names = sorted(
                {
                    event.workload_name
                    for event in group
                    if event.workload_name
                }
            )
            summary = str(payload.get("summary", "OmniKube grouped alert"))
            logger.info(
                "[Alert Manager] Flush dispatching group %d/%d "
                "(tenant=%s cluster=%s workloads=%s)",
                index + 1,
                len(groups),
                tenant_id,
                cluster_id,
                workload_names,
            )
            self._dispatcher.dispatch_grouped_alert_sync(summary, payload)

        logger.info(
            "[Alert Manager] Staggered flush dispatch complete (%d group(s))",
            len(groups),
        )

    def get_queue_status(self) -> dict[str, Any]:
        with self._lock:
            elapsed = time.monotonic() - self._window_started_at
            remaining = max(0.0, BUFFER_WINDOW_SEC - elapsed)
            events = [
                {
                    "pod_name": event.pod_name,
                    "namespace": event.namespace,
                    "metric": event.metric,
                    "value": round(event.value, 1),
                    "threshold": round(event.threshold, 1),
                    "cluster_id": event.cluster_id,
                    "node_id": event.node_id,
                }
                for event in self._events
            ]
            return {
                "pending_count": len(events),
                "window_seconds": BUFFER_WINDOW_SEC,
                "window_remaining_seconds": round(remaining, 1),
                "events": events,
            }

    def _flush_loop(self) -> None:
        while self._running:
            time.sleep(FLUSH_CHECK_INTERVAL_SEC)
            try:
                self._flush_if_window_elapsed()
            except Exception as exc:
                logger.error("Alert buffer flush loop error: %s", exc)

    def _flush_if_window_elapsed(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._window_started_at
            if elapsed < BUFFER_WINDOW_SEC or not self._events:
                return

            events = list(self._events)
            self._events.clear()
            self._window_started_at = time.monotonic()

        logger.info(
            "Alert buffer window elapsed (%.0fs): flushing %d buffered event(s)",
            elapsed,
            len(events),
        )
        self._flush_grouped_events(events)

    def _group_events(self, events: list[AlertEvent]) -> list[list[AlertEvent]]:
        if not events:
            return []

        parent = list(range(len(events)))

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(left: int, right: int) -> None:
            root_left = find(left)
            root_right = find(right)
            if root_left != root_right:
                parent[root_right] = root_left

        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                same_cluster = (
                    events[i].cluster_id
                    and events[i].cluster_id == events[j].cluster_id
                )
                same_node = events[i].node_id and events[i].node_id == events[j].node_id
                if same_cluster or same_node:
                    union(i, j)

        grouped: dict[int, list[AlertEvent]] = defaultdict(list)
        for index, event in enumerate(events):
            grouped[find(index)].append(event)

        return list(grouped.values())

    def _build_summary_payload(self, group: list[AlertEvent]) -> dict[str, Any]:
        cluster_ids = sorted({event.cluster_id for event in group if event.cluster_id})
        node_ids = sorted({event.node_id for event in group if event.node_id})
        workloads = [
            {
                "pod_name": event.pod_name,
                "workload_name": event.workload_name,
                "namespace": event.namespace,
                "metric": event.metric,
                "value": round(event.value, 1),
                "threshold": round(event.threshold, 1),
                "node_id": event.node_id,
            }
            for event in group
        ]
        cluster_id = cluster_ids[0] if cluster_ids else "unknown"
        tenant_id = str(group[0].tenant_id) if group else DEFAULT_TENANT_ID

        summary = (
            f"OmniKube grouped alert: {len(group)} workload breach(es) "
            f"in cluster={cluster_id}"
        )

        return {
            "status": "ALERT",
            "summary": summary,
            "tenant_id": tenant_id,
            "cluster_id": cluster_id,
            "node_ids": node_ids,
            "workload_count": len(group),
            "workloads": workloads,
            "max_cpu": max((event.value for event in group if event.metric == "cpu"), default=0),
            "max_memory": max(
                (event.value for event in group if event.metric == "memory"), default=0
            ),
        }

    def _flush_grouped_events(self, events: list[AlertEvent]) -> None:
        groups = self._group_events(events)
        logger.info(
            "Alert deduplication grouped %d buffered event(s) into %d dispatch group(s)",
            len(events),
            len(groups),
        )

        if not groups:
            logger.info("Alert buffer flush complete — no groups to dispatch")
            return

        flush_worker = threading.Thread(
            target=self._dispatch_flush_groups_staggered,
            args=(groups,),
            name="omnikube-alert-flush-dispatch",
            daemon=True,
        )
        flush_worker.start()
        logger.info(
            "Alert buffer flush complete — staggered dispatch worker started "
            "(%d group(s), %.1fs HTTP spacing)",
            len(groups),
            HTTP_POST_SPACING_SEC,
        )
