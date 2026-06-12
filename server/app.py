import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

HOST = "0.0.0.0"
PORT = 5000
DB_PATH = "/app/data/omnikube.db"
CONFIG_PATH = "/app/data/omnikube-config.json"
API_TOKEN = "premium_secret_2026"
METRICS_LIMIT = 30
ALERT_COOLDOWN_SEC = 60

TIMEFRAME_SQL_OFFSETS: dict[str, str] = {
    "1h": "-1 hour",
    "6h": "-6 hours",
    "12h": "-12 hours",
    "24h": "-24 hours",
}

DEFAULT_CONFIG: dict[str, Any] = {
    "slack_webhook_url": "",
    "discord_webhook_url": "",
    "cpu_alert_threshold": 80,
    "memory_alert_threshold": 90,
}

MOCK_METRICS = [
    {
        "id": i,
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=(30 - i) * 5)).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
        "cpu": round(18 + (i * 3.7) % 42, 1),
        "memory": round(52 + (i * 2.3) % 28, 1),
    }
    for i in range(1, 31)
]

_config_lock = threading.Lock()
_alert_lock = threading.Lock()
_last_alert_at = 0.0


class ConfigStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._data = dict(DEFAULT_CONFIG)

    def load(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            self.save()
            return

        try:
            with open(self.path, encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                self._data = {**DEFAULT_CONFIG, **payload}
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[OmniKube Server] Config load failed, using defaults: {exc}")

    def save(self) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        try:
            with open(self.path, "w", encoding="utf-8") as handle:
                json.dump(self._data, handle, indent=2)
            return True
        except OSError as exc:
            print(f"[OmniKube Server] Config save failed: {exc}")
            return False

    def get(self) -> dict[str, Any]:
        with _config_lock:
            return dict(self._data)

    def update(self, updates: dict[str, Any]) -> dict[str, Any]:
        with _config_lock:
            if "slack_webhook_url" in updates:
                self._data["slack_webhook_url"] = str(updates["slack_webhook_url"]).strip()
            if "discord_webhook_url" in updates:
                self._data["discord_webhook_url"] = str(updates["discord_webhook_url"]).strip()
            if "cpu_alert_threshold" in updates:
                self._data["cpu_alert_threshold"] = float(updates["cpu_alert_threshold"])
            if "memory_alert_threshold" in updates:
                self._data["memory_alert_threshold"] = float(updates["memory_alert_threshold"])
            self.save()
            return dict(self._data)


config_store = ConfigStore(CONFIG_PATH)


def init_database() -> None:
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cluster_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    cpu REAL NOT NULL,
                    memory REAL NOT NULL
                )
                """
            )

            count = conn.execute("SELECT COUNT(*) FROM cluster_metrics").fetchone()[0]
            if count == 0:
                conn.executemany(
                    """
                    INSERT INTO cluster_metrics (timestamp, cpu, memory)
                    VALUES (?, ?, ?)
                    """,
                    [(row["timestamp"], row["cpu"], row["memory"]) for row in MOCK_METRICS],
                )
            conn.commit()
    except sqlite3.Error as exc:
        print(f"[OmniKube Server] Database initialization failed: {exc}")


def _filter_mock_metrics_by_timeframe(timeframe: str | None, limit: int) -> list[dict[str, Any]]:
    rows = list(MOCK_METRICS)
    sql_offset = TIMEFRAME_SQL_OFFSETS.get(timeframe or "")
    if not sql_offset:
        return rows[:limit]

    cutoff = datetime.now(timezone.utc) + _parse_sqlite_offset(sql_offset)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        try:
            ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts >= cutoff:
            filtered.append(row)

    filtered.sort(key=lambda item: int(item["id"]), reverse=True)
    return filtered[:limit] if limit else filtered


def _parse_sqlite_offset(offset: str) -> timedelta:
    parts = offset.strip().split()
    if len(parts) != 2:
        return timedelta()

    amount = int(parts[0].lstrip("-"))
    unit = parts[1].lower().rstrip("s")
    if unit == "hour":
        return timedelta(hours=-amount)
    if unit == "minute":
        return timedelta(minutes=-amount)
    if unit == "day":
        return timedelta(days=-amount)
    return timedelta()


def fetch_metrics(limit: int = METRICS_LIMIT, timeframe: str | None = None) -> list[dict[str, Any]]:
    sql_offset = TIMEFRAME_SQL_OFFSETS.get(timeframe or "")

    try:
        if not os.path.exists(DB_PATH):
            return _filter_mock_metrics_by_timeframe(timeframe, limit)

        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            if sql_offset:
                rows = conn.execute(
                    """
                    SELECT id, timestamp, cpu, memory
                    FROM cluster_metrics
                    WHERE datetime(substr(timestamp, 1, 19)) >= datetime('now', ?)
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (sql_offset, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, timestamp, cpu, memory
                    FROM cluster_metrics
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()

        if not rows:
            return _filter_mock_metrics_by_timeframe(timeframe, limit)

        return [dict(row) for row in rows]
    except (sqlite3.Error, OSError) as exc:
        print(f"[OmniKube Server] Metrics query failed, using fallback data: {exc}")
        return _filter_mock_metrics_by_timeframe(timeframe, limit)


def fetch_analytics() -> dict[str, Any]:
    try:
        if not os.path.exists(DB_PATH):
            cpus = [float(row["cpu"]) for row in MOCK_METRICS]
            memories = [float(row["memory"]) for row in MOCK_METRICS]
            return {
                "max_cpu": round(max(cpus), 1),
                "avg_cpu": round(sum(cpus) / len(cpus), 1),
                "max_memory": round(max(memories), 1),
                "avg_memory": round(sum(memories) / len(memories), 1),
                "sample_count": len(MOCK_METRICS),
            }

        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                """
                SELECT
                    MAX(cpu),
                    AVG(cpu),
                    MAX(memory),
                    AVG(memory),
                    COUNT(*)
                FROM cluster_metrics
                """
            ).fetchone()

        if not row or row[4] == 0:
            cpus = [float(m["cpu"]) for m in MOCK_METRICS]
            memories = [float(m["memory"]) for m in MOCK_METRICS]
            return {
                "max_cpu": round(max(cpus), 1),
                "avg_cpu": round(sum(cpus) / len(cpus), 1),
                "max_memory": round(max(memories), 1),
                "avg_memory": round(sum(memories) / len(memories), 1),
                "sample_count": len(MOCK_METRICS),
            }

        return {
            "max_cpu": round(float(row[0]), 1),
            "avg_cpu": round(float(row[1]), 1),
            "max_memory": round(float(row[2]), 1),
            "avg_memory": round(float(row[3]), 1),
            "sample_count": int(row[4]),
        }
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        print(f"[OmniKube Server] Analytics query failed, using fallback data: {exc}")
        cpus = [float(m["cpu"]) for m in MOCK_METRICS]
        memories = [float(m["memory"]) for m in MOCK_METRICS]
        return {
            "max_cpu": round(max(cpus), 1),
            "avg_cpu": round(sum(cpus) / len(cpus), 1),
            "max_memory": round(max(memories), 1),
            "avg_memory": round(sum(memories) / len(memories), 1),
            "sample_count": len(MOCK_METRICS),
        }


def fetch_latest_metric_row() -> dict[str, Any] | None:
    try:
        if not os.path.exists(DB_PATH):
            return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None

        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT id, timestamp, cpu, memory
                FROM cluster_metrics
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        if row is None:
            return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None

        return dict(row)
    except (sqlite3.Error, OSError) as exc:
        print(f"[OmniKube Server] Latest metric query failed, using fallback row: {exc}")
        return dict(MOCK_METRICS[-1]) if MOCK_METRICS else None


def _post_json(url: str, payload: dict[str, Any]) -> bool:
    if not url:
        return False

    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
        print(f"[OmniKube Server] Webhook alert delivered to {url[:48]}...")
        return True
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"[OmniKube Server] Webhook delivery failed: {exc}")
        return False


MOCK_WEBHOOK_PAYLOAD: dict[str, Any] = {
    "status": "TEST",
    "message": "OmniKube webhook connectivity test",
    "cpu": 42.5,
}


def dispatch_test_webhooks() -> dict[str, Any]:
    settings = config_store.get()
    slack_url = settings.get("slack_webhook_url", "")
    discord_url = settings.get("discord_webhook_url", "")
    results: dict[str, str] = {}

    if not slack_url and not discord_url:
        return {"error": "No webhook URLs configured.", "results": results}

    if slack_url:
        results["slack"] = "sent" if _post_json(slack_url, MOCK_WEBHOOK_PAYLOAD) else "failed"
    if discord_url:
        results["discord"] = (
            "sent"
            if _post_json(discord_url, {**MOCK_WEBHOOK_PAYLOAD, "content": MOCK_WEBHOOK_PAYLOAD["message"]})
            else "failed"
        )

    print("[OmniKube Server] Test webhook dispatch completed")
    return {"status": "ok", "payload": MOCK_WEBHOOK_PAYLOAD, "results": results}


def _dispatch_high_cpu_alert(current_cpu: float, memory: float, threshold: float) -> None:
    global _last_alert_at

    with _alert_lock:
        now = time.monotonic()
        if now - _last_alert_at < ALERT_COOLDOWN_SEC:
            return
        _last_alert_at = now

    settings = config_store.get()
    slack_url = settings.get("slack_webhook_url", "")
    discord_url = settings.get("discord_webhook_url", "")

    if slack_url:
        payload = {"status": "ALERT", "cpu": current_cpu}
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            slack_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
            print("[Alert Engine] Webhook triggered successfully")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"[Alert Engine] Slack webhook delivery failed: {exc}")

    if discord_url:
        message = (
            f"OmniKube Alert: High CPU load detected at {current_cpu:.1f}% "
            f"(threshold: {threshold:.0f}%). Memory: {memory:.1f}%."
        )
        _post_json(discord_url, {"content": message})

    if not slack_url and not discord_url:
        print(
            "[OmniKube Server] High CPU alert suppressed (no webhooks configured): "
            f"CPU {current_cpu:.1f}% exceeded threshold {threshold:.0f}%."
        )


def maybe_trigger_cpu_alert(_metrics: list[dict[str, Any]] | None = None) -> None:
    latest = fetch_latest_metric_row()
    if latest is None:
        return

    cpu_utilization = float(latest.get("cpu", 0))
    memory = float(latest.get("memory", 0))
    cpu_threshold = float(config_store.get().get("cpu_alert_threshold", 80))

    if cpu_utilization <= cpu_threshold:
        return

    worker = threading.Thread(
        target=_dispatch_high_cpu_alert,
        args=(cpu_utilization, memory, cpu_threshold),
        daemon=True,
    )
    worker.start()


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OmniKube | Executive Cloud Command</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script>
    tailwind.config = {
      theme: {
        extend: {
          colors: {
            midnight: '#0b0f19',
            panel: '#111827',
            accent: '#6366f1',
            glow: '#22d3ee',
          }
        }
      }
    }
  </script>
  <style>
    body { background: radial-gradient(circle at top, #1e1b4b 0%, #0b0f19 45%, #020617 100%); }
    .glass { background: rgba(17, 24, 39, 0.72); backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,0.08); }
    .metric-glow { box-shadow: 0 0 40px rgba(99, 102, 241, 0.25); }
    .alert-pulse { animation: alertPulse 1.2s ease-in-out infinite; }
    @keyframes alertPulse {
      0%, 100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.45); }
      50% { box-shadow: 0 0 24px 6px rgba(239, 68, 68, 0.35); }
    }
    input[type=range] { -webkit-appearance: none; appearance: none; height: 6px; border-radius: 9999px; background: rgba(99,102,241,0.35); outline: none; }
    input[type=range]::-webkit-slider-thumb { -webkit-appearance: none; width: 18px; height: 18px; border-radius: 50%; background: #818cf8; cursor: pointer; box-shadow: 0 0 12px rgba(129,140,248,0.6); }
    input[type=range]::-moz-range-thumb { width: 18px; height: 18px; border-radius: 50%; background: #818cf8; cursor: pointer; border: none; }
  </style>
</head>
<body class="min-h-screen text-slate-100 antialiased">
  <div class="max-w-7xl mx-auto px-6 py-10 space-y-8">
    <header>
      <p class="text-sm uppercase tracking-[0.35em] text-indigo-300/80">OmniKube CloudMetrics</p>
      <h1 class="text-4xl md:text-5xl font-semibold mt-2 bg-gradient-to-r from-white via-indigo-100 to-cyan-200 bg-clip-text text-transparent">
        Executive Infrastructure Command
      </h1>
      <p class="text-slate-400 mt-3 max-w-2xl">
        Premium SaaS telemetry for founders operating mission-critical Kubernetes fleets.
      </p>
    </header>

    <section class="grid md:grid-cols-3 gap-6">
      <article class="glass metric-glow rounded-3xl p-8">
        <p class="text-sm uppercase tracking-widest text-slate-400">Max Peak CPU</p>
        <p id="max-peak-cpu" class="text-6xl font-bold mt-4 text-amber-200">--%</p>
        <p id="max-peak-memory" class="text-slate-400 mt-3 text-sm">Memory peak: --%</p>
      </article>
      <article class="glass metric-glow rounded-3xl p-8">
        <p class="text-sm uppercase tracking-widest text-slate-400">Historical Average</p>
        <p id="avg-cpu" class="text-6xl font-bold mt-4 text-indigo-200">--%</p>
        <p id="avg-memory" class="text-slate-400 mt-3 text-sm">Memory avg: --%</p>
      </article>
      <article id="status-badge" class="glass metric-glow rounded-3xl p-8 flex flex-col justify-center transition-all duration-300">
        <p class="text-sm uppercase tracking-widest text-slate-400">System Status</p>
        <div class="flex items-center gap-3 mt-4">
          <span class="relative flex h-4 w-4">
            <span id="status-ping" class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
            <span id="status-dot" class="relative inline-flex rounded-full h-4 w-4 bg-emerald-500"></span>
          </span>
          <p id="status-value" class="text-3xl font-bold text-emerald-300">Protected</p>
        </div>
        <p id="status-label" class="text-slate-400 mt-3 text-sm">Live CPU: <span id="live-cpu">--</span>% | Memory: <span id="live-memory">--</span>%</p>
      </article>
    </section>

    <section class="glass rounded-3xl p-6">
      <div class="flex items-center justify-between mb-5 gap-4">
        <h2 class="text-xl font-semibold">Rolling Telemetry Chart</h2>
        <div class="flex items-center gap-3">
          <select id="timeframe-select"
            class="rounded-xl bg-slate-900/80 border border-white/10 px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-500">
            <option value="1h">Last 1 Hour</option>
            <option value="6h">Last 6 Hours</option>
            <option value="12h">Last 12 Hours</option>
            <option value="24h" selected>Last 24 Hours</option>
          </select>
          <span id="last-updated" class="text-xs text-slate-400">Awaiting sync...</span>
        </div>
      </div>
      <div class="h-72">
        <canvas id="metrics-chart"></canvas>
      </div>
      <div class="mt-4 grid grid-cols-2 gap-4 text-sm text-slate-400">
        <p>Timeframe Avg CPU: <span id="chart-avg-cpu" class="font-medium text-indigo-200">--%</span></p>
        <p class="text-right">Timeframe Avg Memory: <span id="chart-avg-memory" class="font-medium text-cyan-200">--%</span></p>
      </div>
    </section>

    <section class="grid lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 glass rounded-3xl p-6">
        <div class="flex items-center justify-between mb-5">
          <h2 class="text-xl font-semibold">Historical Cluster Logs</h2>
          <span id="sample-count" class="text-xs text-slate-400">0 samples</span>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full text-sm">
            <thead class="text-left text-slate-400 border-b border-white/10">
              <tr>
                <th class="py-3 pr-4">ID</th>
                <th class="py-3 pr-4">Timestamp</th>
                <th class="py-3 pr-4">CPU</th>
                <th class="py-3">Memory</th>
              </tr>
            </thead>
            <tbody id="metrics-table" class="divide-y divide-white/5"></tbody>
          </table>
        </div>
      </div>

      <aside class="glass rounded-3xl p-6 border border-indigo-500/20">
        <div class="flex items-center gap-2 mb-2">
          <span class="px-2 py-1 rounded-full text-xs font-semibold bg-indigo-500/20 text-indigo-200">PRO TIER</span>
        </div>
        <h2 class="text-xl font-semibold">Pro Tier Settings</h2>
        <p class="text-slate-400 text-sm mt-2 mb-6">
          Configure executive alerting channels and CPU thresholds for automated webhook dispatch.
        </p>
        <form id="settings-form" class="space-y-4">
          <label class="block">
            <span class="text-sm text-slate-300">Discord Webhook URL</span>
            <input id="discord-webhook" type="url" placeholder="https://discord.com/api/webhooks/..."
              class="mt-2 w-full rounded-xl bg-slate-900/80 border border-white/10 px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500" />
          </label>
          <label class="block">
            <span class="text-sm text-slate-300">Slack Webhook URL</span>
            <input id="slack-webhook" type="url" placeholder="https://hooks.slack.com/services/..."
              class="mt-2 w-full rounded-xl bg-slate-900/80 border border-white/10 px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500" />
          </label>
          <label class="block">
            <div class="flex items-center justify-between">
              <span class="text-sm text-slate-300">CPU Alert Threshold</span>
              <span id="cpu-threshold-value" class="text-sm font-semibold text-indigo-300">80%</span>
            </div>
            <input id="cpu-threshold" type="range" min="1" max="100" step="1" value="80"
              class="mt-3 w-full" />
          </label>
          <button type="submit"
            class="w-full rounded-xl bg-indigo-500 hover:bg-indigo-400 transition-colors py-3 font-semibold">
            Save Config
          </button>
          <button type="button" id="test-webhook-btn"
            class="w-full rounded-xl bg-slate-800 hover:bg-slate-700 border border-white/10 transition-colors py-3 font-semibold">
            Test Webhook
          </button>
          <p id="settings-status" class="text-xs hidden"></p>
        </form>
      </aside>
    </section>
  </div>

  <script>
    const API_TOKEN = "premium_secret_2026";
    let cpuThreshold = 80;
    let metricsChart = null;

    const authHeaders = {
      "Content-Type": "application/json",
      "X-OmniKube-Token": API_TOKEN
    };

    function initChart() {
      const ctx = document.getElementById("metrics-chart").getContext("2d");
      metricsChart = new Chart(ctx, {
        type: "line",
        data: {
          labels: [],
          datasets: [
            {
              label: "CPU %",
              data: [],
              borderColor: "#818cf8",
              backgroundColor: "rgba(129, 140, 248, 0.15)",
              tension: 0.35,
              fill: true,
              pointRadius: 2
            },
            {
              label: "Memory %",
              data: [],
              borderColor: "#22d3ee",
              backgroundColor: "rgba(34, 211, 238, 0.12)",
              tension: 0.35,
              fill: true,
              pointRadius: 2
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: {
            mode: "index",
            intersect: false
          },
          plugins: {
            legend: { labels: { color: "#cbd5e1" } },
            tooltip: {
              mode: "index",
              intersect: false,
              backgroundColor: "rgba(15, 23, 42, 0.95)",
              titleColor: "#e2e8f0",
              bodyColor: "#cbd5e1",
              borderColor: "rgba(129, 140, 248, 0.35)",
              borderWidth: 1,
              padding: 12,
              callbacks: {
                label: (context) => `${context.dataset.label}: ${Math.round(context.parsed.y)}%`
              }
            }
          },
          scales: {
            x: { ticks: { color: "#94a3b8", maxTicksLimit: 8 }, grid: { color: "rgba(255,255,255,0.05)" } },
            y: { min: 0, max: 100, ticks: { color: "#94a3b8" }, grid: { color: "rgba(255,255,255,0.05)" } }
          }
        }
      });
    }

    function setStatusAlert(isAlert) {
      const badge = document.getElementById("status-badge");
      const ping = document.getElementById("status-ping");
      const dot = document.getElementById("status-dot");
      const value = document.getElementById("status-value");

      if (isAlert) {
        badge.classList.add("alert-pulse", "border", "border-red-500/40");
        ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75";
        dot.className = "relative inline-flex rounded-full h-4 w-4 bg-red-500";
        value.className = "text-3xl font-bold text-red-300";
        value.textContent = "Alert: High Load";
        return;
      }

      badge.classList.remove("alert-pulse", "border", "border-red-500/40");
      ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75";
      dot.className = "relative inline-flex rounded-full h-4 w-4 bg-emerald-500";
      value.className = "text-3xl font-bold text-emerald-300";
      value.textContent = "Protected";
    }

    function updateChartAverages(cpuData, memoryData) {
      if (!cpuData.length) {
        document.getElementById("chart-avg-cpu").textContent = "--%";
        document.getElementById("chart-avg-memory").textContent = "--%";
        return;
      }

      const avgCpu = cpuData.reduce((sum, value) => sum + value, 0) / cpuData.length;
      const avgMemory = memoryData.reduce((sum, value) => sum + value, 0) / memoryData.length;
      document.getElementById("chart-avg-cpu").textContent = `${Math.round(avgCpu)}%`;
      document.getElementById("chart-avg-memory").textContent = `${Math.round(avgMemory)}%`;
    }

    function updateChart(records) {
      if (!metricsChart || !records.length) {
        updateChartAverages([], []);
        return;
      }

      const chronological = [...records].reverse();
      const cpuData = chronological.map((row) => Number(row.cpu));
      const memoryData = chronological.map((row) => Number(row.memory));

      metricsChart.data.labels = chronological.map((row) => row.timestamp.split(" ").slice(-2).join(" "));
      metricsChart.data.datasets[0].data = cpuData;
      metricsChart.data.datasets[1].data = memoryData;
      metricsChart.update("none");
      updateChartAverages(cpuData, memoryData);
    }

    function getSelectedTimeframe() {
      return document.getElementById("timeframe-select").value;
    }

    function renderAnalytics(analytics) {
      document.getElementById("max-peak-cpu").textContent = `${Math.round(analytics.max_cpu)}%`;
      document.getElementById("max-peak-memory").textContent = `Memory peak: ${Math.round(analytics.max_memory)}%`;
      document.getElementById("avg-cpu").textContent = `${Math.round(analytics.avg_cpu)}%`;
      document.getElementById("avg-memory").textContent = `Memory avg: ${Math.round(analytics.avg_memory)}%`;
      document.getElementById("sample-count").textContent = `${analytics.sample_count} samples`;
    }

    function renderMetrics(records) {
      if (!records.length) return;

      const latest = records[0];
      const cpu = Number(latest.cpu);
      const memory = Number(latest.memory);

      document.getElementById("live-cpu").textContent = Math.round(cpu);
      document.getElementById("live-memory").textContent = Math.round(memory);
      document.getElementById("last-updated").textContent = `Last sync: ${latest.timestamp}`;
      setStatusAlert(cpu > cpuThreshold);
      updateChart(records);

      const tbody = document.getElementById("metrics-table");
      tbody.innerHTML = records.map((row) => `
        <tr class="hover:bg-white/5 transition-colors">
          <td class="py-3 pr-4 text-slate-300">#${row.id}</td>
          <td class="py-3 pr-4">${row.timestamp}</td>
          <td class="py-3 pr-4 font-medium ${Number(row.cpu) > cpuThreshold ? "text-red-300" : "text-indigo-200"}">${row.cpu}%</td>
          <td class="py-3 font-medium text-cyan-200">${row.memory}%</td>
        </tr>
      `).join("");
    }

    async function loadSettings() {
      try {
        const response = await fetch("/api/settings", { headers: authHeaders });
        if (!response.ok) return;
        const settings = await response.json();
        document.getElementById("discord-webhook").value = settings.discord_webhook_url || "";
        document.getElementById("slack-webhook").value = settings.slack_webhook_url || "";
        const threshold = Number(settings.cpu_alert_threshold ?? 80);
        document.getElementById("cpu-threshold").value = threshold;
        document.getElementById("cpu-threshold-value").textContent = `${threshold}%`;
        cpuThreshold = threshold;
      } catch (error) {
        console.error("Settings load failed:", error);
      }
    }

    document.getElementById("cpu-threshold").addEventListener("input", (event) => {
      const value = Number(event.target.value);
      document.getElementById("cpu-threshold-value").textContent = `${value}%`;
    });

    document.getElementById("settings-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      const status = document.getElementById("settings-status");

      const payload = {
        discord_webhook_url: document.getElementById("discord-webhook").value.trim(),
        slack_webhook_url: document.getElementById("slack-webhook").value.trim(),
        cpu_alert_threshold: Number(document.getElementById("cpu-threshold").value)
      };

      try {
        const response = await fetch("/api/settings", {
          method: "POST",
          headers: authHeaders,
          body: JSON.stringify(payload)
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "Save failed");

        cpuThreshold = Number(result.cpu_alert_threshold ?? 80);
        status.textContent = "Configuration saved successfully.";
        status.className = "text-xs text-emerald-300";
        status.classList.remove("hidden");
        await refreshDashboard();
      } catch (error) {
        status.textContent = error.message || "Unable to save configuration.";
        status.className = "text-xs text-red-300";
        status.classList.remove("hidden");
      }

      setTimeout(() => status.classList.add("hidden"), 3000);
    });

    document.getElementById("test-webhook-btn").addEventListener("click", async () => {
      const status = document.getElementById("settings-status");
      try {
        const response = await fetch("/api/test-webhook", {
          method: "POST",
          headers: authHeaders,
          body: "{}"
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "Test failed");
        status.textContent = `Test webhook sent: ${JSON.stringify(result.results)}`;
        status.className = "text-xs text-emerald-300";
        status.classList.remove("hidden");
      } catch (error) {
        status.textContent = error.message || "Webhook test failed.";
        status.className = "text-xs text-red-300";
        status.classList.remove("hidden");
      }
      setTimeout(() => status.classList.add("hidden"), 4000);
    });

    async function refreshDashboard() {
      try {
        const timeframe = getSelectedTimeframe();
        const [metricsRes, analyticsRes] = await Promise.all([
          fetch(`/api/metrics?timeframe=${encodeURIComponent(timeframe)}`, {
            headers: { "X-OmniKube-Token": API_TOKEN }
          }),
          fetch("/api/analytics", { headers: { "X-OmniKube-Token": API_TOKEN } })
        ]);
        if (metricsRes.ok) {
          renderMetrics(await metricsRes.json());
        }
        if (analyticsRes.ok) {
          renderAnalytics(await analyticsRes.json());
        }
      } catch (error) {
        console.error("Dashboard sync failed:", error);
      }
    }

    document.getElementById("timeframe-select").addEventListener("change", refreshDashboard);

    initChart();
    loadSettings().then(refreshDashboard);
    setInterval(refreshDashboard, 5000);
  </script>
</body>
</html>
"""


class ManagementHandler(BaseHTTPRequestHandler):
    def _read_json_body(self) -> dict[str, Any] | None:
        try:
            length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return None

        if length == 0:
            return {}

        try:
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else None
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return None

    def _send_json(self, status: int, payload: object) -> None:
        try:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _send_html(self, html: str) -> None:
        try:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _is_authorized(self) -> bool:
        return self.headers.get("X-OmniKube-Token") == API_TOKEN

    def _handle_metrics(self) -> None:
        if not self._is_authorized():
            self._send_json(401, {"error": "Unauthorized. Valid X-OmniKube-Token required."})
            return

        params = parse_qs(urlparse(self.path).query)
        timeframe = params.get("timeframe", [None])[0]
        metrics = fetch_metrics(METRICS_LIMIT, timeframe=timeframe)
        maybe_trigger_cpu_alert(metrics)
        self._send_json(200, metrics)

    def _handle_settings_get(self) -> None:
        if not self._is_authorized():
            self._send_json(401, {"error": "Unauthorized. Valid X-OmniKube-Token required."})
            return

        self._send_json(200, config_store.get())

    def _handle_settings_post(self) -> None:
        if not self._is_authorized():
            self._send_json(401, {"error": "Unauthorized. Valid X-OmniKube-Token required."})
            return

        payload = self._read_json_body()
        if payload is None:
            self._send_json(400, {"error": "Invalid JSON payload."})
            return

        try:
            if "cpu_alert_threshold" in payload:
                threshold = float(payload["cpu_alert_threshold"])
                if not 1 <= threshold <= 100:
                    raise ValueError("CPU threshold must be between 1 and 100.")
            updated = config_store.update(payload)
            self._send_json(200, updated)
        except (TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _handle_analytics(self) -> None:
        if not self._is_authorized():
            self._send_json(401, {"error": "Unauthorized. Valid X-OmniKube-Token required."})
            return

        self._send_json(200, fetch_analytics())

    def _handle_test_webhook(self) -> None:
        if not self._is_authorized():
            self._send_json(401, {"error": "Unauthorized. Valid X-OmniKube-Token required."})
            return

        result = dispatch_test_webhooks()
        if "error" in result and not result.get("results"):
            self._send_json(400, result)
            return

        self._send_json(200, result)

    def do_GET(self) -> None:
        try:
            if self.path == "/":
                self._send_html(DASHBOARD_HTML)
                return

            if urlparse(self.path).path == "/api/metrics":
                self._handle_metrics()
                return

            if self.path == "/api/analytics":
                self._handle_analytics()
                return

            if self.path == "/api/settings":
                self._handle_settings_get()
                return

            self.send_error(404, "Not Found")
        except Exception as exc:
            print(f"[OmniKube Server] GET {self.path} failed: {exc}")
            self._send_json(500, {"error": "Internal server error."})

    def do_POST(self) -> None:
        try:
            if self.path == "/api/settings":
                self._handle_settings_post()
                return

            if self.path == "/api/test-webhook":
                self._handle_test_webhook()
                return

            self.send_error(404, "Not Found")
        except Exception as exc:
            print(f"[OmniKube Server] POST {self.path} failed: {exc}")
            self._send_json(500, {"error": "Internal server error."})

    def log_message(self, format: str, *args) -> None:
        print(f"[OmniKube Server] {self.address_string()} - {format % args}")


def main() -> None:
    config_store.load()
    init_database()

    server = HTTPServer((HOST, PORT), ManagementHandler)
    print(f"[OmniKube Server] Management gateway listening on http://{HOST}:{PORT}")
    print(
        "[OmniKube Server] Dashboard: /  |  API: /api/metrics, /api/analytics, "
        "/api/settings, /api/test-webhook"
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[OmniKube Server] Stopped.")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
