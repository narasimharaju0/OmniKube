import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import psutil

INTERVAL_SEC = 5
HOST = "0.0.0.0"
PORT = 8080


class MetricsState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.cpu = 0.0
        self.memory = 0.0

    def update(self, cpu: float, memory: float) -> None:
        with self._lock:
            self.cpu = cpu
            self.memory = memory

    def snapshot(self) -> tuple[float, float]:
        with self._lock:
            return self.cpu, self.memory


metrics_state = MetricsState()


def collect_metrics_loop() -> None:
    psutil.cpu_percent(interval=None)

    while True:
        cpu = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory().percent
        metrics_state.update(cpu, memory)
        print(f"[CloudMetrics] CPU: {cpu:.0f}% | Memory: {memory:.0f}%")
        time.sleep(INTERVAL_SEC)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_error(404, "Not Found")
            return

        cpu, memory = metrics_state.snapshot()
        body = f"CPU: {cpu:.0f}%\nMemory: {memory:.0f}%\n"

        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    print(
        "[CloudMetrics] Starting metrics collector "
        f"(interval: {INTERVAL_SEC}s, endpoint: /metrics). Press Ctrl+C to stop."
    )

    collector = threading.Thread(target=collect_metrics_loop, daemon=True)
    collector.start()

    server = HTTPServer((HOST, PORT), MetricsHandler)
    print(f"[CloudMetrics] Serving metrics on http://{HOST}:{PORT}/metrics")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[CloudMetrics] Stopped.")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
