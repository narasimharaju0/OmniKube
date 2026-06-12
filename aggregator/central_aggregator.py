import re
import time
import urllib.error
import urllib.request

METRICS_URL = "http://omnikube-agent-service:8080/metrics"
INTERVAL_SEC = 10
REQUEST_TIMEOUT_SEC = 5

CPU_PATTERN = re.compile(r"CPU:\s*([\d.]+)%")
MEMORY_PATTERN = re.compile(r"Memory:\s*([\d.]+)%")


def parse_metrics(body: str) -> tuple[float, float] | None:
    cpu_match = CPU_PATTERN.search(body)
    memory_match = MEMORY_PATTERN.search(body)
    if not cpu_match or not memory_match:
        return None
    return float(cpu_match.group(1)), float(memory_match.group(1))


def fetch_metrics() -> tuple[float, float] | None:
    try:
        with urllib.request.urlopen(METRICS_URL, timeout=REQUEST_TIMEOUT_SEC) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        print(f"[Central Dashboard] Scrape failed: HTTP {exc.code} {exc.reason}")
        return None
    except urllib.error.URLError as exc:
        print(f"[Central Dashboard] Scrape failed: {exc.reason}")
        return None
    except TimeoutError:
        print("[Central Dashboard] Scrape failed: request timed out")
        return None

    metrics = parse_metrics(body)
    if metrics is None:
        print("[Central Dashboard] Scrape failed: could not parse metrics payload")
    return metrics


def main() -> None:
    print(
        "[Central Dashboard] Starting central aggregator "
        f"(interval: {INTERVAL_SEC}s). Press Ctrl+C to stop."
    )

    try:
        while True:
            metrics = fetch_metrics()
            if metrics is not None:
                cpu, memory = metrics
                print(
                    "[Central Dashboard] Scraped Node Data successfully - "
                    f"Avg CPU: {cpu:.0f}% | Avg Memory: {memory:.0f}%"
                )
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n[Central Dashboard] Stopped.")


if __name__ == "__main__":
    main()
