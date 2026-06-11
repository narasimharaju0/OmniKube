import time

import psutil

INTERVAL_SEC = 5


def main() -> None:
    print(
        "[CloudMetrics] Starting metrics collector "
        f"(interval: {INTERVAL_SEC}s). Press Ctrl+C to stop."
    )
    psutil.cpu_percent(interval=None)

    try:
        while True:
            time.sleep(INTERVAL_SEC)
            cpu = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory().percent
            print(f"[CloudMetrics] CPU: {cpu:.0f}% | Memory: {memory:.0f}%")
    except KeyboardInterrupt:
        print("\n[CloudMetrics] Stopped.")


if __name__ == "__main__":
    main()
