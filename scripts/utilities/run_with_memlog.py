#!/usr/bin/env python3
"""Run a Python script and report peak RSS memory usage.

Usage:
    python scripts/utilities/run_with_memlog.py <script.py> [args...]
"""
import subprocess
import sys
import threading
import time


def monitor_memory(pid: int, stop: threading.Event) -> int:
    peak_kb = 0
    while not stop.is_set():
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        val = int(line.split()[1])
                        if val > peak_kb:
                            peak_kb = val
                        break
        except (FileNotFoundError, IOError, OSError):
            pass
        stop.wait(1)
    return peak_kb


def main() -> int:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <script.py> [args...]", file=sys.stderr)
        return 1

    cmd = [sys.executable] + sys.argv[1:]

    print(f"PID: will start")
    print(f"Running: {' '.join(cmd)}")
    print()

    process = subprocess.Popen(cmd)
    print(f"PID: {process.pid}")
    print()

    stop = threading.Event()
    t = threading.Thread(target=monitor_memory, args=(process.pid, stop), daemon=True)
    t.start()

    process.wait()
    stop.set()
    t.join()

    try:
        import resource
        rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
        peak_mb = rusage.ru_maxrss / 1024
        print(f"\nPeak RSS (resource.RUSAGE_CHILDREN): {peak_mb:.2f} MB")
    except (ImportError, AttributeError):
        print("\n(Could not get rusage from resource module)")

    return process.returncode


if __name__ == "__main__":
    sys.exit(main())
