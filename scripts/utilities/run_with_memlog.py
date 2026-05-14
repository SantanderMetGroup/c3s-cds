#!/usr/bin/env python3
"""Run a Python script and report peak RSS memory usage.

Usage:
    python scripts/utilities/run_with_memlog.py <script.py> [args...]
"""
import subprocess
import sys
import threading
import time


def monitor_memory(pid: int, stop: threading.Event, history: list) -> None:
    start_time = time.time()
    while not stop.is_set():
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        val = int(line.split()[1])
                        # Guardamos tiempo relativo en segundos y memoria en MB
                        history.append((time.time() - start_time, val / (1024.0 ** 2)))
                        break
        except (FileNotFoundError, IOError, OSError):
            pass
        stop.wait(1)


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
    history = []
    t = threading.Thread(target=monitor_memory, args=(process.pid, stop, history), daemon=True)
    t.start()

    process.wait()
    stop.set()
    t.join()

    try:
        import resource
        rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
        peak_mb = rusage.ru_maxrss / 1024 ** 2
        print(f"\nPeak RSS (resource.RUSAGE_CHILDREN): {peak_mb:.2f} GB")
    except (ImportError, AttributeError):
        print("\n(Could not get rusage from resource module)")

    if history:
        try:
            # Se importa al final de la ejecución para que no cause overhead mientras el subproceso corre
            import matplotlib.pyplot as plt
            times, mems = zip(*history)
            
            plt.figure(figsize=(10, 5))
            plt.plot(times, mems, marker='.', linestyle='-', color='b', label='VmRSS')
            plt.title('Memory Usage (VmRSS) over Time')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Memory (GB)')
            plt.grid(True)
            plt.legend()
            
            out_img = 'memory_usage_plot.png'
            plt.savefig(out_img)
            print(f"Memory usage plot saved to {out_img}")
        except ImportError:
            print("\nmatplotlib is not installed. Skipping plot generation.")

    return process.returncode

if __name__ == "__main__":
    sys.exit(main())