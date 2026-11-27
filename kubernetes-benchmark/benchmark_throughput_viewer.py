"""Open the offered-load comparisons and achieved-throughput/latency curves."""

from benchmark_viewer import run_viewer

if __name__ == "__main__":
    run_viewer(sweep=True)
