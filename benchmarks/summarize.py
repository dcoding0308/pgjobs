"""Print min-max ranges across all saved benchmark runs, for the README.

uv run python benchmarks/summarize.py
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def main() -> None:
    runs = [json.loads(path.read_text()) for path in sorted(RESULTS_DIR.glob("run-*.json"))]
    if not runs:
        raise SystemExit("no saved runs in benchmarks/results/")

    throughput: dict[tuple[int, int], list[float]] = defaultdict(list)
    latency: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for run in runs:
        for row in run["throughput"]:
            key = (row["processes"], row["concurrency_per_process"])
            throughput[key].append(row["jobs_per_second"])
        for row in run["latency"]:
            for metric in ("p50_ms", "p95_ms", "max_ms"):
                latency[row["mode"]][metric].append(row[metric])

    print(f"{len(runs)} runs: {', '.join(run['recorded_at'] for run in runs)}")
    print(f"machine: {runs[-1]['machine']}\n")
    print("| Worker processes x concurrency | Jobs / second |\n|---|---|")
    for (processes, concurrency), values in sorted(throughput.items()):
        print(f"| {processes} x {concurrency} | {min(values):,.0f} - {max(values):,.0f} |")
    print("\n| Pickup latency | p50 | p95 | max |\n|---|---|---|---|")
    for mode, metrics in latency.items():
        cells = " | ".join(
            f"{min(metrics[m]):.1f} - {max(metrics[m]):.1f} ms"
            for m in ("p50_ms", "p95_ms", "max_ms")
        )
        print(f"| {mode} | {cells} |")


if __name__ == "__main__":
    main()
