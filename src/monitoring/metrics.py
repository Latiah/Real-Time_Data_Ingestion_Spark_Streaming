"""
Performance metrics collection.

Every micro-batch write appends one line to a CSV file in
outputs/performance/. generate_performance_report() then aggregates that
CSV into the summary described in docs/performance_metrics.md.

Metrics are measured directly from real pipeline runs -- nothing here
fabricates numbers. Until the pipeline has actually been run, the report
will simply be empty/absent, which is the intended, honest behavior.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.config.settings import get_paths_config

logger = logging.getLogger(__name__)

METRICS_FILENAME = "batch_metrics.csv"
METRICS_FIELDS = ["batch_id", "record_count", "elapsed_seconds", "throughput_rows_per_sec", "recorded_at"]


def _metrics_file() -> Path:
    return get_paths_config().performance_dir / METRICS_FILENAME


def record_batch_metrics(batch_id: int, record_count: int, elapsed_seconds: float) -> None:
    """Append one row of measured metrics for a single micro-batch."""
    path = _metrics_file()
    file_exists = path.exists()

    throughput = record_count / elapsed_seconds if elapsed_seconds > 0 else float("inf")

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "batch_id": batch_id,
                "record_count": record_count,
                "elapsed_seconds": round(elapsed_seconds, 4),
                "throughput_rows_per_sec": round(throughput, 2),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )


def generate_performance_report() -> dict:
    """
    Aggregate outputs/performance/batch_metrics.csv into summary statistics.

    Returns a dict of computed metrics, or an empty dict with a warning log
    if no batches have been recorded yet (e.g. pipeline never run).
    """
    path = _metrics_file()
    if not path.exists():
        logger.warning("No metrics file found at %s -- run the pipeline first.", path)
        return {}

    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return {}

    total_records = sum(int(r["record_count"]) for r in rows)
    total_time = sum(float(r["elapsed_seconds"]) for r in rows)
    avg_batch_time = total_time / len(rows)
    avg_throughput = sum(float(r["throughput_rows_per_sec"]) for r in rows) / len(rows)
    overall_throughput = total_records / total_time if total_time > 0 else float("inf")

    return {
        "total_batches": len(rows),
        "total_records_processed": total_records,
        "total_processing_time_seconds": round(total_time, 4),
        "average_batch_processing_time_seconds": round(avg_batch_time, 4),
        "average_throughput_rows_per_sec": round(avg_throughput, 2),
        "overall_throughput_rows_per_sec": round(overall_throughput, 2),
    }


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(generate_performance_report(), indent=2))
