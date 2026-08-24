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
import math
from datetime import datetime, timezone
from pathlib import Path

from src.config.settings import get_paths_config

logger = logging.getLogger(__name__)

METRICS_FILENAME = "batch_metrics.csv"
METRICS_FIELDS = ["batch_id", "record_count", "elapsed_seconds", "throughput_rows_per_sec", "recorded_at"]


def _finite(value: str) -> float | None:
    """
    Parse a CSV cell as a finite float, or return None.

    Guards the aggregates below against non-numeric, blank, and infinite
    cells. `inf` matters in particular: an older version of this module wrote
    it whenever a batch was measured at zero seconds, and because the metrics
    file is append-only, one such row would otherwise make every average
    computed from it `inf` forever.
    """
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _metrics_file() -> Path:
    return get_paths_config().performance_dir / METRICS_FILENAME


def record_batch_metrics(batch_id: int, record_count: int, elapsed_seconds: float) -> None:
    """Append one row of measured metrics for a single micro-batch."""
    path = _metrics_file()
    file_exists = path.exists()

    # Left blank rather than recorded as inf when the batch was too fast to
    # measure: an infinite cell would propagate into every average derived
    # from this append-only file. A blank is skipped by _finite() instead.
    throughput = round(record_count / elapsed_seconds, 2) if elapsed_seconds > 0 else ""

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(
            {
                "batch_id": batch_id,
                "record_count": record_count,
                "elapsed_seconds": round(elapsed_seconds, 4),
                "throughput_rows_per_sec": throughput,
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

    total_records = sum(int(r["record_count"]) for r in rows if _finite(r["record_count"]) is not None)

    batch_times = [t for t in (_finite(r["elapsed_seconds"]) for r in rows) if t is not None]
    throughputs = [t for t in (_finite(r["throughput_rows_per_sec"]) for r in rows) if t is not None]

    total_time = sum(batch_times)

    report = {
        "total_batches": len(rows),
        "total_records_processed": total_records,
        "total_processing_time_seconds": round(total_time, 4),
        "average_batch_processing_time_seconds": (
            round(total_time / len(batch_times), 4) if batch_times else None
        ),
        "average_throughput_rows_per_sec": (
            round(sum(throughputs) / len(throughputs), 2) if throughputs else None
        ),
        # Guarded rather than reported as inf: a total of zero means nothing
        # measurable was written, for which no throughput figure is honest.
        "overall_throughput_rows_per_sec": (
            round(total_records / total_time, 2) if total_time > 0 else None
        ),
    }

    skipped = len(rows) - len(throughputs)
    if skipped:
        logger.warning(
            "%d of %d rows had an unusable throughput value and were excluded "
            "from the averages.", skipped, len(rows),
        )

    return report


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(generate_performance_report(), indent=2))
