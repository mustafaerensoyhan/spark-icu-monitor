#!/usr/bin/env python3
"""
Stream generator for the ICU heart-rate monitoring pipeline.

Spark Structured Streaming can watch a directory and treat every NEW file that
appears in it as a fresh micro-batch of streaming data. This script produces
those files: it replays heart-rate readings into ./stream_input/ one small CSV
at a time, pausing between files, so Spark sees a live feed.

Data source:
  * If data/iomt_sample.csv exists, realistic heart-rate VALUES are sampled from
    it (the heart-rate column is auto-detected, so any IoMT CSV works).
  * If that file is missing, realistic values are generated synthetically, so
    the pipeline is never blocked waiting on the dataset.

Simulated, compressed event time:
  Each batch advances a SIMULATED clock by BATCH_SECONDS, while we only sleep
  REAL_SLEEP real seconds between files. That lets a 2-minute tumbling window
  fill and close within seconds of wall-clock time, so the alert fires quickly
  enough to screenshot. Spark windows on the event_time column written here,
  NOT on wall-clock time, which is why this works.

Guaranteed anomaly:
  Two "at-risk" patients are deliberately driven above 100 bpm for a sustained
  stretch covering at least two consecutive windows, so the sustained-HR alert
  reliably triggers during a demo run.
"""

import csv
import os
import random
import time
from datetime import datetime, timedelta

# ---------------------------------------------------------------- configuration
INPUT_DIR = "stream_input"                 # the folder Spark watches
DATA_CSV = os.path.join("data", "iomt_sample.csv")

NORMAL_PATIENTS = ["P-001", "P-002", "P-003", "P-004", "P-005", "P-006"]
ATRISK_PATIENTS = ["P-007", "P-008"]       # these two will sustain a high HR
ALL_PATIENTS = NORMAL_PATIENTS + ATRISK_PATIENTS

BATCH_SECONDS = 30        # simulated seconds advanced per batch
REAL_SLEEP = 1.5          # real seconds to wait between writing files
NUM_BATCHES = 24          # 24 * 30s = 12 simulated minutes -> ~6 windows
ATRISK_START_BATCH = 4    # at-risk patients run hot from this batch...
ATRISK_END_BATCH = 14     # ...up to (not including) this batch -> >=2 windows


def load_real_hr_values(path):
    """Return a list of realistic heart-rate floats from the dataset, or None.

    Auto-detects the heart-rate column by name so the script works with whatever
    IoMT CSV you downloaded, regardless of its exact column layout.
    """
    if not os.path.exists(path):
        return None
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return None
        hr_idx = None
        for i, name in enumerate(header):
            n = name.strip().lower()
            if "heart" in n or "bpm" in n or "pulse" in n or n == "hr":
                hr_idx = i
                break
        if hr_idx is None:
            return None
        values = []
        for row in reader:
            if len(row) <= hr_idx:
                continue
            try:
                v = float(row[hr_idx])
            except ValueError:
                continue
            if 20 < v < 250:          # keep only physiologically sane values
                values.append(v)
    return values or None


def normal_hr(pool):
    """A normal-ish heart rate (real value if we have a dataset, else synthetic)."""
    if pool:
        candidates = [v for v in pool if v <= 100]
        if candidates:
            return round(random.choice(candidates))
    return random.randint(60, 95)


def high_hr(pool):
    """An elevated heart rate (>100), preferring real high values if available."""
    if pool:
        highs = [v for v in pool if v > 100]
        if highs:
            return round(random.choice(highs))
    return random.randint(105, 130)


def write_atomic(directory, filename, rows):
    """Write rows to a temp file then rename into place.

    The rename is atomic on a single filesystem, which guarantees Spark never
    reads a half-written file (a classic file-streaming pitfall).
    """
    tmp = os.path.join(directory, "." + filename + ".tmp")
    final = os.path.join(directory, filename)
    with open(tmp, "w", newline="") as f:
        writer = csv.writer(f)
        for r in rows:
            writer.writerow(r)
    os.rename(tmp, final)


def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    real_pool = load_real_hr_values(DATA_CSV)
    source = f"real values from {DATA_CSV}" if real_pool else "synthetic (no dataset found)"
    print(f"[generator] data source : {source}")
    print(f"[generator] writing to  : ./{INPUT_DIR}/   (Ctrl+C to stop early)")
    print(f"[generator] at-risk      : {ATRISK_PATIENTS} run hot batches "
          f"{ATRISK_START_BATCH}-{ATRISK_END_BATCH - 1}\n")

    sim_clock = datetime(2025, 1, 1, 8, 0, 0)   # arbitrary fixed start time

    for b in range(NUM_BATCHES):
        rows = []
        for p in ALL_PATIENTS:
            # spread each reading randomly within this batch's 30s span
            ts = sim_clock + timedelta(seconds=random.randint(0, BATCH_SECONDS - 1))
            if p in ATRISK_PATIENTS and ATRISK_START_BATCH <= b < ATRISK_END_BATCH:
                hr = high_hr(real_pool)
            else:
                hr = normal_hr(real_pool)
            rows.append((ts.strftime("%Y-%m-%d %H:%M:%S"), p, hr))

        write_atomic(INPUT_DIR, f"batch_{b:03d}.csv", rows)
        print(f"[generator] batch {b:03d}  sim_time={sim_clock:%H:%M:%S}  rows={len(rows)}")

        sim_clock += timedelta(seconds=BATCH_SECONDS)
        time.sleep(REAL_SLEEP)

    # Flush marker: a far-future timestamp jumps the watermark past the last real
    # window so Spark finalizes and emits any remaining windows right away.
    flush_ts = (sim_clock + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    write_atomic(INPUT_DIR, "zzz_flush.csv", [(flush_ts, "P-000", 70)])
    print("\n[generator] flush marker written. Done.")


if __name__ == "__main__":
    main()