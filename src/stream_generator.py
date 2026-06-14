#!/usr/bin/env python3
"""
Stream generator for the ICU heart-rate monitoring pipeline.

Replays heart-rate readings into ./stream_input/ one CSV file at a time so Spark
Structured Streaming sees a live feed. Real values are sampled from
data/iomt_sample.csv when present (heart-rate column auto-detected); otherwise
realistic synthetic values are used.

Event time is simulated and compressed (BATCH_SECONDS per batch while sleeping
only REAL_SLEEP real seconds), so 2-minute windows close within seconds.

Two at-risk patients are driven above 100 bpm for two consecutive windows so the
sustained alert reliably fires.

Pass --late to also inject a few deliberately out-of-order readings, used by
src/late_data_demo.py to demonstrate the watermark dropping late data.
"""

import csv
import os
import random
import sys
import time
from datetime import datetime, timedelta

INPUT_DIR = "stream_input"
DATA_CSV = os.path.join("data", "iomt_sample.csv")

NORMAL_PATIENTS = ["P-001", "P-002", "P-003", "P-004", "P-005", "P-006"]
ATRISK_PATIENTS = ["P-007", "P-008"]
ALL_PATIENTS = NORMAL_PATIENTS + ATRISK_PATIENTS

BATCH_SECONDS = 30
REAL_SLEEP = 1.5
NUM_BATCHES = 24
ATRISK_START_BATCH = 4
ATRISK_END_BATCH = 14


def load_real_hr_values(path):
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
            if 20 < v < 250:
                values.append(v)
    return values or None


def normal_hr(pool):
    if pool:
        candidates = [v for v in pool if v <= 100]
        if candidates:
            return round(random.choice(candidates))
    return random.randint(60, 95)


def high_hr(pool):
    if pool:
        highs = [v for v in pool if v > 100]
        if highs:
            return round(random.choice(highs))
    return random.randint(105, 130)


def write_atomic(directory, filename, rows):
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

    sim_clock = datetime(2025, 1, 1, 8, 0, 0)

    for b in range(NUM_BATCHES):
        rows = []
        for p in ALL_PATIENTS:
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

    # Optional: inject deliberately out-of-order readings to demonstrate the
    # watermark. By now event time is ~08:11, so a reading stamped 08:05 is far
    # behind the 2-minute watermark and will be dropped.
    if "--late" in sys.argv:
        late_ts = datetime(2025, 1, 1, 8, 5, 0).strftime("%Y-%m-%d %H:%M:%S")
        write_atomic(INPUT_DIR, "yzz_late.csv", [
            (late_ts, "P-007", 200),
            (late_ts, "P-008", 195),
            (late_ts, "P-001", 188),
        ])
        print("[generator] LATE data injected: 3 out-of-order readings stamped 08:05:00")

    # Flush marker: a far-future timestamp finalizes any remaining windows.
    flush_ts = (sim_clock + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    write_atomic(INPUT_DIR, "zzz_flush.csv", [(flush_ts, "P-000", 70)])
    print("\n[generator] flush marker written. Done.")


if __name__ == "__main__":
    main()
