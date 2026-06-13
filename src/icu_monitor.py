#!/usr/bin/env python3
"""
ICU heart-rate monitor - Spark Structured Streaming (Scenario B).

Reads a stream of (event_time, patient_id, heart_rate) rows from a watched
folder, groups them into TUMBLING 2-minute windows, computes each patient's
average heart rate per window, and raises a SUSTAINED clinical alert when a
patient's average exceeds 100 bpm in TWO CONSECUTIVE windows (so a single
spike does not trigger it - only a sustained elevation does).

Pipeline:
  readStream(watched dir) -> parse event_time -> withWatermark(2 min)
  -> groupBy(2-min tumbling window, patient) -> avg(hr)
  -> filter(avg_hr > 100)   # the filtered alert output stream
  -> foreachBatch -> escalate to a SUSTAINED alert on 2 consecutive windows

Where state lives:
  1. Spark-managed: the windowed average per (patient, window), bounded by the
     watermark, which lets Spark finalize a window and drop its state.
  2. Application: `elevated_history` remembers each patient's elevated window
     starts, so two-consecutive-window detection works across micro-batches.
"""

import os
import shutil
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

INPUT_DIR = "stream_input"
CHECKPOINT_DIR = "checkpoint"
WINDOW_DURATION = "2 minutes"
WATERMARK_DELAY = "2 minutes"
HR_THRESHOLD = 100.0

SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("heart_rate", DoubleType(), True),
])

elevated_history = {}


def handle_batch(batch_df, batch_id):
    rows = batch_df.orderBy("patient_id", "win_start").collect()
    if not rows:
        return
    print(f"\n================ micro-batch {batch_id}: elevated windows ================")
    for r in rows:
        pid = r["patient_id"]
        start = r["win_start"]
        end = r["win_end"]
        avg_hr = r["avg_hr"]
        print(f"  patient={pid}  window=[{start} -> {end}]  "
              f"avg_hr={avg_hr:5.1f} bpm   (above {HR_THRESHOLD:.0f})")
        prev_start = start - timedelta(minutes=2)
        seen = elevated_history.setdefault(pid, set())
        if prev_start in seen:
            print(f"  *** SUSTAINED CLINICAL ALERT ***  patient {pid}: average "
                  f"HR above {HR_THRESHOLD:.0f} bpm for TWO consecutive windows "
                  f"(ending {end}). Notify ICU staff.")
        seen.add(start)


def main():
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)
    os.makedirs(INPUT_DIR, exist_ok=True)

    spark = (SparkSession.builder
             .appName("icu-hr-monitor")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = (spark.readStream
           .schema(SCHEMA)
           .option("header", "false")
           .option("maxFilesPerTrigger", 1)
           .csv(INPUT_DIR))

    parsed = raw.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

    windowed = (parsed
                .withWatermark("event_time", WATERMARK_DELAY)
                .groupBy(window(col("event_time"), WINDOW_DURATION), col("patient_id"))
                .agg(avg("heart_rate").alias("avg_hr")))

    elevated = (windowed
                .filter(col("avg_hr") > HR_THRESHOLD)
                .select(
                    col("window.start").alias("win_start"),
                    col("window.end").alias("win_end"),
                    col("patient_id"),
                    col("avg_hr")))

    query = (elevated.writeStream
             .outputMode("append")
             .foreachBatch(handle_batch)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime="2 seconds")
             .start())

    print("ICU monitor running. Waiting for windows to close...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
