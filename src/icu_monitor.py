#!/usr/bin/env python3
"""
ICU heart-rate monitor - Spark Structured Streaming (Scenario B).

Two outputs from one streaming pipeline:
  * AUDIT  : every finalized (patient, window) average is appended to CSV, giving
             a complete record of all patients - not only the alarms.
  * ALERTS : windows above 100 bpm are filtered into an alert stream and escalated
             to a SUSTAINED clinical alert when a patient is elevated in two
             consecutive windows.

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
AUDIT_CHECKPOINT_DIR = "checkpoint_audit"
AUDIT_OUTPUT_DIR = "output/window_averages"
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
    for d in (CHECKPOINT_DIR, AUDIT_CHECKPOINT_DIR, "output"):
        if os.path.exists(d):
            shutil.rmtree(d)
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

    all_windows = windowed.select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        col("patient_id"),
        col("avg_hr"))

    elevated = all_windows.filter(col("avg_hr") > HR_THRESHOLD)

    # AUDIT sink: every window average, for every patient, appended to CSV.
    audit_query = (all_windows.writeStream
                   .outputMode("append")
                   .format("csv")
                   .option("path", AUDIT_OUTPUT_DIR)
                   .option("header", "true")
                   .option("checkpointLocation", AUDIT_CHECKPOINT_DIR)
                   .trigger(processingTime="2 seconds")
                   .start())

    # ALERT sink: filtered elevated stream + two-consecutive-window escalation.
    alert_query = (elevated.writeStream
                   .outputMode("append")
                   .foreachBatch(handle_batch)
                   .option("checkpointLocation", CHECKPOINT_DIR)
                   .trigger(processingTime="2 seconds")
                   .start())

    print("ICU monitor running. Waiting for windows to close...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
