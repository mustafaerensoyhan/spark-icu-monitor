#!/usr/bin/env python3
"""
Late-data demonstration - Spark Structured Streaming watermark behaviour.

A watermark of 2 minutes means Spark will not wait for data older than
(latest event time seen - 2 minutes); anything older is "late" and is dropped
from the windowed state. This script makes that visible.

Run together with:  python src/stream_generator.py --late
The generator sends the normal stream and then injects out-of-order readings.
For every micro-batch this script prints which readings fall behind the current
watermark and would therefore be dropped - the exact rule Spark applies inside
the windowed aggregation.
"""

import os
import shutil
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

INPUT_DIR = "stream_input"
CHECKPOINT_DIR = "checkpoint_late"
WATERMARK_DELAY_MIN = 2          # must match the monitor's watermark

SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("heart_rate", DoubleType(), True),
])

# Driver-side memory of the latest event time seen, which the watermark tracks.
state = {"max_event_time": None}


def inspect_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    prior_max = state["max_event_time"]
    watermark = (prior_max - timedelta(minutes=WATERMARK_DELAY_MIN)) if prior_max else None

    late = [r for r in rows
            if watermark is not None and r["event_time"] is not None
            and r["event_time"] < watermark]

    if late:
        print(f"\n################ batch {batch_id}: LATE DATA DROPPED BY WATERMARK ################")
        print(f"  current watermark = {watermark}  "
              f"(latest seen {prior_max} minus {WATERMARK_DELAY_MIN} min)")
        for r in late:
            ws = r["event_time"].replace(second=0)
            ws = ws - timedelta(minutes=ws.minute % 2)
            print(f"  DROPPED: patient={r['patient_id']}  event_time={r['event_time']}  "
                  f"hr={r['heart_rate']:.0f} bpm  ->  its window starting {ws} "
                  f"already closed; this reading is ignored.")

    batch_max = max((r["event_time"] for r in rows if r["event_time"] is not None),
                    default=None)
    if batch_max is not None and (state["max_event_time"] is None
                                  or batch_max > state["max_event_time"]):
        state["max_event_time"] = batch_max


def main():
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)
    os.makedirs(INPUT_DIR, exist_ok=True)

    spark = (SparkSession.builder
             .appName("late-data-demo")
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

    query = (parsed.writeStream
             .foreachBatch(inspect_batch)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime="2 seconds")
             .start())

    print("Late-data demo running. Watching for out-of-order readings...")
    print(f"(watermark = latest event time - {WATERMARK_DELAY_MIN} minutes)\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
