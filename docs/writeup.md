# Written Explanation — Scenario B: Hospital Patient Monitoring

## Why a tumbling window?

The clinical goal is to detect a *sustained* abnormal heart rate, not a single
transient spike. A tumbling window (fixed size, non-overlapping) is the natural
fit because it partitions time into discrete, equal 2-minute buckets, and each
reading belongs to exactly one bucket. This gives a clean, unambiguous "average
heart rate for this patient during this 2-minute period" with no double-counting.

It also makes the "two consecutive windows" rule simple to define: because the
windows are non-overlapping and back-to-back, the window immediately following
[08:02, 08:04) is exactly [08:04, 08:06). I can therefore detect a sustained
elevation by checking whether the same patient was above threshold in a window
and in the very next one.

A sliding window would create overlapping buckets, so a single elevated reading
could appear in several windows and inflate the apparent duration of the event —
the opposite of what we want when distinguishing a sustained condition from a
brief spike. A session window keys off gaps in activity, which is not meaningful
for continuously-sampled vital signs. Tumbling is the correct choice.

## Where the pipeline requires state

Structured Streaming is stateful in two distinct places here:

1. **Spark-managed aggregation state.** The windowed aggregation
   `groupBy(window(event_time, "2 minutes"), patient_id).agg(avg(heart_rate))`
   must remember a running sum and count for every open (patient, window) pair so
   it can compute the average incrementally as new readings arrive across
   micro-batches. `withWatermark("event_time", "2 minutes")` bounds this state:
   once event time has advanced 2 minutes past a window's end, Spark treats that
   window as final, emits it (append mode), and discards its state. Without the
   watermark, this state would grow without bound.

2. **Application-level cross-window state.** Detecting "two consecutive elevated
   windows" requires remembering, per patient, which earlier windows were
   elevated. The `elevated_history` dictionary in the `foreachBatch` sink holds,
   for each patient, the set of window-start times whose average exceeded 100 bpm.
   When a newly finalized elevated window arrives, the sink checks whether the
   immediately preceding window (start minus 2 minutes) is already in that set; if
   so, it raises the sustained clinical alert.

## Note on the data

The IoMT dataset (Kaggle) provides realistic per-patient heart-rate values but is
a single snapshot per patient rather than a per-patient time series. To simulate a
live ICU telemetry feed, the stream generator replays these real heart-rate values
into a watched directory, assigning patient IDs and a compressed event-time clock.
Two patients are deliberately driven above threshold for two consecutive windows so
the sustained-alert path is exercised and observable.
