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

## Limitations and design choices

- **The dataset is a snapshot, not a time series.** The IoMT dataset records one
  row per patient, with no native timestamp or per-patient sequence of readings.
  A real ICU feed would stream many readings per patient over time. The generator
  bridges this by replaying the real heart-rate values as a timestamped,
  per-patient stream using a compressed event-time clock. This faithfully
  simulates the streaming mechanics; the temporal pattern is generated, not measured.
- **The anomaly is injected deliberately.** Two patients are driven above
  threshold for two consecutive windows so the alert path is exercised and
  reproducible. The same code fires on genuine elevations in production.
- **Compressed event time.** Event time advances 30 simulated seconds per batch
  while the generator sleeps ~1.5 real seconds, so a 2-minute window closes within
  seconds. The windowing is unaffected because Spark windows on event time.
- **Driver-side consecutive-window state.** The two-consecutive-window check uses
  an in-memory dictionary on the driver: simple and correct for this scope, but
  not checkpointed. A production version would keep this entirely inside Spark
  (e.g. a stream-stream self-join of consecutive windows), so it survives restarts.

## On keeping the consecutive-window state inside Spark

A natural question is whether the two-consecutive-window check could live entirely
inside Spark's managed state instead of the driver-side dictionary. This was
investigated, and the result is itself instructive about Structured Streaming's
design:

- `applyInPandasWithState` (arbitrary stateful processing) cannot be chained in
  append mode directly after a windowed aggregation. Spark rejects the plan with:
  "applyInPandasWithState in append mode is not supported after aggregation".
- A stream-stream self-join of consecutive windows is nominally supported through
  watermark propagation, but chaining it after the windowed aggregation did not
  reliably emit in testing - a known sharp edge of multiple stateful operators.

Because Spark restricts chaining a second stateful operator after a windowed
aggregation, the idiomatic and officially recommended pattern for cross-window
logic that the engine cannot express natively is `foreachBatch`, which is exactly
what this pipeline uses. The windowed average remains fully Spark-managed and
checkpointed; only the lightweight comparison of the current window against the
previous one is held on the driver.

For production fault tolerance, that comparison could be moved into Spark's state
store by decoupling into two queries: query one writes finalized elevated windows
to a sink, and query two consumes them as a fresh stream and applies
`applyInPandasWithState` - which is permitted because its input is no longer an
aggregation. This is noted as the natural next step.
