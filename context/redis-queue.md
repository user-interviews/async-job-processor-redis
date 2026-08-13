# Redis Queue

The processor retains each serialized job while its ID moves through the ready,
delayed, and processing queues. This provides at-least-once delivery: a job may
run again after worker loss, so delegates must tolerate replay.

## Queue lifecycle

### Ready queue

Immediately runnable job IDs enter a FIFO Redis list. A worker atomically moves
one ID from ready to its own processing list before loading and executing the
retained payload.

### Delayed queue

Scheduled job IDs enter a Redis sorted set ordered by execution time. A
background promoter periodically moves due IDs to the ready list. The payload
remains retained throughout the transition.

### Processing queue

Each worker has a processing list and heartbeat. Successful execution removes
the claim and payload. A delegate failure returns the claim to ready. If a
worker heartbeat expires, another processor returns its abandoned claims to
ready for replay.

## Processing concurrency

Without an explicit `parent`, the server keeps one blocking Redis fetch in
flight and schedules fetched jobs through `Async::Idler`. This preserves the
original behavior while preventing the idler from opening unbounded blocking
fetches.

Pass an asynchronous concurrency parent, such as `Async::Semaphore`, to set an
explicit bound. The dispatcher reserves a parent slot before the blocking fetch
and holds it through processing, so blocked fetches and executing jobs share the
same limit.

## Delayed-promotion recovery

A Redis or promotion error does not terminate the promoter. It retries with
exponential backoff starting at 0.25 seconds and capped at 5 seconds. A
successful move after failures reports recovery and resumes the configured
polling interval. `Async::Cancel` exits immediately as normal lifecycle control.

The server logs failed attempts and recovery through `Console`. Applications
can also pass `delayed_jobs_instrumentation`, an object responding to
`call(event, **details)`:

- `:failure` includes `error`, `consecutive_failures`, and `retry_in_seconds`.
- `:recovered` includes the previous `consecutive_failures` count.

Logging and instrumentation failures are isolated so observability cannot stop
scheduled jobs from being promoted.
