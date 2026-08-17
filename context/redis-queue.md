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

Without a semaphore `parent`, the server keeps one blocking Redis fetch in
flight and runs each fetched job as a child of the dispatcher. Passing an
`Async::Task` as `parent` preserves this behavior and places the dispatcher
under that task.

Pass `Async::Semaphore` as `parent` to set an explicit bound. The dispatcher
reserves a semaphore slot before the blocking fetch and holds it through
processing, so blocked fetches and executing jobs share the same limit.

A failed blocking fetch terminates the dispatcher instead of retrying in
process, so dequeuing cannot recover independently of the processing heartbeat.
Stopping the server cancels in-flight workers and releases their semaphore
slots.

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

## Known limitations and future work

Delayed promotion currently moves every due job in one Lua invocation. A large
backlog after an outage can therefore block Redis or exceed Lua argument
limits. Future work should promote jobs in bounded, atomic batches so backlog
size does not make one promotion disproportionately expensive.
