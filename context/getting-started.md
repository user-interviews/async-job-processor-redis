# Getting Started

This guide gives you an overview of the `async-job-processor-redis` gem.

## Installation

Add the gem to your project:

``` shell
$ bundle add async-job-processor-redis
```

## Usage

Here is a full example of the job queue:

``` ruby
require "async"
require "async/job"
require "async/job/processor/redis"

Async do
	buffer = Async::Job::Buffer.new
	
	queue = Async::Job::Builder.build(buffer) do
		dequeue Async::Job::Processor::Redis
	end
	
	# Run the server:
	server = Async{queue.start}
	
	# Enqueue a job:
	queue.call({message: "Hello, World!"})
	
	# Wait for the job to complete:
	job = buffer.pop
	pp job: job
	
	server.stop
end
```

## Bounded processing and instrumentation

Pass `Async::Semaphore` as the parent to bound blocking Redis fetches and job
processing together:

``` ruby
require "async/semaphore"

promoter_events = proc do |event, **|
	warn "Delayed promoter: #{event}"
end

queue = Async::Job::Builder.build(buffer) do
	dequeue Async::Job::Processor::Redis,
		parent: Async::Semaphore.new(20),
		delayed_jobs_instrumentation: promoter_events
end
```

The callback receives `:failure` with the error, consecutive failure count, and
retry delay. After a successful promotion it receives `:recovered` with the
previous failure count. Callback failures are isolated from the promoter.

Omit `parent`, or pass an `Async::Task`, to retain the compatibility behavior:
one blocking fetch stays in flight while fetched jobs run as children of the
dispatcher. A Redis fetch failure stops that dispatcher so it cannot recover
independently of the processing heartbeat.
