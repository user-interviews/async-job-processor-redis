# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2024-2025, by Samuel Williams.

require "async/idler"
require "async/job/coder"
require "async/job/processor/generic"
require "async/semaphore"

require "securerandom"

require_relative "delayed_jobs"
require_relative "job_store"
require_relative "processing_list"
require_relative "ready_list"

module Async
	module Job
		module Processor
			module Redis
				# Redis-backed job processor server.
				# Manages job queues using Redis for distributed job processing across multiple workers.
				# Handles immediate jobs, delayed jobs, and job retry/recovery mechanisms.
				class Server < Generic
					INITIAL_DEQUEUE_RETRY_DELAY = 0.25
					MAXIMUM_DEQUEUE_RETRY_DELAY = 5
					
					# Initialize a new Redis job processor server.
					# @parameter delegate [Object] The delegate object that will process jobs.
					# @parameter client [Async::Redis::Client] The Redis client instance.
					# @parameter prefix [String] The Redis key prefix for job data.
					# @parameter coder [Async::Job::Coder] The job serialization codec.
					# @parameter resolution [Integer] The resolution in seconds for delayed job processing.
					# @parameter parent [Async::Task | Async::Semaphore | Nil] An optional lifecycle parent or concurrency limiter for job processing.
					def initialize(delegate, client, prefix: "async-job", coder: Coder::DEFAULT, resolution: 10, parent: nil)
						super(delegate)
						
						@id = SecureRandom.uuid
						@client = client
						@prefix = prefix
						@coder = coder
						@resolution = resolution
						
						@job_store = JobStore.new(@client, "#{@prefix}:jobs")
						@delayed_jobs = DelayedJobs.new(@client, "#{@prefix}:delayed")
						@ready_list = ReadyList.new(@client, "#{@prefix}:ready")
						@processing_list = ProcessingList.new(@client, "#{@prefix}:processing", @id, @ready_list, @job_store)
						
						# Ordinary task parents preserve the original single blocking fetch loop.
						@parent = parent || Async::Idler.new
						# A semaphore limits both blocking fetches and executing jobs, while the
						# dispatcher task remains responsible for worker lifecycle.
						@semaphore = parent if parent.is_a?(Async::Semaphore)
					end
					
					# Start the job processing loop immediately.
					# @returns [Async::Task | false] The processing task or false if already started.
					def start!
						return false if @task
						
						@task = true
						
						if @semaphore
							# Reserve a caller-supplied concurrency slot before BRPOPLPUSH. Counting
							# blocked fetches prevents the dispatcher from opening unbounded Redis IO.
							Async do |task|
								@task = task
								
								while true
									@semaphore.async(parent: task, annotation: self.class.name) do
										self.dequeue
									end
								end
							ensure
								@task = nil
							end
						else
							@parent.async(transient: true, annotation: self.class.name) do |task|
								@task = task
								
								while true
									self.dequeue(task)
								end
							ensure
								@task = nil
							end
						end
					end
					
					# Start the server and all background processing tasks.
					# Initializes delayed job processing, abandoned job recovery, and the main processing loop.
					def start
						super
						
						# Start the delayed processor, which will move jobs to the ready processor when they are ready:
						@delayed_jobs.start(@ready_list, resolution: @resolution)
						
						# Start the processing processor, which will move jobs to the ready processor when they are abandoned:
						@processing_list.start
						
						self.start!
					end
					
					# Stop the server and all background processing tasks.
					def stop
						@task&.stop
						
						super
					end
					
					# Generates a human-readable string representing the current statistics.
					#
					# e.g. `R=3.42K D=1.23K P=7/2.34K``
					#
					# This can be interpreted as:
					#
					# - R: Number of jobs in the ready list
					# - D: Number of jobs in the delayed queue
					# - P: Number of jobs currently being processed / total number of completed jobs.
					#
					# @returns [String] A string representing the current statistics.
					def status_string
						"R=#{format_count(@ready_list.size)} D=#{format_count(@delayed_jobs.size)} P=#{format_count(@processing_list.size)}/#{format_count(@processing_list.complete_count)}"
					end
					
					# Submit a new job for processing.
					# Jobs with a scheduled_at time are queued for delayed processing, while immediate jobs are added to the ready queue.
					# @parameter job [Hash] The job data to process.
					def call(job)
						scheduled_at = Coder::Time(job["scheduled_at"])
						
						if scheduled_at
							@delayed_jobs.add(@coder.dump(job), scheduled_at, @job_store)
						else
							@ready_list.add(@coder.dump(job), @job_store)
						end
					end
					
					protected
					
					# Dequeue a job from the ready list and process it.
					#
					# If the job fails for any reason, it will be retried.
					#
					# If you do not desire this behavior, you should catch exceptions in the delegate.
					def dequeue(parent = nil)
						_id = fetch_with_backoff
						
						id = _id
						if parent
							parent.async{process(id)}
						else
							process(id)
						end
						_id = nil
					ensure
						@processing_list.retry(_id) if _id
					end
					
					def process(id)
						job = @coder.load(@job_store.get(id))
						@delegate.call(job)
						@processing_list.complete(id)
					rescue => error
						Console.error(self, "Job failed with error!", id: id, exception: error)
						@processing_list.retry(id)
					end
					
					private
					
					# Fetch a job while keeping the current dispatcher or semaphore slot reserved.
					def fetch_with_backoff
						consecutive_failures = 0
						
						loop do
							begin
								return @processing_list.fetch
							rescue Async::Cancel
								raise
							rescue => error
								consecutive_failures += 1
								retry_in_seconds = dequeue_retry_delay(consecutive_failures)
								
								Console.warn(
									self,
									"Job dequeue failed; retrying in #{retry_in_seconds} seconds.",
									error,
									consecutive_failures:,
									retry_in_seconds:,
								)
								
								sleep(retry_in_seconds)
							end
						end
					end
					
					# Compute the bounded exponential delay for a failed dequeue attempt.
					def dequeue_retry_delay(consecutive_failures)
						[INITIAL_DEQUEUE_RETRY_DELAY * (2 ** (consecutive_failures - 1)), MAXIMUM_DEQUEUE_RETRY_DELAY].min
					end
					
					def format_count(value)
						if value > 1_000_000
							"#{(value/1_000_000.0).round(2)}M"
						elsif value > 1_000
							"#{(value/1_000.0).round(2)}K"
						else
							value
						end
					end
				end
			end
		end
	end
end
