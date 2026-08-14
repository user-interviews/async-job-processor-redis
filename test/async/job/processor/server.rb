# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2024-2025, by Samuel Williams.

require "async"
require "async/redis"
require "async/semaphore"

require "sus/fixtures/async/reactor_context"
require "sus/fixtures/console"

require "async/job/buffer"
require "async/job/processor/redis"

describe Async::Job::Processor::Redis do
	include Sus::Fixtures::Async::ReactorContext
	include Sus::Fixtures::Console::CapturedLogger
	
	let(:buffer) {Async::Job::Buffer.new}
	
	let(:prefix) {"async:job:#{SecureRandom.hex(8)}"}
	let(:server) {subject.new(buffer, prefix:, resolution: 1)}
	
	before do
		server.start
	end
	
	after do
		server.stop
	end
	
	let(:job) {{"data" => "test job"}}
	
	it "can schedule a job and have it processed immediately" do
		server.call(job)
		
		expect(buffer.pop).to be == job
	end
	
	with "delayed job" do
		it "can schedule a job and have it processed after a delay" do
			now = Time.now
			delayed_job = job.merge("scheduled_at" => now + 1)
			
			server.call(delayed_job)
			
			expect(buffer.pop).to have_keys(
				"data" => be == job["data"],
			)
		end
	end
	
	with "a failed job" do
		it "can retry a job" do
			server.call(job)
			failed = false
			
			mock(buffer) do |mock|
				mock.before(:call) do |job|
					# The first time the job is called, it will fail, and we record that:
					unless failed
						failed = true
						raise "test error"
					end
				end
			end
			
			# The job was retried:
			processed_job = buffer.pop
			expect(processed_job).to have_keys(
				"data" => be == job["data"],
			)
			
			expect(failed).to be == true
		end
	end
	
	with "#status_string" do
		it "returns a string with the current job counts" do
			expect(server.status_string).to be == "R=0 D=0 P=0/0"
			
			server.call(job)
			sleep 0.1 # Allow some time for the job to be processed.
			
			expect(server.status_string).to be == "R=0 D=0 P=0/1"
		end
	end
	
	with "concurrency limit" do
		let(:server) {subject.new(slow_delegate, prefix:, resolution: 1, parent:)}
		let(:parent) {nil}
		
		# A delegate that yields long enough to observe concurrent processing:
		let(:slow_delegate) do
			Class.new do
				attr :started
				attr :cancelled
				
				def start
				end
				
				def stop
				end
				
				def call(_job)
					@started = true
					sleep 5
				ensure
					@cancelled = true
				end
			end.new
		end
		
		it "preserves default concurrent processing without concurrent fetches" do
			4.times do |i|
				server.call({"data" => "job #{i}"})
			end
			
			Async::Task.current.with_timeout(2) do
				sleep(0.01) until server.status_string.match?(/P=4\//)
			end
			
			expect(server.status_string).to be =~ /P=4\//
		end
		
		with "Async::Task" do
			let(:parent) {Async::Task.current}
			let(:fetch_attempts) {[]}
			let(:server) do
				subject.new(slow_delegate, prefix:, resolution: 1, parent:).tap do |server|
					processing_list = server.instance_variable_get(:@processing_list)
					attempts = fetch_attempts
					
					processing_list.define_singleton_method(:fetch) do
						attempts << true
						sleep
					end
				end
			end
			
			it "keeps only one blocking fetch in flight" do
				sleep 0.01
				
				expect(fetch_attempts).to have_attributes(size: be == 1)
			end
		end
		
		with "Async::Semaphore" do
			let(:parent) {Async::Semaphore.new(2)}
			
			it "can limit concurrent job processing to 2" do
				4.times do |i|
					server.call({"data" => "job #{i}"})
				end
				
				Async::Task.current.with_timeout(2) do
					sleep(0.01) until server.status_string.match?(/P=2\//)
				end
				
				expect(server.status_string).to be =~ /P=2\//
			end
			
			it "cancels in-flight workers when stopped" do
				server.call(job)
				
				Async::Task.current.with_timeout(2) do
					sleep(0.01) until slow_delegate.started
				end
				
				expect(parent.count).to be > 0
				server.stop
				
				Async::Task.current.with_timeout(2) do
					sleep(0.01) until parent.count == 0
				end
				
				expect(slow_delegate.cancelled).to be == true
				expect(parent.count).to be == 0
			end
			
			with "a Redis outage" do
				let(:parent) {Async::Semaphore.new(1)}
				let(:fetch_attempts) {[]}
				let(:server) do
					subject.new(slow_delegate, prefix:, resolution: 1, parent:).tap do |server|
						processing_list = server.instance_variable_get(:@processing_list)
						attempts = fetch_attempts
						
						processing_list.define_singleton_method(:fetch) do
							attempts << Process.clock_gettime(Process::CLOCK_MONOTONIC)
							raise "Redis unavailable"
						end
					end
				end
				
				it "backs off without releasing the worker slot" do
					Async::Task.current.with_timeout(2) do
						sleep(0.01) until fetch_attempts.size >= 2
					end
					
					expect(fetch_attempts[1] - fetch_attempts[0]).to be >= 0.2
					expect(parent.count).to be == 1
					expect(server.send(:dequeue_retry_delay, 10)).to be == 5
					
					expect_console.to have_logged(
						severity: be == :warn,
						message: be(:include?, "Job dequeue failed"),
					)
				end
			end
		end
	end
end
