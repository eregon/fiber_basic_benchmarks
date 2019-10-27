#!/usr/bin/env ruby

require 'socket'
require 'fiber'
require 'json'

# TODO: make these much larger, see if we're effectively batching
# even if we don't mean to...
QUERY_TEXT = "STATUS".freeze
RESPONSE_TEXT = "OK".freeze

if ARGV.size != 3
  STDERR.puts "Usage: ./fiber_test <num workers> <number of requests/batch> <output filename>"
  exit 1
end

NUM_WORKERS = ARGV[0].to_i
NUM_REQUESTS = ARGV[1].to_i
OUTFILE = ARGV[2]

# Fiber reactor code taken from
# https://www.codeotaku.com/journal/2018-11/fibers-are-the-right-solution/index
class Reactor
  def initialize
    @readable = {}
    @writable = {}
  end

  def run
    until @readable.empty? and @writable.empty?
      readable, writable = IO.select(@readable.keys, @writable.keys, [])

      readable.each do |io|
        @readable[io].resume
      end

      writable.each do |io|
        @writable[io].resume
      end
    end
  end

  def wait_readable(io)
    raise "Nil io passed to wait_readable!" if io.nil?
    @readable[io] = Fiber.current
    Fiber.yield
    @readable.delete(io)
  end

  def wait_writable(io)
    raise "Nil io passed to wait_writable!" if io.nil?
    @writable[io] = Fiber.current

    Fiber.yield

    @writable.delete(io)
  end
end

worker_read = []
worker_write = []

master_read = []
master_write = []

writable_idx_for = {}
readable_idx_for = {}

workers = []

#puts "Setting up pipes..."
working_t0 = Time.now

NUM_WORKERS.times do |i|
  r, w = IO.pipe
  worker_read.push r
  master_write.push w
  writable_idx_for[w] = i

  r, w = IO.pipe
  worker_write.push w
  master_read.push r
  readable_idx_for[r] = i
end

reactor = Reactor.new

master_style = :process
master_pid = fork do
  if master_style == :process
    # process-like master
    pending_write_msgs = (1..NUM_WORKERS).map { NUM_REQUESTS }
    pending_read_msgs = pending_write_msgs.dup

    until master_read.empty? && master_write.empty?
      readable, writable = IO.select(master_read, master_write)

      # Receive responses
      readable.each do |io|
        idx = readable_idx_for[io]

        buf = io.read(RESPONSE_TEXT.size)
        if buf != RESPONSE_TEXT
          master_read.delete(io)
          raise.puts "Wrong response from worker! Got #{buf.inspect} instead of #{RESPONSE_TEXT.inspect}!"
        else
          pending_read_msgs[idx] -= 1
          if pending_read_msgs[idx] == 0
            # This changes the indexing of master_read, so it
            # must never be indexed by number. But we don't want
            # to keep seeing it as readable on every select call...
            master_read.delete(io)
          end
        end
      end

      # Send new messages
      writable.each do |io|
        idx = writable_idx_for[io]
        io.print QUERY_TEXT
        pending_write_msgs[idx] -= 1
        if pending_write_msgs[idx] == 0
          # This changes the indexing of master_write, so it
          # must never be indexed by number. But we don't want
          # to keep seeing it as writable on every select call...
          master_write.delete(io)
        end
      end
    end
  elsif master_style == :reactor
    # reactor-like master
    master_fiber = Fiber.new do
      master_subfibers = []
      NUM_WORKERS.times do |worker_num|
        # This fiber will handle a single batch
        f = Fiber.new do
          NUM_REQUESTS.times do |req_num|
            reactor.wait_writable(master_write[worker_num])
            master_write[worker_num].print QUERY_TEXT

            reactor.wait_readable(master_read[worker_num])
            buf = master_read[worker_num].read(RESPONSE_TEXT.size)

            if buf != RESPONSE_TEXT
              raise "Error! Fiber no. #{worker_num} on req #{req_num} expected #{RESPONSE_TEXT.inspect} but got #{buf.inspect}!"
            end
          end
        end
        master_subfibers.push f
        f.resume
      end
    end
    master_fiber.resume
    reactor.run
  end
end

#puts "Setting up fibers..."
NUM_WORKERS.times do |i|
  f = Fiber.new do
    # Worker code
    NUM_REQUESTS.times do |req_num|
      reactor.wait_readable(worker_read[i])
      q = worker_read[i].read(QUERY_TEXT.size)
      if q != QUERY_TEXT
        raise "Fail! Expected #{QUERY_TEXT.inspect} but got #{q.inspect} on request #{req_num.inspect}!"
      end

      reactor.wait_writable(worker_write[i])
      worker_write[i].print(RESPONSE_TEXT)
    end
  end
  workers.push f
end

workers.each { |f| f.resume }
# puts "Resumed all worker Fibers..."

#puts "Starting reactor..."
ok = false
begin
  reactor.run
  ok = true
ensure
  Process.kill :INT, master_pid unless ok
  Process.wait master_pid
end

working_time = Time.now - working_t0
#puts "Done, finished all reactor Fibers!"

out_data = {
  workers: NUM_WORKERS,
  requests_per_batch: NUM_REQUESTS,
  time: working_time,
  success: true,
}

File.open(OUTFILE, "w") { |f| f.write JSON.pretty_generate(out_data) }
