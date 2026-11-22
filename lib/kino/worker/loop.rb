# frozen_string_literal: true

module Kino
  class Worker
    # What a `worker_loop` gets on its worker thread: Kino's loop, for the app's scheduler (#serve).
    class Loop
      # Seconds between lane ticks: the native ticker rings a doorbell wait, and a job wait ends, as often.
      TICK = Native::TICK_SECONDS
      # The most socket events and wakes taken and not yet done: with this many the loop takes requests
      # only, and the native inbox, filling, pushes back on the sockets.
      BACKLOG = 64
      JOB_LOST = "a worker_loop job ended before its socket events or wakes were done"
      LINGER = 1.0
      LINGERING = "the worker_loop is still running #{LINGER} s after its serve returned; most likely " \
        "a task that is not transient keeps its scheduler, and so the worker, from finishing".freeze
      private_constant :TICK, :BACKLOG, :JOB_LOST, :LINGER, :LINGERING

      # @private
      # `serving` is the Kino::Worker whose requests and events the jobs run, `worker` its native handle.
      def initialize(serving, worker)
        @serving = serving
        @worker = worker
        # The descriptor is the native worker's; this IO must never close it.
        @ready = IO.for_fd(worker.ready_fd, autoclose: false)
        # 1 per request job that frees its slot; 0 when the backlog drained, a job ended while the loop
        # drains, or a socket's (or the wakes') job ended before its events were done.
        @done = Thread::Queue.new
        # Each request job's request: true while it holds a slot, false once its connection switched.
        @requests = {}.compare_by_identity
        # Each socket's events not yet done, in order, the first running; the wakes under WAKE.
        @pending = {}
        @backlog = 0
        @waiting = @backlogged = @draining = @lost = @drained = false
      end

      # @private
      def drained? = @drained

      # @private
      # Warns once per drain about each loop still running LINGER after its serve returned.
      def self.warn_lingering(server_id)
        Native.lingering_loops(server_id, LINGER).each { |label| Native.log_line("warn", label, LINGERING) }
      end

      # @private
      # `request`'s connection switched (rack.hijack, or a 101 with a callable): its job frees its slot.
      def self.switched(request) = current&.switched(request)

      # @private
      # A socket's on_open, on its request's job: the socket's later events wait, then get a job.
      def self.opening(id, &on_open)
        custom = current
        custom ? custom.opening(id, &on_open) : yield
      end

      # The loop serving on this thread, if a worker_loop runs it.
      def self.current = Thread.current.thread_variable_get(CUSTOM_LOOP)
      private_class_method :current

      # @private
      def opening(id)
        pending = @pending[id] ||= []
        yield
      ensure
        pending.empty? ? @pending.delete(id) : start_pending(id, pending)
      end

      # @private
      def switched(request)
        return unless @requests[request]

        @requests[request] = false
        free_slot
      end

      # Serve this worker's requests, at most `concurrency` at a time, each as a job the block starts
      # on the thread's fiber scheduler (`task.async { job.call }` under Async) and runs until it first
      # waits, lest dispatch read the worker as idle. Each WebSocket's events, and the wakes, run in
      # order on jobs of their own. Returns once the worker is drained and every job has finished.
      # @param concurrency [Integer] the most requests running at once; one whose connection
      #   switched counts no more
      # @yieldparam job [#call] runs one request, a socket's events, or the wakes
      # @return [nil]
      # @raise [ArgumentError] without a fiber scheduler on this thread
      # @raise [Kino::Error] when a socket's job, or the wakes', ends before its events were done
      def serve(concurrency:, &spawn)
        raise ArgumentError, "worker.serve needs a fiber scheduler on this thread" unless Fiber.current_scheduler
        raise ArgumentError, "worker.serve needs a block that starts each job" unless spawn
        concurrency = Integer(concurrency)
        raise ArgumentError, "concurrency must be positive (got #{concurrency})" unless concurrency.positive?

        @worker.capacity = concurrency
        @spawn = spawn
        running = 0
        loop do
          running -= @done.pop until @done.empty?
          raise Error, JOB_LOST if @lost
          room = running < concurrency
          @backlogged = @backlog >= BACKLOG
          if !room && @backlogged
            # Neither fits: wait for a job or the backlog, back every TICK so the worker never reads stuck.
            freed = @done.pop(timeout: TICK)
            freed ? running -= freed : @worker.woke
            next
          end
          case (item = take(room))
          when Hash
            running += 1
            # Kept from here on: the app may replace the env's entry.
            request = item[KINO_REQUEST]
            @requests[request] = true
            spawn.call(-> { run_request(item, request) })
          when Array, WAKE then queue(item)
          when nil then break
          end
        end
        @draining = true
        @done.pop until @requests.empty? && @pending.empty?
        raise Error, JOB_LOST if @lost
        @drained = true
        @worker.drained(Log.source)
        nil
      end

      private

      def take(room)
        only = room ? (:requests if @backlogged) : :events
        item = only ? @worker.take_nonblock(only) : @worker.take_nonblock
        return item unless item == false

        only ? busy(only) : idle
      end

      # No timeout, so no scheduler timer: the ticker rings a waiting doorbell every TICK (a lane tick).
      def idle
        @ready.wait_readable
        @worker.take_woken
      end

      def busy(only)
        @waiting = true
        @ready.wait_readable if @done.empty?
        return @worker.take_woken(only) if @done.empty?

        @worker.woke
        false
      ensure
        @waiting = false
      end

      def run_request(env, request)
        @serving.serve(env) do |_request, status, headers, chunks|
          request.send_simple(status, headers, chunks)
        end
      rescue Exception # rubocop:disable Lint/RescueException -- the job's failure is the app's scheduler's to see
        # Kino's own rescue failed: the client must not wait for the handle to be collected.
        request.abort
        raise
      ensure
        request.close_waits
        if @requests.delete(request)
          free_slot
        elsif @draining
          @done << 0
        end
      end

      def free_slot
        @done << 1
        @worker.ring if @waiting
      end

      # An event waits on its socket's job, or starts one; a wake folds into one waiting to run.
      def queue(item)
        key = item.equal?(WAKE) ? WAKE : item.first
        pending = @pending[key]
        return if pending && key.equal?(WAKE) && pending.size > 1

        @backlog += 1
        return pending << item if pending

        start_pending(key, @pending[key] = [item])
      end

      # A scheduler that refuses the job (a closing one raises) loses its events: the loop hears so.
      def start_pending(key, pending)
        @spawn.call(-> { run_pending(key, pending) })
      rescue Exception # rubocop:disable Lint/RescueException -- the events are lost, whatever was raised
        @pending.delete(key)
        lose_events
        raise
      end

      def run_pending(key, pending)
        until pending.empty?
          run_event(pending.first)
          pending.shift
          @backlog -= 1
          relieve if @backlogged && @backlog < BACKLOG
        end
      ensure
        @pending.delete(key)
        if pending.empty?
          @done << 0 if @draining
        else
          # The app's scheduler stopped it.
          lose_events
        end
      end

      # Events will never run: serve raises once it hears.
      def lose_events
        @lost = true
        @done << 0
        @worker.ring
      end

      def relieve
        @backlogged = false
        @done << 0
        @worker.ring if @waiting
      end

      # A failed report must not end the job either; its socket is dropped all the same, as a crash.
      def run_event(item)
        @serving.dispatch(item)
      rescue => e
        Native.ws_fail(item.first) if item.is_a?(Array)
        begin
          Log.failure("worker_loop event", e)
        rescue
          Log.error("worker_loop event · #{e.class} (its report failed too)")
        end
      end
    end
  end
end
