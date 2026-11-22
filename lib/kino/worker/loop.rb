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
        # Wakes for the loop: a job freed its slot, the backlog drained, a job ended while the loop
        # drains, or a socket's (or the wakes') job ended before its events were done.
        @done = Thread::Queue.new
        # Each request job's request: true while it holds a slot, false once its connection switched.
        @requests = {}.compare_by_identity
        # Each socket's events not yet done, in order, the first running; the wakes under WAKE.
        @pending = {}
        # Each inline connection on a job, with the IO its job waits on.
        @conns = {}.compare_by_identity
        @running = 0
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
        free_slot(always: true)
      end

      # Serve this worker's requests, at most `concurrency` at a time, each as a job the block starts
      # on the thread's fiber scheduler (`task.async { job.call }` under Async) and runs until it first
      # waits, lest dispatch read the worker as idle. Each WebSocket's events, and the wakes, run in
      # order on jobs of their own. Returns once the worker is drained and every job has finished.
      # A connection served inline runs on a job of its own, each request counting toward
      # `concurrency`, and may step aside once after an answer (`yield`, else `kernel_sleep(0)`).
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
        @concurrency = concurrency
        loop do
          @done.clear
          raise Error, JOB_LOST if @lost
          room = room?
          @backlogged = @backlog >= BACKLOG
          case (item = take(room))
          when Hash
            @running += 1
            # Kept from here on: the app may replace the env's entry.
            request = item[KINO_REQUEST]
            @requests[request] = true
            spawn.call(-> { run_request(item, request) })
          when Native::Conn
            @conns[item] = nil
            spawn.call(-> { run_conn(item) })
          when Array, WAKE then queue(item)
          when nil then break
          end
        end
        @draining = true
        # Each connection's job looks once more, so a retired worker's go to hyper: closing the IO it
        # waits on wakes it (under a scheduler with fiber_interrupt) and leaves the descriptor open.
        @conns.each_value { it&.close }
        @done.pop until @requests.empty? && @pending.empty? && @conns.empty?
        raise Error, JOB_LOST if @lost
        @drained = true
        @worker.drained(Log.source)
        nil
      end

      private

      # Takes what there is room for (Native::Worker#take_nonblock), else waits; false: take again.
      def take(room)
        only = if room
          :requests if @backlogged
        else
          @backlogged ? :connections : :events
        end
        item = only ? @worker.take_nonblock(only) : @worker.take_nonblock
        return item unless item == false

        case only
        when nil then idle
        when :connections then full
        else busy(only)
        end
      end

      # Neither fits: wait for a job or the backlog, back every TICK so the worker never reads stuck.
      # A connection placed meanwhile is taken then.
      def full
        @worker.woke unless @done.pop(timeout: TICK)
        false
      end

      # No timeout, so no scheduler timer: the ticker rings a waiting doorbell every TICK (a lane tick).
      def idle
        @ready.wait_readable
        return @worker.take_woken if room?

        @worker.woke
        false
      end

      # A job that ended, the backlog drained or inline requests taking the room change what fits.
      def busy(only)
        @waiting = true
        @ready.wait_readable if @done.empty?
        return @worker.take_woken(only) if @done.empty? && (only == :events || room?)

        @worker.woke
        false
      ensure
        @waiting = false
      end

      def room? = @running < @concurrency

      # An inline connection's requests, one at a time while the worker has room; between them the
      # job waits on the socket, which only the native side reads, writes and closes.
      def run_conn(conn)
        io = watch(conn)
        loop do
          case (got = conn.next_request(room?))
          when Hash then run_inline(got)
          when :yield then yield_turn
          when :readable, :writable
            begin
              (got == :readable) ? io.wait_readable : io.wait_writable
            rescue IOError
              raise unless io.closed?
              io = watch(conn)
            end
          else break
          end
        end
      ensure
        conn.close
        @conns.delete(conn)
        @done << 0 if @draining
      end

      # Every fiber already ready runs before the job looks again, as async-http yields after each
      # response: the scheduler's own yield where it has one, else kernel_sleep(0).
      def yield_turn
        scheduler = Fiber.current_scheduler or return
        scheduler.respond_to?(:yield) ? scheduler.yield : scheduler.kernel_sleep(0)
      end

      # The IO the loop closes to wake the job; the descriptor is the connection's, never this IO's.
      def watch(conn)
        fd = conn.fd
        @conns[conn] = fd && IO.for_fd(fd, autoclose: false)
      end

      # No rescue: whatever unwinds the job, the connection's close (run_conn) answers with a 500.
      # Worker#serve straight from the job: a method between would cost a Ruby call per request.
      def run_inline(env)
        @running += 1
        request = env[KINO_REQUEST]
        @serving.serve(env) do |_request, status, headers, chunks|
          request.send_simple(status, headers, chunks)
        end
      ensure
        request&.close_waits
        free_slot(always: false)
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
          free_slot(always: true)
        elsif @draining
          @done << 0
        end
      end

      # The count drops at once, for a connection's job that resumes first. The loop hears of it from a
      # request job, or when the worker was full, which dispatch counts until its loop's next take.
      def free_slot(always:)
        was_full = !room?
        @running -= 1
        @done << 0 if always || was_full
        @worker.ring if @waiting || was_full
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
