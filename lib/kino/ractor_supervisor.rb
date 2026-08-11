# frozen_string_literal: true

module Kino
  # @private
  # Spawns worker ractors and keeps them alive. One supervisor thread per
  # ractor: it blocks in Ractor#value, and a crash (anything that kills the
  # ractor, Exception from app code included) wakes it to 500 the in-flight
  # requests and respawn. Clean exits (queue drained) end supervision.
  class RactorSupervisor
    def initialize(server_id, app, workers:, threads:, batch: 1, on_error: nil)
      @server_id = server_id
      @app = app
      @workers = workers
      @threads = threads
      @batch = batch
      @on_error = on_error
      @draining = false
      @lock = Mutex.new
      @supervisor_threads = []
    end

    def start
      @supervisor_threads = Array.new(@workers) { |index| supervise(index) }
      self
    end

    # Flag the drain and join supervisors up to the (numeric) deadline;
    # callers wanting an unbounded wait use #join instead.
    def shutdown(timeout)
      @lock.synchronize { @draining = true }
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      @supervisor_threads.each do |thread|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        thread.join([remaining, 0.01].max)
      end
    end

    def done?
      @supervisor_threads.none?(&:alive?)
    end

    # Block until the workers exit on their own (drain elsewhere): join
    # without flipping the draining flag.
    def join
      @supervisor_threads.each(&:join)
    end

    private

    def supervise(index)
      Thread.new do
        crashes = 0
        loop do
          ractor, worker_ids = spawn_worker
          begin
            ractor.value # blocks until the ractor terminates
            break        # clean exit: queue closed, workers drained
          rescue Ractor::Error => e
            # The ractor died mid-flight. Anything it was serving will never
            # be answered by Ruby: 500 those clients NOW (not when GC gets
            # around to dropping the dead heap), then decide on respawn.
            worker_ids.each { |id| Native.abort_inflight(@server_id, id) }
            break if draining?

            crashes += 1
            Native.record_respawn(@server_id)
            cause = (e.respond_to?(:cause) && e.cause) ? e.cause : e
            Native.log_error("worker ractor #{index} crashed (#{cause.class}: #{cause.message}); respawning")
            # Policy (crash recovery): unlimited respawn
            # keeps the server up under rare crashes but turns a
            # crash-on-every-request bug into a busy loop. A circuit breaker
            # (give up / cool down after N crashes in T seconds) trades
            # availability for fail-fast. Current policy: respawn forever.
          end
        end
      end
    end

    # Fresh ractor + fresh native slots. Slots are never reused across
    # respawns: stale interrupt kicks and dead weak refs go down with the
    # old slot.
    def spawn_worker
      worker_ids = Array.new(@threads) { Native.register_worker(@server_id) }
      ractor = Ractor.new(@server_id, worker_ids, @app, @batch, @on_error) do |server_id, ids, app, batch, on_error|
        ids.map do |id|
          Thread.new do
            # Crashes surface via Ractor#value in the supervisor; don't also
            # spray the backtrace to stderr from inside the dying ractor.
            Thread.current.report_on_exception = false
            Kino::Worker.run(server_id, id, app, batch, on_error)
          end
        end.each(&:join)
      end
      [ractor, worker_ids]
    end

    def draining?
      @lock.synchronize { @draining }
    end
  end
end
