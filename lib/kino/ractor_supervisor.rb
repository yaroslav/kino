# frozen_string_literal: true

module Kino
  # @private
  # Spawns worker ractors and keeps them alive. One supervisor thread per
  # ractor: it blocks in Ractor#value, and a crash (anything that kills the
  # ractor, Exception from app code included) wakes it to 500 the in-flight
  # requests and respawn. Clean exits (queue drained) end supervision.
  class RactorSupervisor
    def initialize(server_id, app, workers:, threads:, batch: 1, hooks: nil, on_worker_exit: nil)
      @server_id = server_id
      @app = app
      @workers = workers
      @threads = threads
      @batch = batch
      @hooks = hooks
      @on_worker_exit = on_worker_exit
      @draining = false
      @lock = Mutex.new
      @supervisor_threads = []
      @worker_slots = {}
      @slot_to_worker = {}
      @replaced = {}
      # The first replacement's index; `replace` increments before using it,
      # so this starts one below the first free index (@workers).
      @next_worker_index = @workers - 1
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
      @lock.synchronize { @supervisor_threads.dup }.each do |thread|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        thread.join([remaining, 0.01].max)
      end
    end

    def done?
      @lock.synchronize { @supervisor_threads.dup }.none?(&:alive?)
    end

    # Block until the workers exit on their own (drain elsewhere): join
    # without flipping the draining flag.
    def join
      @lock.synchronize { @supervisor_threads.dup }.each(&:join)
    end

    # Replace the ractor owning slot `worker_id`: spawn a fresh supervised
    # ractor, then quarantine the old ractor's slots. The old supervisor
    # thread stays blocked in ractor.value on the wedged ractor (it and the
    # ractor leak until process exit; a wedged ractor cannot be
    # force-killed). Returns true if a replacement was spawned.
    def replace(worker_id)
      worker_index = @lock.synchronize { @slot_to_worker[worker_id] }
      return false unless worker_index

      # Idempotent per ractor: a stale monitor snapshot can list two sibling
      # slots of the same ractor, only the first replaces it.
      claimed = @lock.synchronize do
        if @replaced.key?(worker_index)
          false
        else
          @replaced[worker_index] = true
          true
        end
      end
      return false unless claimed

      new_index = @lock.synchronize { @next_worker_index += 1 }
      thread =
        begin
          supervise(new_index) # spawn FIRST, nothing quarantined yet
        rescue
          @lock.synchronize { @replaced.delete(worker_index) } # allow retry next tick
          raise
        end
      slot_ids = @lock.synchronize { @worker_slots[worker_index] } || []
      slot_ids.each { |id| Native.quarantine_slot(@server_id, id) } # quarantine only after success
      @lock.synchronize { @supervisor_threads << thread }
      true
    end

    private

    def supervise(index)
      Thread.new do
        crashes = 0
        loop do
          ractor, worker_ids = spawn_worker(index)
          begin
            ractor.value # blocks until the ractor terminates
            HookFire.fire(@on_worker_exit, "on_worker_exit", index, nil) # clean exit: queue drained
            break        # clean exit: queue closed, workers drained
          rescue Ractor::Error => e
            # The ractor died mid-flight. Anything it was serving will never
            # be answered by Ruby: 500 those clients NOW (not when GC gets
            # around to dropping the dead heap), then decide on respawn.
            worker_ids.each { |id| Native.abort_inflight(@server_id, id) }
            cause = (e.respond_to?(:cause) && e.cause) ? e.cause : e
            HookFire.fire(@on_worker_exit, "on_worker_exit", index, cause)
            break if draining?

            crashes += 1
            Native.record_respawn(@server_id)
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
    def spawn_worker(worker_index)
      worker_ids = Array.new(@threads) { Native.register_worker(@server_id) }
      @lock.synchronize do
        @worker_slots[worker_index] = worker_ids
        worker_ids.each { |id| @slot_to_worker[id] = worker_index }
      end
      ractor = Ractor.new(@server_id, worker_ids, @app, @batch, @hooks) do |server_id, ids, app, batch, hooks|
        ids.map do |id|
          Thread.new do
            # Crashes surface via Ractor#value in the supervisor; don't also
            # spray the backtrace to stderr from inside the dying ractor.
            Thread.current.report_on_exception = false
            Kino::Worker.run(server_id, id, app, batch, hooks)
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
