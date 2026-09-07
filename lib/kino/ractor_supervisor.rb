# frozen_string_literal: true

module Kino
  # @private
  # Spawns worker ractors and keeps them alive. One supervisor thread per
  # ractor: it blocks in Ractor#value, and a crash (anything that kills the
  # ractor, Exception from app code included) wakes it to 500 the in-flight
  # requests and respawn. Clean exits (queue drained at shutdown, or the
  # worker retired by the pool scaler) end supervision.
  #
  # Also the :ractor-mode pool behind PoolScaler: `grow` adds a worker,
  # `retire` sends one home, `groups` lists the ones that may be retired,
  # and `active_count` is what the control plane reports.
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
      # Worker index => true while its ractor runs; => true once the
      # scaler asked it to leave; and the slot ids retired workers gave
      # back, for the next worker to take over.
      @live = {}
      @retiring = {}
      @free_slots = []
      # The first replacement's index; `replace` increments before using it,
      # so this starts one below the first free index (@workers).
      @next_worker_index = @workers - 1
    end

    def start
      @supervisor_threads = Array.new(@workers) { |index| supervise(index) }
      report_active
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

    # Workers alive and serving: the live ones minus those the quarantine
    # monitor abandoned as wedged (their replacements count instead).
    def active_count
      @lock.synchronize { @live.count { |index, _| !@replaced.key?(index) } }
    end

    # Worker index => slot ids for every worker the scaler may retire:
    # live, not already leaving, not quarantined, and past its spawn (a
    # worker marked live whose supervisor thread has not assigned slots
    # yet is not listed until it has).
    def groups
      @lock.synchronize do
        @live.keys
          .reject { |index| @retiring.key?(index) || @replaced.key?(index) || !@worker_slots.key?(index) }
          .to_h { |index| [index, @worker_slots[index].dup] }
      end
    end

    # Add one supervised worker; returns its index.
    def grow
      new_index = @lock.synchronize { @next_worker_index += 1 }
      thread = supervise(new_index)
      @lock.synchronize { @supervisor_threads << thread }
      report_active
      new_index
    end

    # Send a worker home. Its slots stop receiving work now; the worker
    # finishes what it holds, leaves at its next idle tick, and its slots
    # come back to the free list once the ractor has exited. Returns
    # false when there is no such live worker to retire.
    def retire(worker_index)
      slot_ids = @lock.synchronize do
        next nil unless @live.key?(worker_index) && !@retiring.key?(worker_index)

        @retiring[worker_index] = true
        @worker_slots[worker_index]
      end
      return false unless slot_ids

      slot_ids.each { |id| Native.retire_slot(@server_id, id) }
      true
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
      # Live from the moment it is asked for, not from when its thread gets
      # around to spawning: `grow` reports the count right after this.
      @lock.synchronize { @live[index] = true }
      Thread.new do
        Thread.current.name = "supervisor-#{index}"
        crashes = 0
        loop do
          ractor, worker_ids = spawn_worker(index)
          begin
            ractor.value # blocks until the ractor terminates
            HookFire.fire(@on_worker_exit, "on_worker_exit", index, nil) # clean exit: drained or retired
            exited(index, worker_ids)
            break
          rescue Ractor::Error => e
            # The ractor died mid-flight. Anything it was serving will never
            # be answered by Ruby: 500 those clients NOW (not when GC gets
            # around to dropping the dead heap), then decide on respawn.
            worker_ids.each { |id| Native.abort_inflight(@server_id, id) }
            cause = (e.respond_to?(:cause) && e.cause) ? e.cause : e
            HookFire.fire(@on_worker_exit, "on_worker_exit", index, cause)
            if draining?
              exited(index, nil)
              break
            end

            crashes += 1
            Native.record_respawn(@server_id)
            Log.error("worker-#{index} crashed (#{cause.class}: #{cause.message}); respawning")
            # A crashed worker that was on its way out respawns on fresh
            # slots like any other; the scaler retires it again when idle.
            @lock.synchronize { @retiring.delete(index) }
            # Policy (crash recovery): unlimited respawn
            # keeps the server up under rare crashes but turns a
            # crash-on-every-request bug into a busy loop. A circuit breaker
            # (give up / cool down after N crashes in T seconds) trades
            # availability for fail-fast. Current policy: respawn forever.
          end
        end
      end
    end

    # Fresh ractor on fresh or recycled slots. Slots are never reused
    # across crash respawns: stale interrupt kicks and dead weak refs go
    # down with the old slot. They are reused after a clean retirement.
    def spawn_worker(worker_index)
      worker_ids = Array.new(@threads) { claim_slot }
      @lock.synchronize do
        @worker_slots[worker_index] = worker_ids
        worker_ids.each { |id| @slot_to_worker[id] = worker_index }
      end
      # Named so log lines from inside say which worker spoke: the ractor
      # alone for a single thread, `worker-N/thread-M` for more.
      ractor = Ractor.new(@server_id, worker_ids, @app, @batch, @hooks,
        name: "worker-#{worker_index}") do |server_id, ids, app, batch, hooks|
        ids.each_with_index.map do |id, position|
          Thread.new do
            # Crashes surface via Ractor#value in the supervisor; don't also
            # spray the backtrace to stderr from inside the dying ractor.
            Thread.current.report_on_exception = false
            Thread.current.name = "thread-#{position + 1}" if ids.size > 1
            Kino::Worker.run(server_id, id, app, batch, hooks)
          end
        end.each(&:join)
      end
      [ractor, worker_ids]
    end

    # A slot a retired worker gave back, reset for its new occupant, or a
    # fresh one.
    def claim_slot
      id = @lock.synchronize { @free_slots.pop }
      return Native.register_worker(@server_id) unless id

      Native.reset_slot(@server_id, id)
      id
    end

    # Bookkeeping for a supervisor thread that is done: the worker is no
    # longer live, a retired worker's slots go back to the free list, and
    # the thread leaves the join set so a long-lived elastic pool does not
    # accumulate dead threads.
    def exited(index, worker_ids)
      @lock.synchronize do
        @live.delete(index)
        @free_slots.concat(worker_ids) if @retiring.delete(index) && worker_ids
        @supervisor_threads.delete(Thread.current)
      end
      report_active
    end

    def report_active
      Native.set_active_workers(@server_id, active_count)
    end

    def draining?
      @lock.synchronize { @draining }
    end
  end
end
