# frozen_string_literal: true

module Kino
  # @private
  # What RactorSupervisor and ThreadedPool share; they define add_worker, active_count, groups,
  # replace, kill_stragglers, join_set, member? and forget_slots. @lock guards the tables only:
  # nothing claims a slot or starts a thread under it, and what a decision reads shares the
  # critical section of its change. Quarantine: `replace` marks slots first; a crash or exit
  # meanwhile withholds them from the bank, and `unmark` releases them if the quarantine fails.
  class Pool
    # The crash of a worker thread killed short of its loop's end, which no rescue sees.
    KILLED = "the worker's thread was killed"

    def initialize(server_id, app, workers:, threads:, batch: 1, hooks: nil, on_worker_exit: nil)
      @server_id = server_id
      @app = app
      @workers = workers
      @threads = threads
      @batch = batch
      @hooks = hooks
      @on_worker_exit = on_worker_exit
      @lock = Mutex.new
      @bank = SlotBank.new(server_id)
      # Worker index => slot ids, and slot id => worker index. The lists are replaced, never
      # changed in place, so a read under @lock stays a snapshot.
      @slots = {}
      @owner = {}
      # Worker index => true once the scaler asked it to leave.
      @retiring = {}
      @next_index = -1
      # Slot id => true once `replace` marked it; and the marked slots a crash or exit gave up.
      @quarantined = {}
      @withheld = {}
      # Closed at shutdown: no respawn after it, and a waiting respawn stops waiting.
      @halt = Thread::Queue.new
    end

    def start
      @workers.times { add_worker }
      report_active
      self
    end

    def grow
      index = add_worker
      report_active
      index
    end

    # Send worker `index` home: no new work, it leaves when idle, its sockets close with 1001.
    def retire(index)
      slot_ids = @lock.synchronize do
        next nil unless member?(index) && !@retiring.key?(index)

        @retiring[index] = true
        @slots[index].map { |id| [id, @bank.generation(id)] }
      end
      return false unless slot_ids

      slot_ids.each { |id, generation| Native.retire_slot(@server_id, id, generation) }
      true
    end

    # Flag the drain and join the threads for up to `timeout` seconds; dead ones count as joined.
    def shutdown(timeout)
      @halt.close
      Teardown.join_all(timeout) { join_set }
    end

    def done?
      join_set.none?(&:alive?)
    end

    private

    def next_index = @lock.synchronize { @next_index += 1 }

    # Crash recovery for worker `index` on slots `ids`: release all but the marked, report the exit
    # (the block), and pace the respawn by `backoff`; false if wedged, leaving or draining.
    def recover(index, ids, cause, backoff, crashed_at = nil, leaving_respawns: false)
      # Read before the bank may hand the slots on and reset their counts.
      served = @bank.served_any?(ids)
      released, wedged, leaving = forget_slots(index, ids)
      @bank.release(released)
      yield if block_given?
      crashed_at ||= Kino.monotonic
      respawn = !wedged && (leaving_respawns || !leaving) && !draining?
      delay = backoff.crashed(cause, crashed_at, respawn: respawn, served: served)
      !delay.nil? && wait_to_respawn(crashed_at + delay)
    end

    # A bank slot mapped to worker `index`; nil when the block (asked twice) calls it off.
    def claim_slot(index)
      return if @lock.synchronize { yield }

      id = @bank.claim
      kept = @lock.synchronize do
        next false if yield

        map_slot(index, id)
        true
      end
      return id if kept

      @bank.release([id])
      nil
    end

    # Under @lock: slot `id` is worker `index`'s now.
    def map_slot(index, id)
      @slots[index] += [id]
      @owner[id] = index
    end

    # Under @lock: unmap the slots of `ids` still worker `index`'s; returns them.
    def unmap(index, ids)
      ids.select { |id| @owner[id] == index }.each do |id|
        @owner.delete(id)
        @slots[index] -= [id]
      end
    end

    # Under @lock: `ids` but the marked ones, which are withheld for `unmark`.
    def releasable(ids)
      marked, free = ids.partition { |id| @quarantined.key?(id) }
      marked.each { |id| @withheld[id] = true }
      free
    end

    # Quarantine marked slot `id` if still at `generation` (sockets close with 1011); else unmark.
    def quarantine(id, generation)
      return true if Native.quarantine_slot(@server_id, id, generation)

      unmark([id])
      false
    end

    # Unmark `ids`, releasing those a crash or exit withheld meanwhile.
    def unmark(ids)
      withheld = @lock.synchronize do
        ids.select do |id|
          @quarantined.delete(id)
          @withheld.delete(id)
        end
      end
      @bank.release(withheld)
    end

    # on_worker_exit for worker (in :threaded mode, slot) `index`; `cause` is nil for a clean exit.
    def worker_exited(index, cause) = HookFire.fire(@on_worker_exit, "on_worker_exit", index, cause)

    def report_active
      Native.set_active_workers(@server_id, active_count)
    end

    def draining? = @halt.closed?

    # Wait until `deadline` for a respawn; false once the pool is shutting down.
    def wait_to_respawn(deadline)
      remaining = deadline - Kino.monotonic
      @halt.pop(timeout: remaining) if remaining.positive?
      !draining?
    end

    def replacement_failed(index, error)
      Log.error("worker-#{index} was not given a replacement thread: #{error.class}: #{error.message}")
    end
  end
end
