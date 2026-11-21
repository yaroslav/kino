# frozen_string_literal: true

module Kino
  # @private
  # The :threaded-mode Pool: `workers` groups of `threads` Threads, a group being one Ractor's
  # worth of capacity; a crashed thread is replaced in its group, paced by a CrashBackoff.
  class ThreadedPool < Pool
    def initialize(...)
      super
      # index => its threads; quarantine replacements (one thread each: not counted, not retired).
      @groups = {}
      @replacements = {}
      @wedged = {}
    end

    # Groups that are staying: quarantine replacements aside, and minus
    # those already told to leave. A retiring group leaves the count at
    # once, not when its last thread exits, so the scaler never sees a
    # stale surplus and retires past its floor.
    def active_count
      @lock.synchronize do
        @groups.count { |index, _| !@replacements.key?(index) && !@retiring.key?(index) }
      end
    end

    # Group index => slot ids for every group the scaler may retire: not
    # already leaving, not wedged, not a quarantine replacement.
    def groups
      reap
      @lock.synchronize do
        @slots.reject { |index, _| @retiring.key?(index) || @wedged.key?(index) || @replacements.key?(index) }
      end
    end

    # Retiring groups that have left are reaped first.
    def grow
      reap
      super
    end

    # The quarantine replacer for slot `worker_id` at `generation`: mark the slot (a crash
    # meanwhile gets no second replacement), spawn a replacement on a bank slot (a ThreadError
    # unmarks it), then quarantine. False for a slot that moved on since the sample.
    def replace(worker_id, generation)
      marked = @lock.synchronize do
        next false if @quarantined.key?(worker_id) || !@owner.key?(worker_id)
        next false unless @bank.generation(worker_id) == generation

        @quarantined[worker_id] = true
      end
      return false unless marked

      begin
        spawn_group(slots: 1, replacement: true)
      rescue
        unmark([worker_id])
        raise
      end
      if quarantine(worker_id, generation)
        @lock.synchronize { @wedged[@owner[worker_id]] = true if @owner.key?(worker_id) }
      end
      true
    end

    def kill_stragglers
      join_set.each { |thread| thread.kill if thread.alive? }
    end

    private

    def add_worker = spawn_group

    def join_set = @lock.synchronize { @groups.values.flatten }

    def member?(index) = @groups.key?(index)

    # The group is in the table before its first thread starts, so a
    # ThreadError partway through leaves nothing untracked for shutdown.
    def spawn_group(slots: @threads, replacement: false)
      threads = []
      index = @lock.synchronize do
        @next_index += 1
        @groups[@next_index] = threads
        @slots[@next_index] = []
        @replacements[@next_index] = true if replacement
        @next_index
      end
      slots.times do |position|
        id = @bank.claim
        @lock.synchronize { map_slot(index, id) }
        thread = spawn_thread(index, id, CrashBackoff.for(index, position, slots))
        @lock.synchronize { threads << thread }
      end
      index
    end

    def spawn_thread(index, worker_id, backoff)
      Thread.new do
        # Named so log lines from inside say which worker spoke.
        Thread.current.name = "worker-#{worker_id}"
        backoff.started
        error = crashed_at = nil
        begin
          Worker.run(@server_id, worker_id, @app, @batch, @hooks)
        rescue Exception => e # rubocop:disable Lint/RescueException -- a crash, cleaned up after by Worker.run
          error = e
          crashed_at = Kino.monotonic
        ensure
          worker_exited(worker_id, error)
        end
        replace_crashed(index, worker_id, backoff, error, crashed_at) if error
      end
    end

    # A crashed thread's last act, as its replacement's supervisor: release its slot, then after
    # the backoff spawn a fresh thread, unless the group is leaving, the slot wedged, or draining.
    def replace_crashed(index, dead_id, backoff, error, crashed_at)
      Thread.current.name = "supervisor-#{index}"
      return unless recover(index, [dead_id], error, backoff, crashed_at)

      id = claim_slot(index) { draining? || @retiring.key?(index) }
      return unless id

      Native.record_respawn(@server_id)
      thread = spawn_thread(index, id, backoff)
      @lock.synchronize { @groups[index].keep_if(&:alive?) << thread }
    rescue => e
      replacement_failed(index, e)
    end

    # Unmap the group's slots of `ids`; returns the releasable ones, wedged, and leaving.
    def forget_slots(index, ids)
      @lock.synchronize do
        forgotten = unmap(index, ids)
        released = releasable(forgotten)
        [released, released.size < forgotten.size, !@groups.key?(index) || @retiring.key?(index)]
      end
    end

    # Retiring groups whose threads have all exited give their slots back
    # and leave the table. Runs before every pool decision, so the count
    # the control plane sees never lags by more than a scaler tick.
    def reap
      freed = @lock.synchronize do
        done = @retiring.keys.select { |index| @groups[index].none?(&:alive?) }
        done.flat_map do |index|
          @groups.delete(index)
          @retiring.delete(index)
          slots = @slots.delete(index)
          slots.each { |id| @owner.delete(id) }
          releasable(slots)
        end
      end
      return if freed.empty?

      @bank.release(freed)
      report_active
    end
  end
end
