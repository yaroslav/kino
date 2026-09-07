# frozen_string_literal: true

module Kino
  # @private
  # The :threaded-mode worker pool: `workers` groups of `threads` plain
  # Threads, each thread on its own dispatch slot. A group is one ractor's
  # worth of capacity, so `workers` and `max_workers` mean the same thing
  # in both modes. Behind PoolScaler here (`grow`, `retire`, `groups`,
  # `active_count`), the quarantine monitor's replacer (`replace`), and
  # the join/kill sweeps Server#shutdown runs.
  class ThreadedPool
    def initialize(server_id, app, threads:, batch: 1, hooks: nil, on_worker_exit: nil)
      @server_id = server_id
      @app = app
      @threads = threads
      @batch = batch
      @hooks = hooks
      @on_worker_exit = on_worker_exit
      @lock = Mutex.new
      # index => {slots:, threads:}; the groups asked to leave; the slot
      # ids retired groups gave back; quarantine replacements (one thread
      # each, standing in for a wedged slot: neither counted nor retired,
      # so the wedged group keeps counting as the capacity it still is).
      @groups = {}
      @slot_to_group = {}
      @retiring = {}
      @free_slots = []
      @replacements = {}
      @wedged = {}
      @next_index = -1
    end

    def start(workers)
      workers.times { spawn_group }
      report_active
      self
    end

    # Groups alive and serving, quarantine replacements aside.
    def active_count
      @lock.synchronize { @groups.count { |index, _| !@replacements.key?(index) } }
    end

    # Group index => slot ids for every group the scaler may retire: not
    # already leaving, not wedged, not a quarantine replacement.
    def groups
      reap
      @lock.synchronize do
        @groups
          .reject { |index, _| @retiring.key?(index) || @wedged.key?(index) || @replacements.key?(index) }
          .transform_values { |group| group[:slots].dup }
      end
    end

    # Add one group; returns its index.
    def grow
      reap
      index = spawn_group
      report_active
      index
    end

    # Send a group home: its slots stop receiving work now, each thread
    # finishes what it holds and leaves at its next idle tick, and the
    # slots come back to the free list once every thread has exited.
    def retire(index)
      slots = @lock.synchronize do
        next nil unless @groups.key?(index) && !@retiring.key?(index)

        @retiring[index] = true
        @groups[index][:slots]
      end
      return false unless slots

      slots.each { |id| Native.retire_slot(@server_id, id) }
      true
    end

    # The quarantine replacer: spawn a replacement thread on a fresh slot
    # FIRST (may raise ThreadError), then quarantine the wedged slot.
    def replace(worker_id)
      spawn_group(slots: 1, replacement: true)
      Native.quarantine_slot(@server_id, worker_id)
      @lock.synchronize do
        wedged = @slot_to_group[worker_id]
        @wedged[wedged] = true if wedged
      end
      true
    end

    # Join every thread up to the (numeric) deadline.
    def shutdown(timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      all_threads.each do |thread|
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        thread.join([remaining, 0.01].max)
      end
    end

    def done?
      all_threads.none?(&:alive?)
    end

    # Block until every thread exits on its own (drain elsewhere).
    def join
      all_threads.each(&:join)
    end

    def kill_stragglers
      all_threads.each { |thread| thread.kill if thread.alive? }
    end

    private

    def all_threads
      @lock.synchronize { @groups.values.flat_map { |group| group[:threads] } }
    end

    # The group is in the table before its first thread starts, so a
    # ThreadError partway through leaves nothing untracked for shutdown.
    def spawn_group(slots: @threads, replacement: false)
      group = {slots: [], threads: []}
      index = @lock.synchronize do
        @next_index += 1
        @groups[@next_index] = group
        @replacements[@next_index] = true if replacement
        @next_index
      end
      slots.times do
        id = claim_slot
        @lock.synchronize do
          group[:slots] << id
          @slot_to_group[id] = index
        end
        thread = spawn_thread(id)
        @lock.synchronize { group[:threads] << thread }
      end
      index
    end

    def spawn_thread(worker_id)
      Thread.new do
        # Named so log lines from inside say which worker spoke.
        Thread.current.name = "worker-#{worker_id}"
        error = nil
        begin
          Worker.run(@server_id, worker_id, @app, @batch, @hooks)
        rescue Exception => e # rubocop:disable Lint/RescueException -- a hard crash in a threaded worker thread
          error = e
          raise
        ensure
          HookFire.fire(@on_worker_exit, "on_worker_exit", worker_id, error)
        end
      end
    end

    # A slot a retired group gave back, reset for its new occupant, or a
    # fresh one.
    def claim_slot
      id = @lock.synchronize { @free_slots.pop }
      return Native.register_worker(@server_id) unless id

      Native.reset_slot(@server_id, id)
      id
    end

    # Retiring groups whose threads have all exited give their slots back
    # and leave the table. Runs before every pool decision, so the count
    # the control plane sees never lags by more than a scaler tick.
    def reap
      freed = @lock.synchronize do
        done = @retiring.keys.select { |index| @groups[index][:threads].none?(&:alive?) }
        done.each do |index|
          group = @groups.delete(index)
          @retiring.delete(index)
          group[:slots].each { |id| @slot_to_group.delete(id) }
          @free_slots.concat(group[:slots])
        end
        done.any?
      end
      report_active if freed
    end

    def report_active
      Native.set_active_workers(@server_id, active_count)
    end
  end
end
