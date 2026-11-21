# frozen_string_literal: true

module Kino
  # @private
  # Spawns worker ractors and keeps them alive. One supervisor thread per
  # ractor: a crashed worker thread is replaced on a bank slot while its
  # siblings serve on, and a dead ractor's requests get their 500 before it
  # respawns, both paced by a CrashBackoff.
  #
  # The :ractor-mode Pool.
  class RactorSupervisor < Pool
    # A worker thread's crash report, sent to its supervisor's port (.report_crash). The only
    # other message there is Ractor#monitor's end notice (a Symbol up to Ruby 4.0, an Array from
    # 4.1): a class of its own keeps the two apart.
    Crash = Data.define(:position, :id, :error, :backtrace, :started_at, :crashed_at, :reply)

    def initialize(...)
      super
      @supervisor_threads = []
      @replaced = {}
      # Worker index => true while its Ractor runs.
      @live = {}
    end

    # Ractors cannot be force-killed; their clients were already freed by
    # abort_all_inflight. A stuck ractor leaks until process exit.
    def kill_stragglers
      Log.error("shutdown deadline passed with stuck ractor workers") unless done?
    end

    # Workers that are staying: the live ones minus those the quarantine
    # monitor abandoned as wedged (their replacements count instead) and
    # minus those already told to leave. A retiring worker leaves the
    # count at once, not when its ractor finally exits, so the scaler
    # never sees a stale surplus and retires past its floor.
    def active_count
      @lock.synchronize do
        @live.count { |index, _| !@replaced.key?(index) && !@retiring.key?(index) }
      end
    end

    # Worker index => slot ids for every worker the scaler may retire:
    # live, not already leaving, not quarantined, and past its spawn (a
    # worker marked live whose supervisor thread has not assigned slots
    # yet is not listed until it has).
    def groups
      @lock.synchronize do
        @slots.slice(*@live.keys).reject { |index, _| @retiring.key?(index) || @replaced.key?(index) }
      end
    end

    # Replace the Ractor on slot `worker_id` at `generation`: spawn a fresh one, then quarantine
    # the old one's slots; the old Ractor leaks, unrespawned. False for a slot that moved on.
    def replace(worker_id, generation)
      # Checked and claimed at once, so a slot that moves on cannot get a healthy Ractor
      # quarantined; the slots are marked then (see Pool). A stale snapshot replaces a Ractor once.
      worker_index, slot_ids = @lock.synchronize do
        index = @owner[worker_id]
        next unless index && @bank.generation(worker_id) == generation && !@replaced.key?(index)

        @replaced[index] = true
        @slots[index].each { |id| @quarantined[id] = true }
        [index, @slots[index].map { |id| [id, @bank.generation(id)] }]
      end
      return false unless worker_index

      thread =
        begin
          supervise(next_index) # spawn FIRST, nothing quarantined yet
        rescue
          @lock.synchronize { @replaced.delete(worker_index) } # allow retry next tick
          unmark(slot_ids.map(&:first))
          raise
        end
      # Quarantine only after success; each slot's sockets close with 1011.
      slot_ids.each { |id, slot_generation| quarantine(id, slot_generation) }
      @lock.synchronize { @supervisor_threads << thread }
      true
    end

    # @private
    # A worker Ractor's main thread, one worker thread per slot; a crashed one waits for a new slot.
    def self.run_worker(server_id, ids, app, batch, hooks, supervisor)
      # Crashes are the supervisor's to log; no thread here prints one.
      Thread.current.report_on_exception = false
      ended = Thread::Queue.new
      start = lambda do |id, position|
        Thread.new do
          Thread.current.report_on_exception = false
          Thread.current.name = "thread-#{position + 1}" if ids.size > 1
          started_at = Kino.monotonic
          fresh = nil
          begin
            Worker.run(server_id, id, app, batch, hooks)
          rescue Exception => e # rubocop:disable Lint/RescueException -- reported, then replaced
            fresh = report_crash(supervisor, position, id, e, started_at)
          ensure
            replaced = begin
              fresh && start.call(fresh, position)
            rescue ThreadError
              nil
            end
            ended << position unless replaced
          end
        end
      end
      ids.each_with_index { |id, position| start.call(id, position) }
      ids.size.times { ended.pop }
    end

    # Report a crash and return the answer; the exception crosses as a copy, its backtrace aside.
    def self.report_crash(supervisor, position, id, error, started_at)
      crashed_at = Kino.monotonic
      reply = Ractor::Port.new
      crash = Crash.new(position, id, error, error.backtrace, started_at, crashed_at, reply)
      begin
        supervisor << crash
      rescue TypeError, Ractor::Error
        supervisor << crash.with(error: Error.new("#{error.class}: #{error.message}"))
      end
      reply.receive
    ensure
      reply&.close
    end
    private_class_method :report_crash

    private

    def add_worker
      index = next_index
      thread = supervise(index)
      @lock.synchronize { @supervisor_threads << thread }
      index
    end

    def join_set = @lock.synchronize { @supervisor_threads.dup }

    def member?(index) = @live.key?(index)

    def supervise(index)
      # Live from the moment it is asked for, not from when its thread gets
      # around to spawning: `grow` reports the count right after this.
      @lock.synchronize { @live[index] = true }
      Thread.new do
        Thread.current.name = "supervisor-#{index}"
        backoff = CrashBackoff.new("worker-#{index}")
        loop do
          port = Ractor::Port.new
          ractor = spawn_worker(index, port)
          backoff.started
          tend(index, ractor, port)
          begin
            ractor.value
            worker_exited(index, nil) # drained or retired
            exited(index, clean: true)
            break
          rescue Ractor::Error => e
            # The ractor died mid-flight. Anything it was serving will never
            # be answered by Ruby: 500 those clients NOW (not when GC gets
            # around to dropping the dead heap), then decide on respawn.
            # Its sockets close too: their handlers died with it.
            dead = @lock.synchronize { @slots[index] }
            dead.each { |id| Native.abort_inflight(@server_id, id) }
            cause = (e.respond_to?(:cause) && e.cause) ? e.cause : e
            # A replaced worker is not respawned; a leaving one is, and the scaler retires it again.
            respawn = recover(index, dead, cause, backoff, leaving_respawns: true) { worker_exited(index, cause) }
            unless respawn
              exited(index, clean: false)
              break
            end

            Native.record_respawn(@server_id)
            @lock.synchronize { @retiring.delete(index) }
          end
        end
      end
    end

    # Answer each Crash on its own thread (each waits only its own delay) until the Ractor exits.
    def tend(index, ractor, port)
      ractor.monitor(port) # the notice comes after any crash report
      backoffs = Array.new(@threads) { |position| CrashBackoff.for(index, position, @threads) }
      run = {over: false}
      while (crash = port.receive).is_a?(Crash)
        Thread.new(crash) { |report| answer(index, report, backoffs, run) }
      end
    ensure
      @lock.synchronize { run[:over] = true } if run
      port.close
    end

    def answer(index, crash, backoffs, run)
      Thread.current.name = "supervisor-#{index}"
      cause = crash.error
      # A copied exception arrives without its backtrace.
      cause.set_backtrace(crash.backtrace) if crash.backtrace && !cause.frozen?
      backoff = backoffs[crash.position]
      backoff.started(crash.started_at)
      crash.reply << replace_thread(index, crash.id, cause, backoff, crash.crashed_at, run)
    rescue Ractor::ClosedError
      nil # the ractor is gone; the supervisor sees to its slots
    end

    # Release the crashed thread's slot (unless wedged); after its backoff, a fresh one, if due.
    def replace_thread(index, dead_id, cause, backoff, crashed_at, run)
      return unless recover(index, [dead_id], cause, backoff, crashed_at) { worker_exited(index, cause) }

      id = claim_slot(index) { run[:over] || @retiring.key?(index) || @replaced.key?(index) }
      Native.record_respawn(@server_id) if id
      id
    rescue => e
      replacement_failed(index, e)
      nil
    end

    # Unmap before the bank may hand the slots on, so no generation pairs with a later worker.
    def forget_slots(index, ids)
      @lock.synchronize { [releasable(unmap(index, ids)), @replaced.key?(index), @retiring.key?(index)] }
    end

    # Fresh ractor on slots from the bank: fresh ones, or ones a retired
    # worker handed back. It asks for replacement threads on `port`.
    def spawn_worker(worker_index, port)
      worker_ids = Array.new(@threads) { @bank.claim }
      @lock.synchronize do
        @slots[worker_index] = worker_ids
        worker_ids.each { |id| @owner[id] = worker_index }
      end
      # Named so log lines from inside say which worker spoke: the ractor
      # alone for a single thread, `worker-N/thread-M` for more.
      Ractor.new(@server_id, worker_ids, @app, @batch, @hooks, port, name: "worker-#{worker_index}") do |*worker|
        Kino::RactorSupervisor.run_worker(*worker)
      end
    end

    # Bookkeeping for a supervisor thread that is done: the worker is no
    # longer live, a retired worker's slots go back to the bank, and the
    # thread leaves the join set so a long-lived elastic pool does not
    # accumulate dead threads.
    def exited(index, clean:)
      released = @lock.synchronize do
        @live.delete(index)
        @supervisor_threads.delete(Thread.current)
        retired = @retiring.delete(index)
        (retired && clean) ? releasable(@slots[index]) : []
      end
      @bank.release(released)
      report_active
    end
  end
end
