# frozen_string_literal: true

module Kino
  # @private
  # Grows and shrinks the worker pool between `floor` and `ceiling`. One
  # thread on the main ractor polls the native queue and per-slot sensors
  # every tick and drives a pool (RactorSupervisor or ThreadedPool) through
  # three methods: `active_count`, `groups` (worker index => slot ids for
  # every worker that may be retired), `grow`, and `retire(index)`.
  #
  # Policy: grow by one worker per tick once requests have been waiting in
  # the queue on two consecutive ticks (a burst that clears within a tick
  # is not pressure); retire one worker per tick, the one idle longest,
  # once it has been idle for `scale_down_after`. "Idle" means no slot of
  # the worker held a request, an open socket or an open hijacked connection
  # at two consecutive samples, and its served count did not move between them.
  class PoolScaler < Monitor
    TICK = 0.1
    # Consecutive ticks with a non-empty queue before the pool grows.
    PRESSURE_TICKS = 2

    def initialize(server_id:, pool:, floor:, ceiling:, scale_down_after:, tick: TICK)
      super(name: "pool scaler", tick: tick)
      @server_id = server_id
      @pool = pool
      @floor = floor
      @ceiling = ceiling
      @scale_down_after = scale_down_after
      @pressure = 0
      @last_served = {}
      @idle_since = {}
    end

    # One policy step over one set of observations: the monotonic time,
    # the queue depth, and a WorkerRow per slot. Public so the policy is
    # testable without a server.
    def step(now, queued, rows)
      @pressure = queued.positive? ? @pressure + 1 : 0
      if @pressure >= PRESSURE_TICKS
        grow if @pool.active_count < @ceiling
        return
      end

      observe_idle(now, rows)
      return unless @pool.active_count > @floor

      index, since = @idle_since.min_by { |_index, at| at }
      return unless index && now - since >= @scale_down_after

      retire(index)
    end

    private

    def scan
      queued, _in_flight = Native.queue_stats(@server_id)
      step(Kino.monotonic, queued, WorkerRow.all(@server_id))
    end

    def grow
      index = @pool.grow
      return unless index

      Native.record_scale_up(@server_id)
      Log.info("pool grew to #{@pool.active_count} workers (queue pressure)")
    end

    def retire(index)
      return unless @pool.retire(index)

      forget(index)
      Native.record_scale_down(@server_id)
      Log.info("pool shrank to #{@pool.active_count} workers (worker-#{index} idle)")
    end

    # Per worker: idle when no slot is occupied (WorkerRow#occupied?) at this sample and the
    # served total did not move since the previous one; a first sighting is never idle.
    def observe_idle(now, rows)
      by_slot = rows.to_h { |row| [row.slot, row] }
      groups = @pool.groups
      (@idle_since.keys - groups.keys).each { |index| forget(index) }
      groups.each do |index, slot_ids|
        served = slot_ids.sum { |id| by_slot[id]&.served || 0 }
        busy = slot_ids.any? { |id| by_slot[id]&.occupied? }
        idle = !busy && @last_served[index] == served
        @last_served[index] = served
        if idle
          @idle_since[index] ||= now
        else
          @idle_since.delete(index)
        end
      end
    end

    def forget(index)
      @idle_since.delete(index)
      @last_served.delete(index)
    end
  end
end
