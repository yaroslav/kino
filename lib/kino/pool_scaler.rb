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
  # the worker held a request at two consecutive samples and its served
  # count did not move between them, so a worker serving short requests
  # between samples is never mistaken for an idle one.
  class PoolScaler
    TICK = 0.1
    # Consecutive ticks with a non-empty queue before the pool grows.
    PRESSURE_TICKS = 2

    def initialize(server_id:, pool:, floor:, ceiling:, scale_down_after:, tick: TICK)
      @server_id = server_id
      @pool = pool
      @floor = floor
      @ceiling = ceiling
      @scale_down_after = scale_down_after
      @tick = tick
      @pressure = 0
      @last_served = {}
      @idle_since = {}
      @running = false
      @thread = nil
    end

    def start
      @running = true
      @thread = Thread.new do
        Thread.current.name = "pool-scaler"
        run
      end
      self
    end

    def stop
      @running = false
      @thread&.join(@tick * 2)
    end

    # One policy step over one set of observations: the monotonic time,
    # the queue depth, and worker_stats rows ([slot, served, in_flight,
    # busy_ms, quarantined, retired]). Public so the policy is testable
    # without a server.
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

    def run
      tick while @running
    rescue => e
      Log.error("pool scaler crashed: #{e.class}: #{e.message}")
    end

    def tick
      queued, _in_flight = Native.queue_stats(@server_id)
      step(Process.clock_gettime(Process::CLOCK_MONOTONIC), queued, Native.worker_stats(@server_id))
    rescue => e
      # A bad tick must never kill the scaler.
      Log.error("pool scaler tick error: #{e.class}: #{e.message}")
    ensure
      sleep @tick
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

    # Per worker: idle when no slot holds a request at this sample and the
    # served total did not move since the previous one. First sighting of
    # a worker is never idle (it takes two samples to know).
    def observe_idle(now, rows)
      by_slot = rows.to_h { |row| [row[0], row] }
      groups = @pool.groups
      (@idle_since.keys - groups.keys).each { |index| forget(index) }
      groups.each do |index, slot_ids|
        served = slot_ids.sum { |id| by_slot.dig(id, 1) || 0 }
        busy = slot_ids.any? { |id| (by_slot.dig(id, 2) || 0).positive? }
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
