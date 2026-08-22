# frozen_string_literal: true

module Kino
  # @private
  # Polls per-slot busy_ms and, past the timeout, quarantines a wedged slot
  # and asks the replacer to spawn a fresh worker. Runs one thread on the
  # main ractor (uncontended by wedged worker ractors, so it stays
  # responsive in :ractor mode). Never interrupts the wedged worker.
  class QuarantineMonitor
    def initialize(server_id:, timeout_ms:, max:, replacer:, tick: 0.5)
      @server_id = server_id
      @timeout_ms = timeout_ms
      @max = max
      @replacer = replacer
      @tick = tick
      @outstanding = 0
      @at_cap_logged = false
      @running = false
      @thread = nil
    end

    def start
      @running = true
      @thread = Thread.new do
        Thread.current.name = "quarantine"
        run
      end
      self
    end

    def stop
      @running = false
      @thread&.join(@tick * 2)
    end

    private

    def run
      tick while @running
    rescue => e
      Log.error("quarantine monitor crashed: #{e.class}: #{e.message}")
    end

    def tick
      scan_slots
    rescue => e
      # A bad tick must never kill the monitor.
      Log.error("quarantine tick error: #{e.class}: #{e.message}")
    ensure
      sleep @tick
    end

    def scan_slots
      Native.worker_stats(@server_id).each do |index, _served, _in_flight, busy_ms, quarantined|
        next if quarantined || busy_ms <= @timeout_ms

        if @outstanding >= @max
          unless @at_cap_logged
            Log.warn("quarantine at cap (#{@max}); serving at reduced capacity")
            @at_cap_logged = true
          end
          next
        end

        if @replacer.replace(index)
          Native.record_quarantine_replacement(@server_id)
          @outstanding += 1
          @at_cap_logged = false
        end
      end
    end
  end
end
