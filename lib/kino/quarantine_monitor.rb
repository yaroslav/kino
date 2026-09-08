# frozen_string_literal: true

module Kino
  # @private
  # Polls per-slot busy_ms and, past the timeout, quarantines a wedged slot
  # and asks the replacer to spawn a fresh worker. Never interrupts the
  # wedged worker.
  class QuarantineMonitor < Monitor
    def initialize(server_id:, timeout_ms:, max:, replacer:, tick: 0.5)
      super(name: "quarantine monitor", tick: tick)
      @server_id = server_id
      @timeout_ms = timeout_ms
      @max = max
      @replacer = replacer
      @outstanding = 0
      @at_cap_logged = false
    end

    private

    def scan
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
