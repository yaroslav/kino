# frozen_string_literal: true

module Kino
  # @private
  # A thread on the main ractor that calls `scan` every `tick` seconds
  # until stopped. Main-ractor so it stays responsive when worker ractors
  # are wedged. A scan that raises is logged and skipped, never fatal:
  # monitors keep the server healthy, they must not take it down.
  class Monitor
    def initialize(name:, tick:)
      @name = name
      @tick = tick
      @running = false
      @thread = nil
    end

    def start
      @running = true
      @thread = Thread.new do
        Thread.current.name = @name
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
      Log.error("#{@name} crashed: #{e.class}: #{e.message}")
    end

    def tick
      scan
    rescue => e
      Log.error("#{@name} tick error: #{e.class}: #{e.message}")
    ensure
      sleep @tick
    end

    # One poll; subclasses define it.
    def scan
      raise NotImplementedError, "#{self.class} must define scan"
    end
  end
end
