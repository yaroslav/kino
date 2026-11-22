# frozen_string_literal: true

module Kino
  # @private
  # Paces the respawns of one worker and its replacements, in both pools. A worker that crashed
  # within STEADY seconds of starting, before serving a request, waits a delay that doubles per
  # crash in a row, FIRST up to LONGEST; any other respawns at once, and the count starts over.
  # The first crash of a run is logged with its backtrace, each later one with a line.
  class CrashBackoff
    FIRST = 0.1
    LONGEST = 5.0
    STEADY = 1.0

    # The backoff of thread `position` of worker `index`, named as the log names it:
    # `worker-3`, or `worker-3/thread-2`.
    def self.for(index, position, threads)
      new((threads > 1) ? "worker-#{index}/thread-#{position + 1}" : "worker-#{index}")
    end

    def initialize(name, now = Kino.monotonic)
      @subject = "#{name} crashed"
      @streak = 0
      @delay = 0.0
      started(now)
    end

    # The worker, or its replacement, starts now.
    def started(now = Kino.monotonic)
      @started_at = now
    end

    # Log a crash; the seconds to wait before the respawn (`respawn: false`: only log).
    def crashed(cause, now = Kino.monotonic, respawn: true, served: false)
      if served || now - @started_at >= STEADY
        @streak = 0
        @delay = 0.0
        Log.failure(@subject + respawning(respawn, "at once"), cause)
      else
        @streak += 1
        @delay = (@delay * 2).clamp(FIRST, LONGEST)
        quick = "#{@subject} within a second of starting"
        if @streak == 1
          Log.failure(quick + respawning(respawn), cause)
        else
          Log.error("#{quick} again, #{@streak} times in a row (#{cause.class}: #{cause.message})" +
            respawning(respawn))
        end
      end
      @delay if respawn
    end

    private

    def respawning(respawn, at = format("in %.1f s", @delay)) = respawn ? "; respawning #{at}" : ""
  end
end
