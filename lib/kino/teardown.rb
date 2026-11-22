# frozen_string_literal: true

module Kino
  # @private
  module Teardown
    module_function

    # Thread#join(limit), minus the re-raise of the crash the thread died
    # of: that was reported when it happened, and must not cut a shutdown
    # short.
    # @return [Thread, nil] the thread once it has ended, nil on timeout
    def join(thread, limit)
      thread.join(limit)
    rescue Exception # rubocop:disable Lint/RescueException -- the joined thread's crash, reported already
      raise if thread.alive? # not its crash: one aimed at the caller

      thread
    end

    # Join each thread the block lists within `timeout` seconds (then 10 ms each), then any it
    # lists afresh: a joined thread may have added one (a crashed worker's replacement).
    def join_all(timeout)
      deadline = Kino.monotonic + timeout
      joined = {}
      until (pending = yield.reject { |thread| joined.key?(thread) }).empty?
        pending.each do |thread|
          join(thread, [deadline - Kino.monotonic, 0.01].max)
          joined[thread] = true
        end
      end
    end
  end
end
