# frozen_string_literal: true

# Polling for specs that wait on the server or another thread.
module Waiting
  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # How long the block took, in seconds. Not for handlers that run in worker
  # ractors, which need no spec helpers.
  def timed
    started = monotonic
    yield
    monotonic - started
  end

  # Poll the block until it is truthy; past `timeout` seconds, fail naming
  # `what`, and what `detail` returns then.
  def wait_for(what = "the condition", timeout: 5, every: 0.02, detail: nil)
    deadline = monotonic + timeout
    until yield
      raise "timed out after #{timeout}s waiting for #{what}#{": #{detail.call}" if detail}" if monotonic > deadline

      sleep every
    end
  end
end

RSpec.configure do |config|
  config.include Waiting
end
