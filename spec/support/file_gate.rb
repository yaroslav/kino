# frozen_string_literal: true

require "securerandom"
require "tmpdir"

# A gate that worker threads wait at until the spec opens it, in either
# mode: a Thread::Queue cannot cross into a worker ractor, but a file
# path can. Frozen and shareable, so handlers and hooks can hold it.
FileGate = Data.define(:path) do
  # Yields a closed gate, and cleans up after it.
  def self.closed
    gate = new(path: File.join(Dir.tmpdir, "kino-gate-#{SecureRandom.hex(8)}").freeze)
    yield gate
  ensure
    File.delete(gate.path) if gate && File.exist?(gate.path)
  end

  def open = File.write(path, "")

  # Returns once the gate is open, or after `timeout` seconds.
  def wait(timeout = WsClient::DEADLINE)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    Kino.sleep(0.005) until File.exist?(path) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  end
end
