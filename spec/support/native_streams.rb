# frozen_string_literal: true

require "tempfile"

# Native log lines go to fd 1/2 via Rust, invisible to Ruby's $stdout and
# $stderr objects: capture by swapping the file descriptor itself.
module NativeStreams
  def capture_native_stderr(&block)
    capture_native($stderr, &block)
  end

  # With `awaiting`, returns once the stream holds a match for it: the
  # access log is written by a thread of its own, after the response.
  def capture_native_stdout(awaiting: nil, &block)
    capture_native($stdout, awaiting, &block)
  end

  private

  def capture_native(stream, awaiting = nil)
    original = stream.dup
    Tempfile.create("kino-stream") do |captured|
      stream.reopen(captured)
      yield
      stream.flush
      wait_for("#{awaiting.inspect} in the captured stream") { File.read(captured.path).match?(awaiting) } if awaiting
      captured.rewind
      captured.read
    ensure
      stream.reopen(original)
    end
  end
end

RSpec.configure do |config|
  config.include NativeStreams
  # `:quiet` examples make the server log failures: their native stderr is captured, and
  # shown only when the example fails.
  config.around(:example, :quiet) do |example|
    log = capture_native_stderr { example.run }
    warn log if example.exception
  end
end
