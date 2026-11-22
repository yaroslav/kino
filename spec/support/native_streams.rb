# frozen_string_literal: true

require "tempfile"

# Native log lines go to fd 1/2 via Rust, invisible to Ruby's $stdout and
# $stderr objects: capture by swapping the file descriptor itself.
module NativeStreams
  def capture_native_stderr(&block)
    capture_native($stderr, &block)
  end

  def capture_native_stdout(&block)
    capture_native($stdout, &block)
  end

  private

  def capture_native(stream)
    original = stream.dup
    Tempfile.create("kino-stream") do |captured|
      stream.reopen(captured)
      yield
      stream.flush
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
