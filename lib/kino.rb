# frozen_string_literal: true

require_relative "kino/version"
require "kino/kino"

# A high-performance Ractor web server for Ruby 4.0+: Rack 3-based, with
# a Rust (tokio + hyper) front-end and Ractor-parallel Ruby workers, plus
# a threaded fallback mode for apps (such as Rails) that are not
# Ractor-shareable.
#
# The public API is {Kino::Server}, {Kino::Configuration}, {Kino::Check},
# {Kino::Logger}, and {Kino.sleep}; the `kino` executable is implemented
# by {Kino::CLI}.
module Kino
  # Base class for all errors raised by Kino.
  class Error < StandardError; end

  # Raised when mode: :ractor is forced but the app is not Ractor-shareable.
  class UnshareableAppError < Error; end

  # High-resolution sleep that bypasses the VM timer (whose wakeups inside
  # non-main ractors are coarse: ~+2.5ms on Linux). Sleeps on the OS clock
  # with the GVL released; chunked so Thread#kill and shutdown interrupts
  # are honored between chunks.
  #
  # @param seconds [Numeric] how long to sleep; must be non-negative
  # @return [nil]
  # @raise [ArgumentError] for negative or non-numeric durations
  def self.sleep(seconds)
    remaining = Float(seconds)
    raise ArgumentError, "sleep duration must be non-negative" if remaining.negative?

    remaining = Native.sleep_chunk(remaining) while remaining.positive?
    nil
  end
end

require_relative "kino/cli"
require_relative "kino/logger"
require_relative "kino/check"
require_relative "kino/input"
require_relative "kino/null_input"
require_relative "kino/errors_stream"
require_relative "kino/stream"
require_relative "kino/configuration"
require_relative "kino/worker"
require_relative "kino/ractor_supervisor"
require_relative "kino/server"

# Hand the frozen shareable singletons to the native layer: it sets them
# straight into each request's env, so the worker loop allocates neither
# an errors stream nor (for bodyless requests) an input object.
Kino::Native.register_defaults(Kino::ErrorsStream::INSTANCE, Kino::NullInput::INSTANCE)
