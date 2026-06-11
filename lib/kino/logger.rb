# frozen_string_literal: true

require "logger"

module Kino
  # A ::Logger writing through the native async sink: formatted lines go
  # onto a lock-free channel and a Rust flusher thread batches them into
  # the output: no per-line mutex (which serializes every worker thread)
  # and no write syscall on request threads.
  #
  #   # e.g. Rails, config/environments/production.rb:
  #   config.logger = Kino::Logger.new                       # stdout
  #   config.logger = Kino::Logger.new("log/production.log")
  #
  # Durability: a graceful shutdown drains everything; a hard crash can
  # lose the tail of the buffer (the standard async-logging trade-off).
  class Logger < ::Logger
    # @param path [String, nil] log file path (created/appended), or nil
    #   for stdout
    # @param options [Hash] passed through to ::Logger#initialize
    #   (progname:, level:, formatter:, ...)
    def initialize(path = nil, **options)
      super(Device.new(path), **options)
    end

    # The raw IO-like device for integrations that want bytes without
    # ::Logger's formatting: Rack::CommonLogger, ActiveSupport::Logger.new,
    # a BroadcastLogger arm, ... Frozen and holding only an Integer id, so
    # it is Ractor-shareable; one device can serve every worker.
    class Device
      # @param path [String, nil] a file (created/appended) or nil for stdout
      def initialize(path = nil)
        @id = Native.log_device_open(path&.to_s)
        freeze
      end

      # Queue one formatted line on the async sink; never blocks.
      # @param message [String]
      # @return [void]
      def write(message)
        Native.log_device_write(@id, message.to_s)
      end

      # Close the device: the flusher drains its queue and exits. Writes
      # after close are ignored.
      # @return [void]
      def close
        Native.log_device_close(@id)
      end

      # ::Logger probes these on its device.
      def reopen(*) = self
      alias_method :<<, :write
    end
  end
end
