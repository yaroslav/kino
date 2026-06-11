# frozen_string_literal: true

module Kino
  # @private
  # rack.errors: stateless writer into the native logger. Frozen singleton,
  # which also makes it Ractor-shareable; one instance serves all workers.
  class ErrorsStream
    def puts(message)
      Native.log_error(message.to_s)
      nil
    end

    def write(message)
      message = message.to_s
      Native.log_error(message)
      message.bytesize
    end

    def flush
      self
    end

    INSTANCE = new.freeze
  end
end
