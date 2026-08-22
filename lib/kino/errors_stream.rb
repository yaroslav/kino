# frozen_string_literal: true

module Kino
  # @private
  # rack.errors: stateless writer into the server log (one line per
  # call, labelled like every other line Kino writes). Frozen singleton,
  # which also makes it Ractor-shareable; one instance serves all workers.
  class ErrorsStream
    def puts(message)
      Log.error(message.to_s.chomp)
      nil
    end

    def write(message)
      message = message.to_s
      Log.error(message.chomp)
      message.bytesize
    end

    def flush
      self
    end

    INSTANCE = new.freeze
  end
end
