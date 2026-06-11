# frozen_string_literal: true

module Kino
  # @private
  # The stream handed to Rack 3 streaming bodies (`body.call(stream)`).
  # Rack 3 requires it to be full-duplex: writes go to the native response
  # channel (a slow client blocks the writer with the GVL released), reads
  # pull the remaining request body through the request's own rack.input.
  class Stream
    def initialize(request, input)
      @request = request
      @input = input
      @read_closed = false
      @write_closed = false
    end

    def read(length = nil, buffer = nil)
      raise IOError, "stream is closed for reading" if @read_closed

      @input.read(length, buffer)
    end

    def write(chunk)
      raise IOError, "stream is closed for writing" if @write_closed

      @request.write_chunk(chunk)
      chunk.bytesize
    end

    def <<(chunk)
      write(chunk)
      self
    end

    def flush
      self
    end

    def close_read
      @read_closed = true
      nil
    end

    def close_write
      return if @write_closed

      @write_closed = true
      @request.finish
      nil
    end

    def close
      close_read
      close_write
    end

    def closed?
      @read_closed && @write_closed
    end
  end
end
