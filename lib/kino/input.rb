# frozen_string_literal: true

module Kino
  # @private
  # rack.input: forward-only reader over the native streaming body.
  # Rack 3 dropped the rewindability requirement, which is exactly what makes
  # streaming legal here. All output is binary, per spec.
  # Reads take from an offset into the pulled bytes: no copy or slice per read.
  class Input
    CHUNK_SIZE = 65_536

    def initialize(request)
      @request = request
      @buffer = (+"").force_encoding(Encoding::BINARY)
      @pos = 0
      @eof = false
    end

    # IO#read semantics, as Rack::Lint enforces them:
    #   read         -> String ("" at EOF)
    #   read(n)      -> String of up to n bytes, nil at EOF
    #   read(n, buf) -> fills buf, returns it (or nil at EOF)
    def read(length = nil, buffer = nil)
      if length.nil?
        fill_all
        length = available
      else
        fill(length)
        if available.zero? && length.positive?
          buffer&.clear
          return nil
        end
      end

      length = available if length > available
      piece =
        if buffer
          buffer.force_encoding(Encoding::BINARY).bytesplice(0, buffer.bytesize, @buffer, @pos, length)
        else
          @buffer.byteslice(@pos, length)
        end
      @pos += length
      piece
    end

    def gets
      fill_until_newline
      return nil if available.zero?

      index = @buffer.byteindex("\n", @pos)
      read(index ? index - @pos + 1 : available)
    end

    def each
      while (chunk = read(CHUNK_SIZE))
        yield chunk
      end
    end

    def close
      nil
    end

    private

    def available = @buffer.bytesize - @pos

    # A native chunk is a fresh binary string: with nothing left over, it becomes the buffer.
    def pull
      return if @eof

      chunk = @request.read_some(CHUNK_SIZE)
      return @eof = true unless chunk

      if available.zero?
        @buffer = chunk
      else
        @buffer = @buffer.byteslice(@pos, available) if @pos.positive?
        @buffer << chunk
      end
      @pos = 0
    end

    def fill(length)
      pull while available < length && !@eof
    end

    def fill_all
      pull until @eof
    end

    def fill_until_newline
      pull until @buffer.byteindex("\n", @pos) || @eof
    end
  end
end
