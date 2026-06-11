# frozen_string_literal: true

module Kino
  # @private
  # rack.input: forward-only reader over the native streaming body.
  # Rack 3 dropped the rewindability requirement, which is exactly what makes
  # streaming legal here. All output is binary, per spec.
  class Input
    CHUNK_SIZE = 65_536

    def initialize(request)
      @request = request
      @buffer = (+"").force_encoding(Encoding::BINARY)
      @eof = false
    end

    # IO#read semantics, as Rack::Lint enforces them:
    #   read         -> String ("" at EOF)
    #   read(n)      -> String of up to n bytes, nil at EOF
    #   read(n, buf) -> fills buf, returns it (or nil at EOF)
    def read(length = nil, buffer = nil)
      out = buffer ? buffer.clear.force_encoding(Encoding::BINARY) : (+"").force_encoding(Encoding::BINARY)

      if length.nil?
        fill_all
        out << @buffer
        @buffer.clear
        return out
      end

      fill(length)
      return nil if @buffer.empty? && length.positive?

      out << @buffer.slice!(0, length)
      out
    end

    def gets
      fill_until_newline
      return nil if @buffer.empty?

      index = @buffer.index("\n")
      index ? @buffer.slice!(0..index) : @buffer.slice!(0, @buffer.bytesize)
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

    def pull
      return if @eof

      chunk = @request.read_body(CHUNK_SIZE)
      chunk ? @buffer << chunk : @eof = true
    end

    def fill(length)
      pull while @buffer.bytesize < length && !@eof
    end

    def fill_all
      pull until @eof
    end

    def fill_until_newline
      pull until @buffer.index("\n") || @eof
    end
  end
end
