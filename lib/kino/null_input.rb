# frozen_string_literal: true

module Kino
  # @private
  # rack.input for requests that can carry no body (most GETs). A single
  # frozen, Ractor-shareable instance is set by the native layer directly
  # into the env, so bodyless requests allocate no input object at all.
  class NullInput
    EMPTY = String.new("", encoding: Encoding::BINARY).freeze

    # IO#read semantics at permanent EOF: read and read(0) return "",
    # read(n > 0) returns nil. Mirrors what Input does on an empty body,
    # since the native layer swaps the two classes invisibly per request.
    def read(length = nil, buffer = nil)
      empty = length.nil? || length.zero?
      if buffer
        buffer.clear.force_encoding(Encoding::BINARY)
        return empty ? buffer : nil
      end
      empty ? EMPTY : nil
    end

    def gets
      nil
    end

    def each
      # no chunks, ever
    end

    def close
      nil
    end

    INSTANCE = new.freeze
  end
end
