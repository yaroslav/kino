# frozen_string_literal: true

module Kino
  # A native WebSocket (experimental): set `env["rack.upgrade"]` to a handler
  # when `env["rack.upgrade?"]` is `:websocket`. Its callbacks (on_open,
  # on_message, on_drained, on_shutdown, on_close; each optional) run on the
  # worker that accepted the socket, one event at a time. A handle is frozen
  # and shareable, and works from any thread or Ractor. Needs `lanes true`;
  # doc/websockets.md is the guide.
  class WebSocket
    # @return [Integer] the socket's id, unique in the process
    attr_reader :id

    # The socket with this id, from any thread or Ractor; nil once it is gone.
    def self.[](id) = (new(id) if Native.ws_registered(id))

    private_class_method :new

    def initialize(id)
      @id = id
      freeze
    end

    def ==(other) = other.is_a?(WebSocket) && other.id == @id

    alias_method :eql?, :==

    def hash = @id.hash

    # Send a text message; false once the socket is gone or its queue is full.
    def write(data) = Native.ws_write(@id, data)

    # Send a binary message; false as for #write.
    def write_binary(data) = Native.ws_write_binary(@id, data)

    # Send bytes that already form complete WebSocket frames.
    def write_raw(frames) = Native.ws_write_raw(@id, frames)

    # Close after what is queued; on_close follows, and the client has 5 seconds
    # to answer. The first close stands. False once gone, or on a full queue (then #abort).
    # @raise [ArgumentError] for a code outside 1000-1003, 1007-1011 and 3000-4999
    def close(code = 1000) = Native.ws_close(@id, code)

    # Drop the connection without a close frame; on_close follows.
    def abort = Native.ws_abort(@id)

    # Data frames queued and not yet written (a #write_raw counts once); 0 once gone.
    def pending = Native.ws_pending(@id)

    # Receive `topic`'s publishes as `prefix + body + suffix` frames; a full queue drops the
    # socket (`when_full: :close`) or skips the message (`:skip`). False once the socket is gone.
    # @raise [ArgumentError] for a `when_full` other than :close or :skip
    def subscribe(topic, prefix: "", suffix: "", when_full: :close)
      unless %i[close skip].include?(when_full)
        raise ArgumentError, "when_full must be :close or :skip, not #{when_full.inspect}"
      end

      Native.ws_subscribe(@id, topic, prefix, suffix, when_full == :skip)
    end

    # Every subscription to `topic`, or those made with `prefix`; false if none.
    def unsubscribe(topic, prefix: nil) = Native.ws_unsubscribe(@id, topic, prefix)

    # Send `body` to `topic`'s subscribers from any thread or Ractor, as text (valid UTF-8) or
    # with `binary: true`; asynchronous, false only when the fan-out queue is full.
    # @raise [ArgumentError] for a text `body` that is not UTF-8
    def self.publish(topic, body, binary: false) = Native.ws_publish(topic, body, binary)

    # @private
    module Events
      HANDLERS = :kino_websockets
      CALLBACKS = Ractor.make_shareable({message: "on_message", drained: "on_drained",
        shutdown: "on_shutdown", close: "on_close"})

      module_function

      # Kept in a thread variable of the accepting worker thread; returns what on_open raised.
      def open(id, handler)
        ws = WebSocket.__send__(:new, id)
        thread = Thread.current
        sockets = thread.thread_variable_get(HANDLERS) || thread.thread_variable_set(HANDLERS, {})
        sockets[id] = [handler, ws]
        handler.on_open(ws) if handler.respond_to?(:on_open)
        nil
      rescue => e
        failed(e, id, "on_open")
        e
      end

      # A handled message returns its credit; a finished on_shutdown lets the drain close it.
      def dispatch(event, on_error)
        id, kind, data = event
        sockets = Thread.current.thread_variable_get(HANDLERS) or return
        if kind == :message
          handler, ws = sockets[id]
          handler.on_message(ws, data) if handler.respond_to?(:on_message)
        else
          # The socket's last event, on_close, forgets its handler.
          handler, ws = (kind == :close) ? sockets.delete(id) : sockets[id]
          callback = CALLBACKS.fetch(kind)
          handler.public_send(callback, ws) if handler.respond_to?(callback)
        end
      rescue => e
        callback = CALLBACKS.fetch(kind)
        failed(e, id, callback)
        report(e, id, callback, on_error)
      ensure
        case kind
        when :message then Native.ws_handled(id)
        when :shutdown then Native.ws_shutdown_ran(id)
        end
      end

      # Logged with its backtrace; the socket is dropped (on_close follows), counted as a crash.
      def failed(error, id, callback)
        Log.failure("websocket ##{id} #{callback}", error)
        Native.ws_fail(id)
      end

      def report(error, id, callback, on_error)
        HookFire.fire(on_error, "on_error", error, {"kino.callback" => callback, "kino.websocket" => id}.freeze)
      end
    end
  end
end
