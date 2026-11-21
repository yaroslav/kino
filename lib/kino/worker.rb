# frozen_string_literal: true

require "socket"

module Kino
  module Native
    # @private
    # The request handle is its own rack.hijack callable: calling it takes the connection over.
    class Request
      def call = UNIXSocket.for_fd(hijack).tap { Kino::Worker::Loop.switched(self) }

      # Up to `max` bytes of the body, nil at its end; cooperative under a fiber scheduler.
      def read_some(max) = Fiber.current_scheduler ? cooperatively { read_body_nonblock(max) } : read_body(max)

      # All of `chunk`, onto the response stream, the way read_some waits.
      def write_all(chunk) = Fiber.current_scheduler ? cooperatively { write_chunk_nonblock(chunk) } : write_chunk(chunk)

      private

      # Every request's fiber shares the thread: never block it. While the client is slower than
      # the app, wait until the native side rings the descriptor it handed out.
      def cooperatively
        while (step = yield).is_a?(Integer)
          wait_until_rung(step)
        end
        step
      end

      # The request's descriptor, open until #waited: this IO must never close it, and is made
      # afresh each wait, as the number may change.
      def wait_until_rung(fd)
        IO.for_fd(fd, autoclose: false).wait_readable
      ensure
        waited
      end
    end
  end

  # A worker's wake hook: work on the worker's own thread (::on_wake). Needs lanes.
  # An instance is one worker thread's request loop (private API).
  class Worker
    RACK_INPUT = "rack.input"
    RACK_UPGRADE = "rack.upgrade"
    RACK_HIJACK_OFFERED = "rack.hijack?"
    RACK_HIJACK = "rack.hijack"
    KINO_REQUEST = "kino.request"
    # Thread variables, so fibers on the thread see them: the native handle, the wake hook, the Loop.
    HANDLE = :kino_worker
    HOOK = :kino_wake_hook
    CUSTOM_LOOP = :kino_worker_loop
    WAKE = :wake
    WAKE_ENV = Ractor.make_shareable({"kino.callback" => "on_wake"})
    # What a worker without hooks runs with.
    NO_HOOKS = Ractor.make_shareable(
      WorkerHooks.new(on_error: nil, after_worker_boot: nil, after_request_complete: nil, worker_loop: nil,
        access_timing: false)
    )
    private_constant :RACK_INPUT, :RACK_UPGRADE, :RACK_HIJACK_OFFERED, :RACK_HIJACK, :KINO_REQUEST, :HANDLE, :HOOK,
      :CUSTOM_LOOP, :WAKE, :WAKE_ENV, :NO_HOOKS

    # Wakes one worker's hook, from any thread or Ractor (frozen, shareable); returns at once.
    class Waker
      # @private
      def initialize(server_id, worker_id, generation)
        @server_id = server_id
        @worker_id = worker_id
        @generation = generation
        freeze
      end

      # @return [Boolean] false once the worker has exited or is retiring, or without lanes
      def wake = Native.wake_worker(@server_id, @worker_id, @generation)
    end

    # Run `block` on this worker's thread, between requests and socket events, whenever the
    # returned Waker is used; wakes coalesce, a self-waking hook alternates with waiting requests,
    # and calling again replaces the hook. A raising hook goes to on_error, as on_wake's callback.
    # Under a worker_loop it runs as a job of its own, alongside requests, one run at a time.
    # @raise [ArgumentError] off a worker thread, or without a block
    # @return [Waker] wakes this worker
    def self.on_wake(&block)
      thread = Thread.current
      worker = thread.thread_variable_get(HANDLE)
      raise ArgumentError, "Kino::Worker.on_wake must be called on a worker thread" unless worker
      raise ArgumentError, "Kino::Worker.on_wake needs a block" unless block

      Log.warn(WAKE_WARNING) if !worker.lanes? && worker.claim_warning(WAKE_WARNING)
      thread.thread_variable_set(HOOK, block)
      Waker.new(*worker.identity)
    end

    # @private
    # The request loop, the same in both modes. At batch 1 a request costs one Hash and about one
    # FFI crossing (respond_and_take_one); batch > 1 trades fairness for fewer crossings.
    # With no request waiting, the answer's body closes before the worker waits (take_one).
    def self.run(server_id, worker_id, app, batch_size, hooks)
      new(server_id, worker_id, app, batch_size, hooks).run
    end

    # @private
    def initialize(server_id, worker_id, app, batch_size, hooks)
      @server_id = server_id
      @worker_id = worker_id
      @app = app
      @batch_size = batch_size
      @hooks = hooks || NO_HOOKS
    end

    # @private
    def run
      # One resolution of (server, worker) into a native handle; every take
      # after this skips the registry lookup. nil = server already gone.
      # Resolved before after_worker_boot, so that hook can call on_wake.
      @worker = Native.worker(@server_id, @worker_id)
      Thread.current.thread_variable_set(HANDLE, @worker)
      HookFire.fire(@hooks.after_worker_boot, "after_worker_boot", @worker_id)
      take_until_drained if @worker
      ended = true
    ensure
      # Short of its loop's end the worker crashed, by an exception or a kill no rescue sees.
      # Nothing waits on a dead worker: its clients get a 500 now, its sockets a 1011.
      @worker&.crashed unless ended
      # The worker loop is over: its wakers report false from now on.
      @worker&.release
      Thread.current.thread_variable_set(HANDLE, nil)
      Thread.current.thread_variable_set(HOOK, nil)
      Thread.current.thread_variable_set(CUSTOM_LOOP, nil)
    end

    # serve() returns this when the response did NOT ride a fused
    # respond-and-take (streaming body or app error) and the caller must
    # take the next request itself, or when nothing waited for a fused take.
    NOT_FUSED = false
    private_constant :NOT_FUSED

    private

    # The worker's loop, Kino's or the app's (worker_loop), until its queues close or it retires.
    def take_until_drained
      if (custom = @hooks.worker_loop)
        served = Loop.new(self, @worker)
        Thread.current.thread_variable_set(CUSTOM_LOOP, served)
        custom.call(served)
        # Its lane would stay open to dispatch with no one taking.
        raise Error, "worker_loop returned before its worker was drained" unless served.drained?
      elsif @batch_size <= 1
        env = @worker.take_one
        env = handle_one(env) while env
      else
        batch = @worker.take_batch(@batch_size)
        batch = process(batch) while batch
      end
    end

    # Handle one take; returns the next env (fused take) or nil.
    def handle_one(env)
      unless env.is_a?(Hash)
        dispatch(env)
        return @worker.take_one
      end
      result = serve(env) do |request, status, headers, chunks|
        request.respond_and_take_one(@worker, status, headers, chunks)
      end
      result.equal?(NOT_FUSED) ? @worker.take_one : result
    end

    # Handle every take in the batch; returns the next batch (the last simple response
    # rides the fused respond_and_take) or nil on shutdown.
    def process(batch)
      last = batch.size - 1
      batch.each_with_index do |env, index|
        unless env.is_a?(Hash)
          dispatch(env)
          next
        end
        result = serve(env) do |request, status, headers, chunks|
          if index == last
            request.respond_and_take(@worker, @batch_size, status, headers, chunks)
          else
            request.send_simple(status, headers, chunks)
            NOT_FUSED
          end
        end
        return result if index == last && !result.equal?(NOT_FUSED)
      end
      @worker.take_batch(@batch_size)
    end

    def dispatch(item)
      item.equal?(WAKE) ? run_wake_hook : WebSocket::Events.dispatch(item, @hooks.on_error)
    end

    def run_wake_hook
      Thread.current.thread_variable_get(HOOK)&.call
    rescue => e
      Log.failure("on_wake hook", e)
      HookFire.fire(@hooks.on_error, "on_error", e, WAKE_ENV)
    end

    # Run one request through the app. Complete bodies are yielded so the
    # caller picks plain vs fused delivery (the block's return value passes
    # through after the body is closed); streaming bodies are delivered
    # here and return NOT_FUSED. App errors must never kill the worker;
    # hard crashes (Exception) are the supervisor's job; and `abort` does
    # the right thing whether or not the response head already went out.
    def serve(env)
      request = env[KINO_REQUEST]
      env[RACK_INPUT] ||= Input.new(request)
      if @hooks.access_timing
        # The access log's breakdown: the VM's cumulative GC time and
        # allocation count, differenced around the app call.
        gc_before = GC.total_time
        allocated_before = GC.stat(:total_allocated_objects)
        status, headers, body = @app.call(env)
        request.timing(GC.total_time - gc_before, GC.stat(:total_allocated_objects) - allocated_before)
      else
        status, headers, body = @app.call(env)
      end
      if env[RACK_HIJACK_OFFERED]
        return NOT_FUSED if serve_hijacked(request, status, headers, body, env)
      end
      if (handler = env[RACK_UPGRADE])
        return NOT_FUSED if serve_upgrade(request, headers, body, handler, env)
      end

      if body.respond_to?(:to_ary) && !@hooks.after_request_complete
        # No hook: fused fast path, zero cost (an ensure runs inline). The body closes whatever the
        # fused call raised; a failed close is reported here, and what the take brought is served.
        begin
          yield(request, status.to_i, headers, join_chunks(body.to_ary))
        ensure
          begin
            body.close if body.respond_to?(:close)
          rescue => e
            Log.exception(e, env)
            HookFire.fire(@hooks.on_error, "on_error", e, env)
          end
        end
      else
        deliver_unfused(request, status.to_i, headers, body, env)
      end
    rescue => e
      # Abort before the hook: the client's 500 must never wait on a
      # reporting round-trip. The hook is the app's only window onto
      # delivery errors (they happen after app.call returned, so no
      # middleware can see them); its own failures are logged, not raised,
      # because nothing may escape this block and kill the worker.
      Log.exception(e, env)
      request.abort
      HookFire.fire(@hooks.on_error, "on_error", e, env)
      NOT_FUSED
    end

    # A complete answer sent apart from the take (a hook is set, which never waits on the next
    # request), or a streamed one.
    def deliver_unfused(request, status, headers, body, env)
      closing_reported(body, request, env) do
        if body.respond_to?(:to_ary)
          request.send_simple(status, headers, join_chunks(body.to_ary))
        else
          deliver_streaming(request, status, headers, body, env[RACK_INPUT])
        end
      end
      after_request_complete(env, status)
      NOT_FUSED
    end

    # What an app offered rack.hijack did with it: true once it took the connection over, in full
    # or with a callable; nil when it answers as any other request.
    def serve_hijacked(request, status, headers, body, env)
      # Taken over with rack.hijack: the app answers on the connection, and its triple is
      # ignored (Rack 3). The body still closes: middleware cleans up there.
      return closing(body) { true } if request.hijacked?
      partial = headers&.[](RACK_HIJACK)
      if status.to_i == 101 && (partial || (!env[RACK_UPGRADE] && streaming_body?(body)))
        closing(body) { switch_hijacked(request, headers, partial || body, body, env) }
      elsif partial
        closing(body) { hijack_partially(request, status, headers, partial, env) }
      end
    end

    # A native WebSocket for `handler`: true once Kino answered the handshake (its 101, a 500
    # or a 503), false when it cannot accept one and the app's response goes out.
    def serve_upgrade(request, headers, body, handler, env)
      id = accept_upgrade(request, headers, body, handler.respond_to?(:on_shutdown))
      if id
        # The 101 goes out only after on_open, so what it writes and subscribes is in place;
        # if on_open raised, the client gets a 500 before on_error hears of it.
        if (failure = Loop.opening(id) { WebSocket::Events.open(id, handler) })
          request.abort
          WebSocket::Events.report(failure, id, "on_open", @hooks.on_error)
        elsif request.switch_protocols
          after_request_complete(env, 101)
        end
      end
      return true unless id.nil?

      # Once per server, from the first worker that meets the cause.
      Log.warn(UPGRADE_WARNING) if !@worker.lanes? && @worker.claim_warning(UPGRADE_WARNING)
      false
    end

    # Accept a handshake, its 101 held for switch_protocols with the app's headers, and close
    # the body. Nil when Kino cannot (no handshake, no lanes): the app's response goes out.
    # False for Kino's 503 refusal. A body that fails to close drops the new socket (a 500).
    def accept_upgrade(request, headers, body, on_shutdown)
      id = request.upgrade_websocket(headers || {}, on_shutdown)
      closing(body) unless id.nil?
      id
    rescue
      closing(body) if id.nil?
      Native.ws_abort(id) if id
      raise
    end

    # Partial hijack (Rack 3): the rack.hijack callable gets the response stream after the head.
    def hijack_partially(request, status, headers, callable, env)
      deliver_streaming(request, status.to_i, headers.except(RACK_HIJACK), callable, env[RACK_INPUT])
      after_request_complete(env, status.to_i)
      true
    end

    # A 101 with a callable (a partial hijack, or a Rack 3 streaming body): the callable gets
    # the switched connection as a socket. Kino closes it once a streaming body returns.
    def switch_hijacked(request, headers, callable, body, env)
      io = UNIXSocket.for_fd(request.hijack_switched(headers.except(RACK_HIJACK)))
      Loop.switched(request)
      begin
        callable.call(io)
      rescue
        io.close
        raise
      end
      io.close if callable.equal?(body) && !io.closed?
      after_request_complete(env, 101)
      true
    end

    # Runs the block, if any, then closes `body` however it ended.
    def closing(body)
      yield if block_given?
    ensure
      body.close if body.respond_to?(:close)
    end

    # The block, then `body` closed however it ended. A failed close raises into serve's rescue
    # after a block that returned; after one that raised, it is reported here, its 500 first.
    def closing_reported(body, request, env)
      yield
      done = true
    ensure
      begin
        body.close if body.respond_to?(:close)
      rescue => e
        raise if done
        request.abort
        Log.exception(e, env)
        HookFire.fire(@hooks.on_error, "on_error", e, env)
      end
    end

    def streaming_body?(body) = body.respond_to?(:call) && !body.respond_to?(:each)

    UPGRADE_WARNING = "the app set rack.upgrade, but native WebSockets need `lanes true`; " \
      "sending its own response instead"
    WAKE_WARNING = "the app registered a Kino::Worker.on_wake hook, but wakes need `lanes true`; " \
      "the hook will never run"
    private_constant :UPGRADE_WARNING, :WAKE_WARNING

    # Sends the head, then what `body` streams; closing the body is its caller's (Rack).
    def deliver_streaming(request, status, headers, body, input)
      request.send_headers(status, headers)
      if streaming_body?(body)
        # Rack 3 streaming body: the app drives a full-duplex stream whose
        # read side is the request's existing rack.input (a fresh Input
        # here would strand anything the app already buffered from it).
        # Its write side closes only on success; a raising body is cut by serve's abort.
        stream = Stream.new(request, input)
        begin
          body.call(stream)
        rescue
          stream.close_read
          raise
        end
        stream.close
      else
        # Enumerable body: chunked transfer unless the app set content-length.
        # finish only on success: a body that raised must abort the
        # connection (serve's rescue), not fake a clean end of stream that
        # the client cannot tell from a complete response.
        body.each { |chunk| request.write_all(chunk) }
        request.finish
      end
    rescue ClientGone
      # The client left mid-stream: the request is over, and nothing failed.
      nil
    end

    def join_chunks(chunks)
      # Single-chunk bodies (the common case) skip the join copy entirely:
      # the native layer reads raw bytes, so encoding doesn't matter.
      return chunks.first || "" if chunks.size <= 1

      joined = (+"").force_encoding(Encoding::BINARY)
      chunks.each { |chunk| joined << chunk.b }
      joined
    end

    def after_request_complete(env, status)
      HookFire.fire(@hooks.after_request_complete, "after_request_complete", env, status)
    end

    # @private
    # A worker_loop's jobs run these (Worker::Loop).
    public :serve, :dispatch
  end
end
