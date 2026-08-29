# frozen_string_literal: true

module Kino
  # @private
  # The request loop. Identical for threaded and ractor modes.
  #
  # The default (batch 1) hot path allocates one Hash per request: the env
  # arrives with the native request handle embedded under "kino.request",
  # and the common complete-body response rides the fused
  # respond_and_take_one call: ~one FFI crossing per request, no arrays.
  #
  # batch > 1 trades fairness for throughput: a worker grabs up to that
  # many already-queued requests per crossing, adding head-of-line blocking
  # behind slow handlers and stretching effective queue depth.
  module Worker
    RACK_INPUT = "rack.input"
    KINO_REQUEST = "kino.request"

    module_function

    def run(server_id, worker_id, app, batch_size = 1, hooks = nil)
      fire_after_worker_boot(hooks, worker_id)
      # One resolution of (server, worker) into a native handle; every take
      # after this skips the registry lookup. nil = server already gone.
      worker = Native.worker(server_id, worker_id)
      return unless worker
      if batch_size <= 1
        env = worker.take_one
        env = handle_one(env, worker, app, hooks) while env
      else
        batch = worker.take_batch(batch_size)
        batch = process(batch, worker, app, batch_size, hooks) while batch
      end
    end

    # serve() returns this when the response did NOT ride a fused
    # respond-and-take (streaming body or app error) and the caller must
    # take the next request itself. Frozen: worker ractors read it.
    NOT_FUSED = Object.new.freeze

    # Handle one request; returns the next env (fused take) or nil.
    def handle_one(env, worker, app, hooks)
      result = serve(env, app, hooks) do |request, status, headers, chunks|
        request.respond_and_take_one(worker, status, headers, chunks)
      end
      result.equal?(NOT_FUSED) ? worker.take_one : result
    end

    # Handle every env in the batch; returns the next batch (the last
    # simple response rides the fused respond_and_take) or nil on shutdown.
    def process(batch, worker, app, batch_size, hooks)
      last = batch.size - 1
      batch.each_with_index do |env, index|
        result = serve(env, app, hooks) do |request, status, headers, chunks|
          if index == last
            request.respond_and_take(worker, batch_size, status, headers, chunks)
          else
            request.send_simple(status, headers, chunks)
            NOT_FUSED
          end
        end
        return result if index == last && !result.equal?(NOT_FUSED)
      end
      worker.take_batch(batch_size)
    end

    # Run one request through the app. Complete bodies are yielded so the
    # caller picks plain vs fused delivery (the block's return value passes
    # through after the body is closed); streaming bodies are delivered
    # here and return NOT_FUSED. App errors must never kill the worker;
    # hard crashes (Exception) are the supervisor's job; and `abort` does
    # the right thing whether or not the response head already went out.
    def serve(env, app, hooks)
      request = env[KINO_REQUEST]
      env[RACK_INPUT] ||= Input.new(request)
      if hooks&.access_timing
        # The access log's breakdown: the VM's cumulative GC time and
        # allocation count, differenced around the app call.
        gc_before = GC.total_time
        allocated_before = GC.stat(:total_allocated_objects)
        status, headers, body = app.call(env)
        request.timing(GC.total_time - gc_before, GC.stat(:total_allocated_objects) - allocated_before)
      else
        status, headers, body = app.call(env)
      end

      if body.respond_to?(:to_ary)
        chunks = join_chunks(body.to_ary)
        if hooks&.after_request_complete
          # Hook set: do not fuse. Send the complete response, fire the hook
          # after it is out, then signal the caller to take the next request
          # separately (so the hook never waits on the next request).
          request.send_simple(status.to_i, headers, chunks)
          body.close if body.respond_to?(:close)
          fire_after_request_complete(hooks, env, status.to_i)
          NOT_FUSED
        else
          # No hook: fused fast path, unchanged, zero cost.
          result = yield(request, status.to_i, headers, chunks)
          body.close if body.respond_to?(:close)
          result
        end
      else
        deliver_streaming(request, status.to_i, headers, body, env[RACK_INPUT])
        fire_after_request_complete(hooks, env, status.to_i)
        NOT_FUSED
      end
    rescue => e
      # Abort before the hook: the client's 500 must never wait on a
      # reporting round-trip. The hook is the app's only window onto
      # delivery errors (they happen after app.call returned, so no
      # middleware can see them); its own failures are logged, not raised,
      # because nothing may escape this block and kill the worker.
      Log.exception(e, env)
      request.abort
      HookFire.fire(hooks&.on_error, "on_error", e, env)
      NOT_FUSED
    end

    def deliver_streaming(request, status, headers, body, input)
      request.send_headers(status, headers)
      if body.respond_to?(:call) && !body.respond_to?(:each)
        # Rack 3 streaming body: the app drives a full-duplex stream whose
        # read side is the request's existing rack.input (a fresh Input
        # here would strand anything the app already buffered from it).
        stream = Stream.new(request, input)
        begin
          body.call(stream)
        ensure
          stream.close
        end
      else
        # Enumerable body: chunked transfer unless the app set content-length.
        # finish only on success: a body that raised must abort the
        # connection (serve's rescue), not fake a clean end of stream that
        # the client cannot tell from a complete response.
        begin
          body.each { |chunk| request.write_chunk(chunk) }
          request.finish
        ensure
          body.close if body.respond_to?(:close)
        end
      end
    end

    def join_chunks(chunks)
      # Single-chunk bodies (the common case) skip the join copy entirely:
      # the native layer reads raw bytes, so encoding doesn't matter.
      return chunks.first || "" if chunks.size <= 1

      joined = (+"").force_encoding(Encoding::BINARY)
      chunks.each { |chunk| joined << chunk.b }
      joined
    end

    def fire_after_worker_boot(hooks, worker_id)
      HookFire.fire(hooks&.after_worker_boot, "after_worker_boot", worker_id)
    end

    def fire_after_request_complete(hooks, env, status)
      HookFire.fire(hooks&.after_request_complete, "after_request_complete", env, status)
    end

    private_class_method :handle_one, :process, :serve, :deliver_streaming,
      :join_chunks, :fire_after_worker_boot, :fire_after_request_complete
  end
end
