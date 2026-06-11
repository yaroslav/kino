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

    def run(server_id, worker_id, app, batch_size = 1)
      if batch_size <= 1
        env = Native.take_one(server_id, worker_id)
        env = handle_one(env, server_id, worker_id, app) while env
      else
        batch = Native.take_batch(server_id, worker_id, batch_size)
        batch = process(batch, server_id, worker_id, app, batch_size) while batch
      end
    end

    # serve() returns this when the response did NOT ride a fused
    # respond-and-take (streaming body or app error) and the caller must
    # take the next request itself. Frozen: worker ractors read it.
    NOT_FUSED = Object.new.freeze

    # Handle one request; returns the next env (fused take) or nil.
    def handle_one(env, server_id, worker_id, app)
      result = serve(env, app) do |request, status, headers, chunks|
        request.respond_and_take_one(server_id, worker_id, status, headers, chunks)
      end
      result.equal?(NOT_FUSED) ? Native.take_one(server_id, worker_id) : result
    end

    # Handle every env in the batch; returns the next batch (the last
    # simple response rides the fused respond_and_take) or nil on shutdown.
    def process(batch, server_id, worker_id, app, batch_size)
      last = batch.size - 1
      batch.each_with_index do |env, index|
        result = serve(env, app) do |request, status, headers, chunks|
          if index == last
            request.respond_and_take(server_id, worker_id, batch_size,
              status, headers, chunks)
          else
            request.send_simple(status, headers, chunks)
            NOT_FUSED
          end
        end
        return result if index == last && !result.equal?(NOT_FUSED)
      end
      Native.take_batch(server_id, worker_id, batch_size)
    end

    # Run one request through the app. Complete bodies are yielded so the
    # caller picks plain vs fused delivery (the block's return value passes
    # through after the body is closed); streaming bodies are delivered
    # here and return NOT_FUSED. App errors must never kill the worker;
    # hard crashes (Exception) are the supervisor's job; and `abort` does
    # the right thing whether or not the response head already went out.
    def serve(env, app)
      request = env[KINO_REQUEST]
      env[RACK_INPUT] ||= Input.new(request)
      status, headers, body = app.call(env)

      if body.respond_to?(:to_ary)
        result = yield(request, status.to_i, headers, join_chunks(body.to_ary))
        body.close if body.respond_to?(:close)
        result
      else
        deliver_streaming(request, status.to_i, headers, body, env[RACK_INPUT])
        NOT_FUSED
      end
    rescue => e
      Native.log_error("#{e.class}: #{e.message}")
      request.abort
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
        begin
          body.each { |chunk| request.write_chunk(chunk) }
        ensure
          request.finish
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

    private_class_method :handle_one, :process, :serve, :deliver_streaming,
      :join_chunks
  end
end
