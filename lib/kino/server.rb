# frozen_string_literal: true

module Kino
  # Public server API. All network I/O lives in Rust (tokio + hyper); this
  # class only manages lifecycle and the Ruby worker pool.
  #
  # Topology is Puma-style two-level: `workers` ractors × `threads` threads
  # per ractor in :ractor mode; the same total capacity flattened onto plain
  # Threads in :threaded mode (which runs ANY Rack app, Rails included).
  class Server
    # @return [Integer, nil] the bound port (nil until #start; the actual
    #   port when configured with port 0)
    attr_reader :port

    # @return [Symbol] the resolved dispatch mode, :ractor or :threaded
    attr_reader :mode

    # @return [String] the bind address
    attr_reader :bind

    # @return [Boolean] whether TLS termination is configured
    def tls?
      !@tls.nil?
    end

    # Settings precedence: explicit kwargs > config_file DSL > defaults.
    #
    # @param app [#call] a Rack 3 application
    # @param config_file [String, nil] path to a kino.rb config file
    # @param options [Hash] any {Kino::Configuration} setting, e.g. port:,
    #   workers:, threads:, mode:, request_timeout:, tls: cert/key Hash
    # @example
    #   Kino::Server.new(app, config_file: "kino.rb", port: 3000)
    def initialize(app, config_file: nil, **options)
      config = Configuration.new
      config.load_file(config_file) if config_file
      config.merge!(options)
      settings = config.to_h

      @app = app
      @bind = settings[:bind]
      @requested_port = settings[:port]
      @workers = Integer(settings[:workers])
      @mode = resolve_mode(settings[:mode])
      # Default threads per mode: 1 in :ractor (threads inside a ractor
      # share its lock; a measured +17% on fast handlers; raise `workers`
      # for I/O concurrency instead), 3 in :threaded (threads ARE the
      # concurrency there).
      @threads = Integer(settings[:threads] || ((@mode == :ractor) ? 1 : 3))
      @queue_depth = Integer(settings[:queue_depth])
      @queue_timeout_ms = (Float(settings[:queue_timeout]) * 1000).round
      @request_timeout_ms = settings[:request_timeout] ? (Float(settings[:request_timeout]) * 1000).round : 0
      @batch = [Integer(settings[:batch]), 1].max
      @lanes = !!settings[:lanes]
      @log_requests = !!settings[:log_requests]
      @shutdown_timeout = settings[:shutdown_timeout]
      @tokio_threads = settings[:tokio_threads]
      @tls = validate_tls(settings[:tls])
      @pidfile = settings[:pidfile]
      @worker_threads = []
      @supervisor = nil
      @started = false
    end

    # Bind, boot the native front-end, and spawn the worker pool.
    # @return [self]
    # @raise [Kino::Error] when already started
    # @raise [Kino::UnshareableAppError] in forced :ractor mode with an
    #   unshareable app
    def start
      raise Error, "server already started" if @started

      @id, @port = Native.server_start(
        bind: @bind, port: @requested_port,
        queue_depth: @queue_depth, queue_timeout_ms: @queue_timeout_ms,
        request_timeout_ms: @request_timeout_ms,
        tokio_threads: @tokio_threads,
        tls_cert: @tls&.fetch(:cert), tls_key: @tls&.fetch(:key),
        lanes: @lanes, log_requests: @log_requests
      )
      File.write(@pidfile, "#{Process.pid}\n") if @pidfile
      if @mode == :ractor
        @supervisor = RactorSupervisor.new(@id, @app, workers: @workers, threads: @threads, batch: @batch).start
      else
        @worker_threads = (@workers * @threads).times.map do
          worker_id = Native.register_worker(@id)
          Thread.new { Worker.run(@id, worker_id, @app, @batch) }
        end
      end
      @started = true
      self
    end

    # Graceful shutdown: stop accepting, drain in-flight work up to the
    # deadline, then escalate: abort remaining clients (500), interrupt
    # blocked workers, kill stragglers; and tear down the runtime. Always
    # returns by ~deadline + a small epsilon; idempotent.
    #
    # @param timeout [Numeric, nil] drain deadline in seconds (default:
    #   the configured shutdown_timeout)
    # @return [nil]
    def shutdown(timeout: nil)
      return unless @started

      deadline = monotonic_now + (timeout || @shutdown_timeout)
      Native.stop_accepting(@id)

      # Drain: wait for queued + in-flight to reach zero, bounded by deadline.
      until monotonic_now >= deadline
        queued, in_flight = Native.queue_stats(@id)
        break if queued.zero? && in_flight.zero?

        sleep 0.01
      end

      # Idle workers see the closed queue and exit their loops.
      Native.close_queue(@id)
      join_workers(deadline)

      unless workers_done?
        # Past the deadline with stuck handlers: free the clients first,
        # then try to unblock and reap the workers.
        Native.abort_all_inflight(@id)
        Native.interrupt_all_workers(@id)
        join_workers(monotonic_now + 0.2)
        kill_stragglers
      end

      Native.shutdown_runtime(@id, 1_000)
      @worker_threads.clear
      @started = false
      File.delete(@pidfile) if @pidfile && File.exist?(@pidfile)
      nil
    end

    # Block until every worker has exited (i.e. until shutdown).
    # @return [void]
    def wait
      @supervisor ? @supervisor.join : @worker_threads.each(&:join)
    end

    # Production entry point: start, print the banner, trap INT/TERM for
    # graceful shutdown (second signal force-exits), block until done.
    # The `kino` CLI funnels into this too (CLI#serve).
    #
    # @param app [#call] a Rack 3 application
    # @param opts [Hash] see #initialize
    # @return [Kino::Server] the (stopped) server, after shutdown
    def self.run(app, **opts)
      server = new(app, **opts)
      CLI.opening_credits
      server.start
      CLI.action!(server)
      CLI.fin_at_exit
      trap_signals(server)
      server.wait
      server
    end

    # Signal handling shared by Server.run and the kino CLI: INT/TERM drain
    # gracefully (a second signal force-exits), USR1 prints a stats line.
    #
    # @param server [Kino::Server]
    # @return [void]
    def self.trap_signals(server)
      # kill -USR1 <pid> prints a one-line stats snapshot (find the pid in
      # the pidfile when configured).
      trap("USR1") do
        Thread.new { $stdout.puts Kino::CLI.stats_line(server.stats) }
      end
      signaled = false
      %w[INT TERM].each do |signal|
        trap(signal) do
          Process.exit!(1) if signaled
          signaled = true
          $stderr.write("Kino: draining (signal again to force exit)\n")
          # Trap context forbids mutexes; do the real work on a thread.
          Thread.new { server.shutdown }
        end
      end
    end

    # Live snapshot. Counters come from the native layer (one relaxed
    # atomic per request); config echo makes the line self-describing.
    #
    # @return [Hash{Symbol => Object}] mode, lanes, workers, threads,
    #   batch, respawns; plus queued, in_flight, served, rejected,
    #   timeouts (and lane_depths in lanes mode) once started
    def stats
      base = {
        mode: @mode, lanes: @lanes, workers: @workers, threads: @threads,
        batch: @batch, respawns: @supervisor ? @supervisor.respawns : 0
      }
      return base unless @started

      queued, in_flight, served, rejected, timeouts, lane_depths = Native.server_stats(@id)
      base.merge!(queued:, in_flight:, served:, rejected:, timeouts:)
      base[:lane_depths] = lane_depths if lane_depths
      base
    end

    private

    def validate_tls(tls)
      return nil if tls.nil?
      unless tls.is_a?(Hash) && tls[:cert] && tls[:key]
        raise ArgumentError, "tls: expects { cert:, key: } (file paths or inline PEM)"
      end

      {cert: String(tls[:cert]), key: String(tls[:key])}
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def join_workers(deadline)
      if @supervisor
        @supervisor.shutdown([deadline - monotonic_now, 0].max)
      else
        @worker_threads.each do |thread|
          thread.join([deadline - monotonic_now, 0.01].max)
        end
      end
    end

    def workers_done?
      if @supervisor
        @supervisor.done?
      else
        @worker_threads.none?(&:alive?)
      end
    end

    def kill_stragglers
      if @supervisor
        # Ractors cannot be force-killed; their clients were already freed
        # by abort_all_inflight. The stuck ractor leaks until process exit.
        Native.log_error("shutdown deadline passed with stuck ractor workers") unless @supervisor.done?
      else
        @worker_threads.each { |thread| thread.kill if thread.alive? }
      end
    end

    # Policy (mode resolution): when is an app safe for ractor
    # dispatch, and how loudly do we fall back? Current policy: trust
    # Ractor.shareable? on :auto with a stderr warning on fallback; forcing
    # :ractor with an unshareable app is an error, and we never
    # make_shareable the user's app behind their back (deep-freezing
    # someone's object graph is not a server's call to make).
    def resolve_mode(requested)
      case requested
      when :threaded
        :threaded
      when :ractor
        unless Ractor.shareable?(@app)
          raise UnshareableAppError,
            "mode: :ractor requires a Ractor-shareable app (frozen middleware, " \
            "Ractor.shareable_proc endpoints); try Ractor.make_shareable(app) " \
            "or mode: :threaded"
        end
        :ractor
      when :auto
        if Ractor.shareable?(@app)
          :ractor
        else
          warn "Kino: app is not Ractor-shareable; falling back to mode: :threaded"
          :threaded
        end
      else
        raise ArgumentError, "mode must be :auto, :ractor, or :threaded (got #{requested.inspect})"
      end
    end
  end
end
