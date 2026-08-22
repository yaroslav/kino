# frozen_string_literal: true

module Kino
  # Server settings with Puma-style precedence:
  #   explicit Server.new kwargs > config file DSL > defaults.
  class Configuration
    # Every setting and its default; the full reference lives in the
    # generated sample config (`kino --init`).
    DEFAULTS = {
      bind: "127.0.0.1",
      port: 0,
      workers: nil, # resolved to Kino.available_parallelism in #to_h
      threads: nil, # resolved per mode in Server: 1 in :ractor, 3 in :threaded
      mode: :auto,
      queue_depth: 1024,
      queue_timeout: 5.0,
      request_timeout: nil,
      max_connections: nil, # nil = derive from the open-file limit
      max_body_size: 50 * 1024 * 1024, # 50 MB; nil/0 = unlimited
      batch: 1,
      lanes: false,
      log_requests: false,
      on_error: nil,
      after_boot: nil,
      after_worker_boot: nil,
      after_request_complete: nil,
      on_worker_exit: nil,
      shutdown_timeout: 30,
      tokio_threads: nil,
      tls: nil,
      environment: nil,
      pidfile: nil,
      control_bind: nil,
      control_token: nil,
      quarantine_timeout: nil,
      quarantine_max: nil,
      rackup: nil
    }.freeze

    # The known setting names.
    SETTINGS = DEFAULTS.keys.freeze

    # Source template for {.sample}.
    SAMPLE_TEMPLATE = File.expand_path("templates/kino.rb.tt", __dir__)

    # The fully-commented sample config (see `kino --init`).
    # @return [String]
    def self.sample
      File.read(SAMPLE_TEMPLATE)
    end

    # Write the sample config to +path+. Refuses to clobber an existing
    # file unless force: true.
    #
    # @param path [String]
    # @param force [Boolean] overwrite an existing file
    # @return [String] the path written
    # @raise [Kino::Error] when the file exists and force is false
    def self.write_sample(path, force: false)
      if File.exist?(path) && !force
        raise Error, "#{path} already exists (use force: true to overwrite)"
      end

      File.write(path, sample)
      path
    end

    def initialize
      @values = {}
    end

    # @param key [Symbol] a key from DEFAULTS
    # @return [Object] the explicit value, or the default
    def [](key)
      @values.fetch(key) { DEFAULTS.fetch(key) }
    end

    # @param key [Symbol] a key from DEFAULTS
    # @param value [Object]
    # @raise [ArgumentError] for unknown settings
    def set(key, value)
      raise ArgumentError, "unknown setting #{key.inspect}" unless DEFAULTS.key?(key)

      @values[key] = value
    end

    # @return [Boolean] whether the key was explicitly set
    def set?(key)
      @values.key?(key)
    end

    # Load a config file (Ruby DSL) into this configuration.
    # @param path [String]
    # @return [self]
    # @raise [Kino::Error] when the file does not exist
    def load_file(path)
      raise Error, "config file not found: #{path}" unless File.exist?(path)

      DSL.new(self).instance_eval(File.read(path), path, 1)
      self
    end

    # Explicit kwargs win over everything already set.
    # @param options [Hash{Symbol => Object}]
    # @return [self]
    def merge!(options)
      options.each { |key, value| set(key, value) }
      self
    end

    # @return [Hash{Symbol => Object}] every setting, defaults filled in
    def to_h
      SETTINGS.to_h { |key| [key, self[key]] }.tap do |h|
        h[:workers] ||= Kino.available_parallelism
      end
    end

    # The settings Server.new accepts: everything except the keys only the
    # CLI consumes (rackup file selection, RACK_ENV).
    # @return [Hash{Symbol => Object}]
    def server_options
      to_h.except(:rackup, :environment)
    end

    # The config-file DSL, deliberately Puma-shaped:
    #
    #   # kino.rb
    #   bind "0.0.0.0"
    #   port 9292
    #   workers 8           # ractors (or thread groups in :threaded mode)
    #   threads 3           # threads per worker
    #   mode :ractor        # :auto | :ractor | :threaded
    #   queue_depth 2048
    #   queue_timeout 0.5
    #   shutdown_timeout 15
    #   tokio_threads 4
    #   tls cert: "cert.pem", key: "key.pem"
    #
    # Every directive is documented in the generated sample config
    # (`kino --init`); the one-liners here only state the value each
    # directive expects.
    class DSL
      def initialize(config)
        @config = config
      end

      # Address to listen on: a host ("0.0.0.0" accepts non-local
      # connections), or "unix:///path/to.sock" for a unix domain socket
      # (then `port` is unused).
      def bind(host) = @config.set(:bind, host)

      # Port to listen on; 0 picks an ephemeral port.
      def port(port) = @config.set(:port, Integer(port))

      # Worker count (ractors in :ractor mode); defaults to CPU cores.
      def workers(count) = @config.set(:workers, Integer(count))

      # Threads per worker (I/O concurrency inside one ractor); default is
      # mode-dependent: 1 in :ractor mode, 3 in :threaded.
      def threads(count) = @config.set(:threads, Integer(count))

      # Dispatch mode: :auto, :ractor, or :threaded.
      def mode(mode) = @config.set(:mode, mode.to_sym)

      # Bounded request-queue depth; overflow earns clients a 503.
      def queue_depth(depth) = @config.set(:queue_depth, Integer(depth))

      # Seconds a request may wait for queue space before the 503.
      def queue_timeout(seconds) = @config.set(:queue_timeout, Float(seconds))

      # Seconds the app gets before the client receives a 504; nil = off.
      def request_timeout(seconds) = @config.set(:request_timeout, seconds && Float(seconds))

      # Max connections served at once; beyond it, new connections wait in
      # the kernel backlog. Defaults to most of the open-file limit.
      def max_connections(count) = @config.set(:max_connections, Integer(count))

      # Max request-body bytes before a 413; nil disables (delegate to a
      # fronting proxy). Default 50 MB.
      def max_body_size(bytes) = @config.set(:max_body_size, bytes && Integer(bytes))

      # Requests a worker may grab per queue visit (default 1).
      def batch(count) = @config.set(:batch, Integer(count))

      # EXPERIMENTAL per-worker lane dispatch.
      def lanes(enabled) = @config.set(:lanes, !!enabled)

      # Native access log: one status-colored line per request to stdout.
      def log_requests(enabled) = @config.set(:log_requests, !!enabled)

      # Called with (exception, env) when a worker catches an app or
      # delivery error; wire your error tracker here. Takes a callable
      # or a block. Must be Ractor-shareable in :ractor mode.
      def on_error(handler = nil, &block) = @config.set(:on_error, handler || block)

      # Called once on the main thread after the worker pool is up. The
      # readiness seam (wire sd_notify or a "server ready" metric here).
      def after_boot(handler = nil, &block) = @config.set(:after_boot, handler || block)

      # Called once inside each worker (a ractor in :ractor mode) before it
      # serves, with the worker's slot id. Must be Ractor-shareable in
      # :ractor mode (build it with Ractor.shareable_proc).
      def after_worker_boot(handler = nil, &block) = @config.set(:after_worker_boot, handler || block)

      # Called inside the worker after each successful response with
      # (env, status). Hot path: leave unset for zero cost. Must be
      # Ractor-shareable in :ractor mode.
      def after_request_complete(handler = nil, &block) = @config.set(:after_request_complete, handler || block)

      # Called on the main thread when a worker exits, with (worker_index,
      # error_or_nil). error is the crash cause, or nil on a clean exit.
      def on_worker_exit(handler = nil, &block) = @config.set(:on_worker_exit, handler || block)

      # Graceful-shutdown drain deadline in seconds.
      def shutdown_timeout(seconds) = @config.set(:shutdown_timeout, seconds)

      # Threads for the tokio (Rust I/O) runtime; default: one per core.
      def tokio_threads(count) = @config.set(:tokio_threads, Integer(count))

      # TLS termination; file paths or inline PEM strings.
      def tls(cert:, key:) = @config.set(:tls, {cert: cert, key: key})

      # Sets RACK_ENV (unless already set) before the CLI loads the app.
      def environment(env) = @config.set(:environment, env.to_s)

      # Write the master PID here on start.
      def pidfile(path) = @config.set(:pidfile, path.to_s)

      # Serve the read-only control plane (live stats as JSON at /stats,
      # Prometheus text at /metrics, /ready and /live probes) on this
      # address: "host:port" or "unix://path". Off unless set.
      def control_bind(addr) = @config.set(:control_bind, addr.to_s)

      # When set, /stats and /metrics require "Authorization: Bearer <token>".
      # The probes stay open; they carry no data.
      def control_token(token) = @config.set(:control_token, token.to_s)

      # Quarantine a dispatch slot whose current request has run longer
      # than this many seconds, spawning a replacement to restore capacity.
      # Off unless set. Set it above your slowest legitimate endpoint (and
      # typically above request_timeout).
      def quarantine_timeout(seconds)
        seconds &&= Float(seconds)
        if seconds && seconds <= 0
          raise ArgumentError, "quarantine_timeout must be greater than 0 (got #{seconds})"
        end
        @config.set(:quarantine_timeout, seconds)
      end

      # Cap on the total number of replacement events over the process
      # lifetime. Past the cap the monitor stops replacing and the server
      # runs at reduced capacity. Default: the worker count in :ractor
      # mode, workers x threads in :threaded.
      def quarantine_max(count)
        count = Integer(count)
        raise ArgumentError, "quarantine_max must be >= 1 (got #{count})" if count < 1
        @config.set(:quarantine_max, count)
      end

      # Rackup file the `kino` CLI loads (positional argument wins).
      def rackup(path) = @config.set(:rackup, path.to_s)
    end
  end
end
