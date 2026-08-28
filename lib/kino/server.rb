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

    # @return [Integer, nil] the control plane's TCP port (nil until #start,
    #   when the control plane is off, or for a unix-socket bind)
    attr_reader :control_port

    # @return [Symbol] the resolved dispatch mode, :ractor or :threaded
    attr_reader :mode

    # @return [String] the bind address
    attr_reader :bind

    # @return [Boolean] whether TLS termination is configured
    def tls?
      !@tls.nil?
    end

    # @return [Boolean] whether the bind is a unix domain socket
    #   ("unix:///path/to.sock")
    def unix?
      @bind.start_with?("unix://")
    end

    # Where the server listens, once started: `http://host:port`
    # (`https` under TLS), or the `unix://` socket path.
    # @return [String]
    def url
      unix? ? @bind : "http#{"s" if tls?}://#{@bind}:#{@port}"
    end

    # Where the control plane listens, once started, or nil when it is
    # off: `http://host:port`, or its `unix://` socket path.
    # @return [String, nil]
    def control_url
      return nil unless @control_bind
      return @control_bind if @control_bind.start_with?("unix://")

      "http://#{@control_bind.rpartition(":").first}:#{@control_port}"
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
      @on_error = validate_hook(settings[:on_error], :on_error)
      @after_worker_boot = validate_hook(settings[:after_worker_boot], :after_worker_boot)
      @after_request_complete = validate_hook(settings[:after_request_complete], :after_request_complete)
      @after_boot = validate_hook(settings[:after_boot], :after_boot)
      @on_worker_exit = validate_hook(settings[:on_worker_exit], :on_worker_exit)
      @mode = resolve_mode(settings[:mode])
      @worker_hooks = WorkerHooks.new(
        on_error: @on_error,
        after_worker_boot: @after_worker_boot,
        after_request_complete: @after_request_complete,
        # The access log's GC and allocation figures come from the VM's
        # process-wide counters, so they are measured only where one
        # request at a time can own them: the GVL serializes :threaded
        # mode, and a single ractor has nothing to race.
        access_timing: !!settings[:log_requests] && (@mode == :threaded || @workers == 1)
      )
      # Default threads per mode: 1 in :ractor (threads inside a ractor
      # share its lock; a measured +17% on fast handlers; raise `workers`
      # for I/O concurrency instead), 3 in :threaded (threads ARE the
      # concurrency there).
      @threads = Integer(settings[:threads] || ((@mode == :ractor) ? 1 : 3))
      @queue_depth = Integer(settings[:queue_depth])
      @queue_timeout_ms = (Float(settings[:queue_timeout]) * 1000).round
      @request_timeout_ms = settings[:request_timeout] ? (Float(settings[:request_timeout]) * 1000).round : 0
      @max_connections = settings[:max_connections] ? Integer(settings[:max_connections]) : default_max_connections
      @max_body_size = Integer(settings[:max_body_size] || 0)
      @batch = [Integer(settings[:batch]), 1].max
      @lanes = !!settings[:lanes]
      @log_requests = !!settings[:log_requests]
      @shutdown_timeout = settings[:shutdown_timeout]
      @io_shards = !!settings[:io_shards]
      @io_threads =
        if settings[:io_threads].nil?
          nil
        else
          Integer(settings[:io_threads])
        end
      if @io_threads && @io_threads < 1
        raise ArgumentError, "io_threads must be >= 1"
      end
      Log.warn("io_threads has no effect unless io_shards is true") if @io_threads && !@io_shards
      @tokio_threads = settings[:tokio_threads]
      @tls = validate_tls(settings[:tls])
      if @tls && unix?
        raise ArgumentError, "TLS is not supported on a unix socket bind; terminate TLS at the proxy in front"
      end
      @pidfile = settings[:pidfile]
      @control_bind = settings[:control_bind]&.to_s
      @control_token = settings[:control_token]&.to_s
      # An empty token (e.g. control_token ENV["KINO_CONTROL_TOKEN"] with the
      # var unset) must not half-disable auth: treat it as auth off, not as
      # "require a zero-length Bearer token".
      @control_token = nil if @control_token && @control_token.empty?
      @quarantine_timeout_ms = settings[:quarantine_timeout] ? (Float(settings[:quarantine_timeout]) * 1000).round : nil
      @quarantine_max =
        if settings[:quarantine_max]
          Integer(settings[:quarantine_max])
        elsif @mode == :ractor
          @workers
        else
          @workers * @threads
        end
      @worker_threads = []
      @worker_threads_lock = Mutex.new
      @supervisor = nil
      @quarantine_monitor = nil
      @started = false
    end

    # Bind, boot the native front-end, and spawn the worker pool.
    # @return [self]
    # @raise [Kino::Error] when already started
    # @raise [Kino::UnshareableAppError] in forced :ractor mode with an
    #   unshareable app
    def start
      raise Error, "server already started" if @started

      # Claim the pidfile before binding: refusing to start (another
      # instance is alive) must not leave a booted native runtime behind.
      write_pidfile if @pidfile
      booted = false
      begin
        @id, @port, @control_port = Native.server_start(
          bind: @bind, port: @requested_port,
          queue_depth: @queue_depth, queue_timeout_ms: @queue_timeout_ms,
          request_timeout_ms: @request_timeout_ms,
          max_connections: @max_connections,
          max_body_size: @max_body_size,
          io_shards: @io_shards,
          io_threads: @io_threads,
          tokio_threads: @tokio_threads,
          tls_cert: @tls&.fetch(:cert), tls_key: @tls&.fetch(:key),
          lanes: @lanes, log_requests: @log_requests,
          mode: @mode.to_s, workers: @workers, threads: @threads, batch: @batch,
          control_bind: @control_bind, control_token: @control_token
        )
        booted = true
      ensure
        remove_pidfile if @pidfile && !booted
      end
      # GC anchor for zero-copy response buffers: held for the server's
      # lifetime so in-flight buffers survive even a worker ractor crash.
      @pin_keeper = Native.pin_keeper(@id)
      if @mode == :ractor
        @supervisor = RactorSupervisor.new(@id, @app, workers: @workers, threads: @threads,
          batch: @batch, hooks: @worker_hooks, on_worker_exit: @on_worker_exit).start
      else
        @worker_threads = (@workers * @threads).times.map { spawn_worker_thread }
      end
      start_quarantine_monitor if @quarantine_timeout_ms
      Native.control_ready(@id)
      HookFire.fire(@after_boot, "after_boot")
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

      @quarantine_monitor&.stop
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
      # The control thread reports "draining" for the whole drain and stops
      # only now, once there is nothing left to report.
      Native.control_stop(@id)
      # The runtime is gone, so hyper has dropped every pinned buffer;
      # the keeper (and the strings it marked) may now be collected.
      @pin_keeper = nil
      @worker_threads.clear
      @started = false
      remove_pidfile if @pidfile
      nil
    end

    # Block until every worker has exited (i.e. until shutdown).
    # @return [void]
    def wait
      @supervisor ? @supervisor.join : @worker_threads.each(&:join)
    end

    # Production entry point: build the server and {#run} it. The `kino`
    # CLI funnels into this too (CLI#serve).
    #
    # @param app [#call] a Rack 3 application
    # @param opts [Hash] see #initialize
    # @return [Kino::Server] the (stopped) server, after shutdown
    def self.run(app, **opts)
      new(app, **opts).run
    end

    # Serve until shut down: start, print the banner, trap INT/TERM for
    # graceful shutdown (second signal force-exits), block until done.
    # The Rack handler calls this on a server it built itself.
    #
    # @return [self] after shutdown
    def run
      # Startup output must land immediately even when stdout is a pipe or
      # file (process supervisors, `kino > server.log`, `rails server`
      # under Docker); block buffering would hold the banner back until
      # exit.
      $stdout.sync = true
      CLI.opening_credits
      start
      CLI.action!(self)
      CLI.fin_at_exit
      self.class.trap_signals(self)
      wait
      self
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
        Thread.new { Log.info(CLI.stats_line(server.stats)) }
      end
      signaled = false
      %w[INT TERM].each do |signal|
        trap(signal) do
          Process.exit!(1) if signaled
          signaled = true
          Log.warn("draining (signal again to force exit)")
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
    #   timeouts, worker_status, quarantined, queue_time (and lane_depths in
    #   lanes mode) once started
    def stats
      base = {
        mode: @mode, lanes: @lanes, workers: @workers, threads: @threads,
        batch: @batch, respawns: 0
      }
      return base unless @started

      queued, in_flight, served, rejected, timeouts, respawns, lane_depths = Native.server_stats(@id)
      base.merge!(queued:, in_flight:, served:, rejected:, timeouts:, respawns:)
      base[:lane_depths] = lane_depths if lane_depths
      rows = Native.worker_stats(@id)
      base[:worker_status] = rows.map do |index, served, in_flight, busy_ms, quarantined|
        {index:, served:, in_flight:, busy_ms:, quarantined:}
      end
      base[:quarantined] = rows.count { |_index, _served, _in_flight, _busy_ms, quarantined| quarantined }
      count, sum_seconds = Native.queue_time(@id)
      base[:queue_time] = {count:, sum_seconds:}
      base
    end

    private

    # Register a fresh dispatch slot and run a worker thread on it; returns
    # the thread. Used at boot and by the quarantine replacer.
    def spawn_worker_thread
      worker_id = Native.register_worker(@id)
      Thread.new do
        # Named so log lines from inside say which worker spoke.
        Thread.current.name = "worker-#{worker_id}"
        error = nil
        begin
          Worker.run(@id, worker_id, @app, @batch, @worker_hooks)
        rescue Exception => e # rubocop:disable Lint/RescueException -- a hard crash in a threaded worker thread
          error = e
          raise
        ensure
          HookFire.fire(@on_worker_exit, "on_worker_exit", worker_id, error)
        end
      end
    end

    # Track a replacement thread spawned outside the initial pool assignment
    # (the quarantine replacer) so shutdown's join/done?/kill sweeps see it.
    def track_replacement_thread(thread)
      @worker_threads_lock.synchronize { @worker_threads << thread }
    end

    # @private
    # The :threaded-mode quarantine replacer: spawns a replacement worker
    # thread, quarantines the wedged slot, then tracks the new thread so
    # shutdown's join/done?/kill sweeps see it. Built from bound Method
    # objects instead of a server reference, so it drives the server
    # through those methods without send or instance_variable_get.
    class ThreadedReplacer
      def initialize(server_id:, spawner:, tracker:)
        @server_id = server_id
        @spawner = spawner
        @tracker = tracker
      end

      def replace(worker_id)
        thread = @spawner.call # spawn FIRST (may raise ThreadError)
        Native.quarantine_slot(@server_id, worker_id) # quarantine after success
        @tracker.call(thread)
        true
      end
    end
    private_constant :ThreadedReplacer

    # A replacer.replace(worker_id) spawns a replacement worker, then
    # quarantines the wedged slot, mode-appropriately. In :ractor the
    # supervisor is the replacer; in :threaded a small object over
    # spawn_worker_thread.
    def start_quarantine_monitor
      replacer = @supervisor || ThreadedReplacer.new(server_id: @id, spawner: method(:spawn_worker_thread),
        tracker: method(:track_replacement_thread))
      @quarantine_monitor = QuarantineMonitor.new(
        server_id: @id, timeout_ms: @quarantine_timeout_ms,
        max: @quarantine_max, replacer: replacer
      ).start
    end

    def validate_tls(tls)
      return nil if tls.nil?
      unless tls.is_a?(Hash) && tls[:cert] && tls[:key]
        raise ArgumentError, "tls: expects { cert:, key: } (file paths or inline PEM)"
      end

      {cert: String(tls[:cert]), key: String(tls[:key])}
    end

    def validate_hook(handler, name)
      return nil if handler.nil?
      unless handler.respond_to?(:call)
        raise ArgumentError, "#{name} must respond to #call (got #{handler.class})"
      end

      handler
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # Default connection cap: most of the process open-file limit. A
    # connection flood's failure mode is descriptor exhaustion, and in
    # :ractor/:threaded mode the app's own sockets and files share this
    # process's table, so leave headroom. Scales with `ulimit -n`; raise the
    # OS limit (or set max_connections) to allow more.
    def default_max_connections
      soft, = Process.getrlimit(Process::RLIMIT_NOFILE)
      return 65_536 if soft == Process::RLIM_INFINITY

      [soft * 8 / 10, 64].max
    end

    # Claim the pidfile for this process. O_EXCL creation fails on ANY
    # existing directory entry (regular file, symlink, even a dangling
    # one), so a live instance's pidfile is never overwritten and a
    # symlink is never followed. A leftover entry whose owner is gone is
    # replaced; one that does not hold a pid is refused, not clobbered.
    def write_pidfile
      claim_pidfile
    rescue Errno::EEXIST
      refuse_unless_stale
      begin
        # Unlink removes the entry itself; a symlink's target is untouched.
        File.unlink(@pidfile)
      rescue Errno::ENOENT
        # Vanished on its own; the claim below settles any remaining race.
      end
      begin
        claim_pidfile
      rescue Errno::EEXIST
        raise Error, "lost the race for #{@pidfile}: another instance is starting"
      end
    end

    def claim_pidfile
      File.open(@pidfile, File::WRONLY | File::CREAT | File::EXCL, 0o644) do |file|
        file.write("#{Process.pid}\n")
      end
    end

    # @raise [Kino::Error] when the pidfile's owner is still alive, or the
    #   file does not look like a pidfile at all
    def refuse_unless_stale
      content = begin
        File.read(@pidfile)
      rescue Errno::ENOENT
        return # already gone; nothing to refuse
      end
      pid = Integer(content.strip, exception: false)
      unless pid&.positive?
        raise Error, "refusing to overwrite #{@pidfile}: does not hold a pid"
      end
      raise Error, "already running (pid #{pid}, per #{@pidfile})" if process_alive?(pid)
    end

    def process_alive?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true # exists, just not ours to signal
    end

    # Delete only a pidfile that is still ours: by shutdown time the path
    # may belong to a replacement instance, or an operator may have
    # repointed it at something that is not a pidfile at all.
    def remove_pidfile
      File.unlink(@pidfile) if File.read(@pidfile) == "#{Process.pid}\n"
    rescue Errno::ENOENT
      nil
    end

    def join_workers(deadline)
      if @supervisor
        @supervisor.shutdown([deadline - monotonic_now, 0].max)
      else
        threads = @worker_threads_lock.synchronize { @worker_threads.dup }
        threads.each do |thread|
          thread.join([deadline - monotonic_now, 0.01].max)
        end
      end
    end

    def workers_done?
      if @supervisor
        @supervisor.done?
      else
        threads = @worker_threads_lock.synchronize { @worker_threads.dup }
        threads.none?(&:alive?)
      end
    end

    def kill_stragglers
      if @supervisor
        # Ractors cannot be force-killed; their clients were already freed
        # by abort_all_inflight. The stuck ractor leaks until process exit.
        Log.error("shutdown deadline passed with stuck ractor workers") unless @supervisor.done?
      else
        threads = @worker_threads_lock.synchronize { @worker_threads.dup }
        threads.each { |thread| thread.kill if thread.alive? }
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
        if (name = unshareable_worker_hook_name)
          raise Error,
            "mode: :ractor requires a Ractor-shareable #{name} hook " \
            "(build it with Ractor.shareable_proc, or use mode: :threaded)"
        end
        :ractor
      when :auto
        if !Ractor.shareable?(@app)
          Log.warn("app is not Ractor-shareable; falling back to mode: :threaded")
          :threaded
        elsif (name = unshareable_worker_hook_name)
          Log.warn("#{name} hook is not Ractor-shareable; falling back to mode: :threaded")
          :threaded
        else
          :ractor
        end
      else
        raise ArgumentError, "mode must be :auto, :ractor, or :threaded (got #{requested.inspect})"
      end
    end

    # The hooks that ride into worker context (a ractor in :ractor mode)
    # and so must be Ractor-shareable there. after_boot and on_worker_exit
    # run on the main thread and are exempt.
    def worker_context_hooks
      [[:on_error, @on_error], [:after_worker_boot, @after_worker_boot],
        [:after_request_complete, @after_request_complete]]
    end

    # The name of the first worker-context hook that is set but not
    # Ractor-shareable, or nil if all set ones are. Used by both the
    # :ractor raise and the :auto warn/fallback branches, so "first
    # offender" is defined once.
    def unshareable_worker_hook_name
      bad = worker_context_hooks.find { |_name, hook| !(hook.nil? || Ractor.shareable?(hook)) }
      bad&.first
    end
  end
end
