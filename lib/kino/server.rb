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
      # Internal: false serves worker_loop connections through hyper, for specs of that path.
      @inline = options.delete(:_inline) != false
      config = Configuration.new
      config.load_file(config_file) if config_file
      config.merge!(options)
      settings = config.to_h

      @app = app
      @bind = settings[:bind]
      @requested_port = settings[:port]
      configure_pool(settings)
      configure_limits(settings)
      configure_io(settings)
      configure_quarantine(settings)
      @pool = nil
      @quarantine_monitor = nil
      @pool_scaler = nil
      @started = false
      # Under @lifecycle: @stopping while a shutdown runs, @shutdown_error
      # from the last one that failed; @shutdown_ended signals each end.
      @lifecycle = Mutex.new
      @shutdown_ended = ConditionVariable.new
      @stopping = false
      @shutdown_error = nil
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
          http2: @http2, lanes: @lanes, worker_loop: @worker_loop, log_requests: @log_requests,
          # With one usable CPU a spinner would hold the CPU its client's request needs.
          inline: @inline, inline_spin: Kino.available_parallelism >= 2,
          websocket_max_message: @websocket_max_message, websocket_max: @websocket_max,
          websocket_ping_interval_ms: @websocket_ping_interval_ms,
          mode: @mode.to_s, workers: @workers, max_workers: @max_workers,
          threads: @threads, batch: @batch,
          control_bind: @control_bind, control_token: @control_token
        )
        booted = true
      ensure
        remove_pidfile if @pidfile && !booted
      end
      # GC anchor for zero-copy response buffers: held for the server's
      # lifetime so in-flight buffers survive even a worker ractor crash.
      @pin_keeper = Native.pin_keeper(@id)
      warn_scheduler_cap if @mode == :ractor
      @pool = ((@mode == :ractor) ? RactorSupervisor : ThreadedPool).new(@id, @app, workers: @workers,
        threads: @threads, batch: @batch, hooks: @worker_hooks, on_worker_exit: @on_worker_exit).start
      start_quarantine_monitor if @quarantine_timeout_ms
      start_pool_scaler if elastic?
      Native.control_ready(@id)
      HookFire.fire(@after_boot, "after_boot")
      @lifecycle.synchronize do
        @shutdown_error = nil
        @started = true
      end
      self
    end

    # Graceful shutdown: stop accepting, close WebSockets (1001, after on_shutdown), drain
    # in-flight work up to the deadline, then escalate: abort remaining clients (500), interrupt
    # blocked workers, kill stragglers; and tear down the runtime. Always
    # returns by ~deadline + a small epsilon; idempotent. One runs at a
    # time: a call made during another waits for it to end, then returns
    # as #wait does, or shuts down itself if the server still serves. A
    # failure before it stops accepting leaves the server serving; a later
    # one leaves it stopped. Either way the error is raised here and by
    # #wait.
    #
    # @param timeout [Numeric, nil] drain deadline in seconds, not negative;
    #   Float::INFINITY waits for the drain however long it takes (default:
    #   the configured shutdown_timeout)
    # @return [nil]
    # @raise [ArgumentError] when timeout is NaN, negative or not a number,
    #   whether the server runs or not
    def shutdown(timeout: nil)
      seconds = timeout.nil? ? @shutdown_timeout : drain_seconds(timeout, :timeout)
      return unless @started
      return wait unless claim_shutdown

      stopped_accepting = false
      failure = nil
      begin
        deadline = Kino.monotonic + seconds
        @pool_scaler&.stop
        @quarantine_monitor&.stop
        Native.stop_accepting(@id)
        stopped_accepting = true
        drain(deadline)
        stop_workers(deadline)
        tear_down
      rescue Exception => failure # rubocop:disable Lint/RescueException -- recorded for #wait, then re-raised
        raise
      ensure
        end_shutdown(failure, stopped: stopped_accepting)
      end
      nil
    end

    # Block until a #shutdown, from a signal or any thread, has ended; at
    # once when the server is not running.
    #
    # @return [void]
    # @raise [Exception] what the shutdown raised, when it failed
    def wait
      failure = @lifecycle.synchronize do
        @shutdown_ended.wait(@lifecycle) while @started && !@shutdown_error
        @shutdown_error
      end
      raise failure if failure
    end

    # Production entry point: build the server and {#run} it. The `kino`
    # CLI funnels into this too (CLI#serve).
    #
    # @param app [#call] a Rack 3 application
    # @param credits [Boolean] print the opening credits (the CLI prints them
    #   itself, ahead of loading the app, and passes false)
    # @param on_ready [#call, nil] called once the ready block is out; the
    #   CLI's seam for replaying held-back app output
    # @param opts [Hash] see #initialize
    # @return [Kino::Server] the (stopped) server, after shutdown
    def self.run(app, credits: true, on_ready: nil, **opts)
      new(app, **opts).run(credits: credits, on_ready: on_ready)
    end

    # Serve until shut down: start, print the banner, trap INT/TERM for
    # graceful shutdown (second signal force-exits), block until done.
    # The Rack handler calls this on a server it built itself.
    #
    # @return [self] after shutdown
    # @raise [Exception] what a failed shutdown raised
    def run(credits: true, on_ready: nil)
      # Startup output must land immediately even when stdout is a pipe or
      # file (process supervisors, `kino > server.log`, `rails server`
      # under Docker); block buffering would hold the banner back until
      # exit.
      $stdout.sync = true
      CLI.opening_credits if credits
      start
      CLI.action!(self)
      CLI.fin_at_exit
      self.class.trap_signals(self)
      on_ready&.call
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
          # Its error is reported once: #wait raises it on the main thread.
          Thread.new do
            Thread.current.report_on_exception = false
            server.shutdown
          end
        end
      end
    end

    # Live snapshot. Counters come from the native layer (one relaxed
    # atomic per request); config echo makes the line self-describing.
    # Keys in /stats's order; :responses_cut and :websockets cover the whole process.
    #
    # @return [Hash{Symbol => Object}] while it serves, every /stats key but state
    #   and version; otherwise the config echo: mode, lanes, workers, threads, batch,
    #   respawns, max_workers, active_workers, scale_ups and scale_downs
    def stats
      (@started && Native.stats(@id)) || {
        mode: @mode, lanes: @lanes, workers: @workers, threads: @threads, batch: @batch, respawns: 0,
        max_workers: @max_workers, active_workers: @workers, scale_ups: 0, scale_downs: 0
      }
    end

    private

    # Say goodbye to the sockets, then wait for queued and in-flight work, up to `deadline`.
    def drain(deadline)
      # Each socket's on_shutdown, then its 1001, spread over at most a second and half of what
      # is left of the drain. Seconds as they are: a deadline may be infinite.
      Native.ws_leave_all(@id, deadline - Kino.monotonic)

      # Drain: wait for queued + in-flight to reach zero, bounded by deadline.
      # A socket counts until its on_close is with its worker; hijacked connections close below.
      until Kino.monotonic >= deadline
        queued, in_flight, sockets = Native.queue_stats(@id)
        break if queued.zero? && in_flight.zero? && sockets.zero?

        sleep 0.01
      end
    end

    # Close the queues and join the pool; past `deadline`, free the clients and reap the workers.
    def stop_workers(deadline)
      # Idle workers see the closed queue and exit their loops.
      # Switched hijacked connections close too, so an app serving one sees EOF.
      Native.close_queue(@id)
      join_workers(deadline)

      unless @pool.done?
        # Past the deadline with stuck handlers: free the clients first,
        # then try to unblock and reap the workers.
        Native.abort_all_inflight(@id)
        Native.interrupt_all_workers(@id)
        join_workers(Kino.monotonic + 0.2)
        @pool.kill_stragglers
      end
    end

    def tear_down
      # Only now the other hijacked requests: an unanswered long poll gets a 503.
      Native.close_bridges(@id)
      Native.shutdown_runtime(@id, 1_000)
      # The control thread reports "draining" for the whole drain and stops
      # only now, once there is nothing left to report.
      Native.control_stop(@id)
      # The runtime is gone, so hyper has dropped every pinned buffer;
      # the keeper (and the strings it marked) may now be collected.
      @pin_keeper = nil
      remove_pidfile if @pidfile
    end

    # The pool's shape: hooks, mode, workers and their ceiling, lanes, threads.
    def configure_pool(settings)
      worker_hooks = WORKER_HOOKS.to_h { |name| [name, validate_hook(settings[name], name)] }
      @after_boot = validate_hook(settings[:after_boot], :after_boot)
      @on_worker_exit = validate_hook(settings[:on_worker_exit], :on_worker_exit)
      @worker_loop = !worker_hooks[:worker_loop].nil?
      # Unset, lanes follow worker_loop, which runs on them.
      @lanes = settings[:lanes].nil? ? @worker_loop : !!settings[:lanes]
      raise ArgumentError, "worker_loop needs lanes; drop `lanes false`" if @worker_loop && !@lanes
      @mode = resolve_mode(settings[:mode], worker_hooks)
      @workers = Integer(settings[:workers] || default_workers)
      # The pool ceiling; equal to the floor for a fixed pool.
      @max_workers = settings[:max_workers].nil? ? @workers : Integer(settings[:max_workers])
      if @max_workers < @workers
        raise ArgumentError, "max_workers (#{@max_workers}) must be at least workers (#{@workers})"
      end
      @scale_down_after = settings[:scale_down_after].nil? ? 30.0 : Float(settings[:scale_down_after])
      raise ArgumentError, "scale_down_after must be positive" unless @scale_down_after.positive?
      if !settings[:scale_down_after].nil? && !elastic?
        Log.warn("scale_down_after has no effect unless max_workers is above workers")
      end
      @worker_hooks = WorkerHooks.new(
        **worker_hooks,
        # The access log's GC and allocation figures come from the VM's
        # process-wide counters, so they are measured only where one
        # request at a time can own them: the GVL serializes :threaded
        # mode, and a single ractor (a pool that can never grow past one)
        # has nothing to race. A worker_loop interleaves its requests.
        access_timing: !!settings[:log_requests] && !@worker_loop && (@mode == :threaded || @max_workers == 1)
      )
      # Default threads per mode: 1 in :ractor (threads inside a ractor
      # share its lock; a measured +17% on fast handlers; raise `workers`
      # for I/O concurrency instead), 3 in :threaded (threads ARE the
      # concurrency there).
      @threads = Integer(settings[:threads] || ((@mode == :ractor) ? 1 : 3))
    end

    # Queue, timeouts and sizes, the WebSocket budgets, and logging.
    def configure_limits(settings)
      # 0 would hand requests only to a worker already waiting, and 503 the rest.
      @queue_depth = positive_integer(settings[:queue_depth], :queue_depth)
      @queue_timeout_ms = whole_ms(settings[:queue_timeout])
      @request_timeout_ms = settings[:request_timeout] ? whole_ms(settings[:request_timeout]) : 0
      @max_connections = settings[:max_connections] ? Integer(settings[:max_connections]) : default_max_connections
      @max_body_size = Integer(settings[:max_body_size] || 0)
      @batch = [Integer(settings[:batch]), 1].max
      Log.warn("batch has no effect with worker_loop") if @worker_loop && @batch > 1
      @websocket_max_message = positive_integer(settings[:websocket_max_message], :websocket_max_message)
      @websocket_max = settings[:websocket_max] && positive_integer(settings[:websocket_max], :websocket_max)
      @websocket_ping_interval_ms = milliseconds(settings[:websocket_ping_interval], :websocket_ping_interval)
      @log_requests = !!settings[:log_requests]
      @shutdown_timeout = drain_seconds(settings[:shutdown_timeout], :shutdown_timeout)
    end

    # The I/O runtime, TLS, HTTP/2, the pidfile and the control plane.
    def configure_io(settings)
      @io_shards = !!settings[:io_shards]
      @io_threads = Integer(settings[:io_threads]) unless settings[:io_threads].nil?
      if @io_threads && @io_threads < 1
        raise ArgumentError, "io_threads must be >= 1"
      end
      Log.warn("io_threads has no effect unless io_shards is true") if @io_threads && !@io_shards
      @tokio_threads = settings[:tokio_threads]
      @tls = validate_tls(settings[:tls])
      if @tls && unix?
        raise ArgumentError, "TLS is not supported on a unix socket bind; terminate TLS at the proxy in front"
      end
      @http2 = settings.fetch(:http2, true) ? true : false
      @pidfile = settings[:pidfile]
      @control_bind = settings[:control_bind]&.to_s
      @control_token = settings[:control_token]&.to_s
      # An empty token (e.g. control_token ENV["KINO_CONTROL_TOKEN"] with the
      # var unset) must not half-disable auth: treat it as auth off, not as
      # "require a zero-length Bearer token".
      @control_token = nil if @control_token && @control_token.empty?
    end

    def configure_quarantine(settings)
      @quarantine_timeout_ms = settings[:quarantine_timeout] && whole_ms(settings[:quarantine_timeout])
      @quarantine_max =
        if settings[:quarantine_max]
          Integer(settings[:quarantine_max])
        elsif @mode == :ractor
          @workers
        else
          @workers * @threads
        end
    end

    # Both pools are quarantine replacers: replace(worker_id, generation) spawns a
    # replacement worker, then quarantines the wedged slot.
    def start_quarantine_monitor
      @quarantine_monitor = QuarantineMonitor.new(
        server_id: @id, timeout_ms: @quarantine_timeout_ms,
        max: @quarantine_max, replacer: @pool
      ).start
    end

    # Ruby's M:N scheduler runs non-main ractors' Ruby code on at most
    # RUBY_MAX_CPU native threads (default 8). Workers past that cap share
    # timeslices instead of adding parallelism, and a fresh ractor can wait
    # seconds for its first one while the others are CPU-bound.
    def warn_scheduler_cap
      cap = Kino.ruby_max_cpu
      return if @max_workers <= cap

      Log.warn("#{@max_workers} ractor workers exceed RUBY_MAX_CPU=#{cap}: only #{cap} can run " \
        "Ruby code at once; set RUBY_MAX_CPU=#{@max_workers} for CPU-bound apps")
    end

    # Threads are not limited by RUBY_MAX_CPU, ractors are.
    def default_workers
      cpus = Kino.available_parallelism
      (@mode == :ractor) ? [cpus, Kino.ruby_max_cpu].min.clamp(1..) : cpus
    end

    def start_pool_scaler
      @pool_scaler = PoolScaler.new(
        server_id: @id, pool: @pool, floor: @workers, ceiling: @max_workers,
        scale_down_after: @scale_down_after
      ).start
    end

    def validate_tls(tls)
      return nil if tls.nil?
      unless tls.is_a?(Hash) && tls[:cert] && tls[:key]
        raise ArgumentError, "tls: expects { cert:, key: } (file paths or inline PEM)"
      end

      {cert: String(tls[:cert]), key: String(tls[:key])}
    end

    def positive_integer(value, name)
      number = Integer(value, exception: false)
      return number if number&.positive?

      raise ArgumentError, "#{name} must be a positive Integer (got #{value.inspect})"
    end

    # Seconds to drain: infinity is no deadline; negative and NaN (no clock passes it) are refused.
    def drain_seconds(value, name)
      number = Float(value, exception: false)
      return number if number && !number.nan? && !number.negative?

      raise ArgumentError, "#{name} must be a non-negative number of seconds (got #{value.inspect})"
    end

    def whole_ms(seconds) = (Float(seconds) * 1000).round

    # Seconds as whole milliseconds: nil and 0 mean off (0); anything above zero is at least 1.
    def milliseconds(value, name)
      seconds = value.nil? ? 0.0 : Float(value, exception: false)
      unless seconds&.finite? && !seconds.negative?
        raise ArgumentError, "#{name} must be a finite, non-negative number of seconds or nil (got #{value.inspect})"
      end

      seconds.zero? ? 0 : [whole_ms(seconds), 1].max
    end

    def validate_hook(handler, name)
      return nil if handler.nil?
      unless handler.respond_to?(:call)
        raise ArgumentError, "#{name} must respond to #call (got #{handler.class})"
      end

      handler
    end

    # A pool that can grow: the ceiling is above the floor.
    def elastic?
      @max_workers > @workers
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

    # Wait out a shutdown under way, then claim the next; false once stopped.
    def claim_shutdown
      @lifecycle.synchronize do
        @shutdown_ended.wait(@lifecycle) while @stopping
        next false unless @started

        @stopping = true
        @shutdown_error = nil
        true
      end
    end

    def end_shutdown(failure, stopped:)
      @lifecycle.synchronize do
        @stopping = false
        @started = false if stopped
        @shutdown_error = failure
        @shutdown_ended.broadcast
      end
    end

    JOIN_SLICE = 0.1
    private_constant :JOIN_SLICE

    # Join the workers up to the monotonic `deadline`; a worker_loop's a slice at a time, with
    # Worker::Loop.warn_lingering between slices.
    def join_workers(deadline)
      return @pool.shutdown([deadline - Kino.monotonic, 0].max) unless @worker_loop

      loop do
        @pool.shutdown((deadline - Kino.monotonic).clamp(0, JOIN_SLICE))
        Worker::Loop.warn_lingering(@id)
        break if @pool.done? || Kino.monotonic >= deadline
      end
    end

    # Policy (mode resolution): when is an app safe for ractor
    # dispatch, and how loudly do we fall back? Current policy: trust
    # Ractor.shareable? on :auto with a stderr warning on fallback; forcing
    # :ractor with an unshareable app is an error, and we never
    # make_shareable the user's app behind their back (deep-freezing
    # someone's object graph is not a server's call to make).
    def resolve_mode(requested, worker_hooks)
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
        if (name = unshareable_hook_name(worker_hooks))
          raise Error,
            "mode: :ractor requires a Ractor-shareable #{name} hook " \
            "(build it with Ractor.shareable_proc, or use mode: :threaded)"
        end
        :ractor
      when :auto
        if !Ractor.shareable?(@app)
          Log.warn("app is not Ractor-shareable; falling back to mode: :threaded")
          :threaded
        elsif (name = unshareable_hook_name(worker_hooks))
          Log.warn("#{name} hook is not Ractor-shareable; falling back to mode: :threaded")
          :threaded
        else
          :ractor
        end
      else
        raise ArgumentError, "mode must be :auto, :ractor, or :threaded (got #{requested.inspect})"
      end
    end

    # The name of the first of `hooks` (name => hook) that is set but not
    # Ractor-shareable, or nil if all set ones are. Used by both the
    # :ractor raise and the :auto warn/fallback branches, so "first
    # offender" is defined once.
    def unshareable_hook_name(hooks)
      hooks.find { |_name, hook| !(hook.nil? || Ractor.shareable?(hook)) }&.first
    end
  end
end
