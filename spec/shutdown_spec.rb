# frozen_string_literal: true

require "socket"
require "tmpdir"

# Graceful shutdown and backpressure. Ordering is asserted through
# observable effects (response codes, return timing), never bare sleeps.
RSpec.describe "graceful shutdown" do
  def sleeper_app(duration)
    lambda do |_env|
      sleep duration
      [200, {"content-type" => "text/plain"}, ["finished"]]
    end
  end

  def slow_app
    Ractor.shareable_proc do |_env|
      Kino.sleep(0.4)
      [200, {}, []]
    end
  end

  # Unix bind, control plane and pidfile in `dir`: the files a finished
  # shutdown removes.
  def settings(dir, mode, **extra)
    {mode: mode, workers: 1, threads: 1, bind: "unix://#{dir}/kino.sock",
     control_bind: "unix://#{dir}/control.sock", pidfile: "#{dir}/kino.pid", **extra}
  end

  def boot(mode, app, **extra)
    Dir.mktmpdir do |dir|
      server = Kino::Server.new(app, **settings(dir, mode, **extra)).start
      yield server, dir
    ensure
      server&.shutdown(timeout: 0)
    end
  end

  # A drain starts by closing the listener: the socket file stays until
  # the runtime is gone, but nothing accepts on it.
  def refuses_connections?(sock)
    UNIXSocket.open(sock).close
    false
  rescue Errno::ECONNREFUSED, Errno::ENOENT
    true
  end

  # A GET over the unix socket, from a thread whose value is the status
  # line, or nil when the connection closes without one.
  def request_in_background(dir)
    Thread.new do
      UNIXSocket.open("#{dir}/kino.sock") do |socket|
        socket.write("GET / HTTP/1.1\r\nHost: kino\r\nConnection: close\r\n\r\n")
        socket.read.lines.first&.chomp
      end
    rescue SystemCallError
      nil
    end
  end

  # Server.run on `app` (Ruby source for a shareable Rack app) in a child
  # process, with `worker_loop` a server whose workers run each request as
  # an Async task, yielded once its signal traps are in (on_ready runs
  # after them) and always reaped. As the child begins to exit, it prints
  # which of the files a finished shutdown removes are still there.
  def launch(mode, app = "Ractor.shareable_proc { |_env| [200, {}, []] }", worker_loop: false, **extra)
    Dir.mktmpdir do |dir|
      serve = "Ractor.shareable_proc { |worker| " \
        "Sync { |task| worker.serve(concurrency: 8) { |job| task.async { job.call } } } }"
      script = <<~RUBY
        Warning[:experimental] = false
        require "kino"
        #{'require "async"' if worker_loop}
        at_exit { puts "left at exit: \#{%w[kino.pid kino.sock control.sock].select { |f| File.exist?(f) }.join(" ")}." }
        Kino::Server.run(#{app}, credits: false, on_ready: -> { puts "spec: ready" },
          #{"worker_loop: #{serve}," if worker_loop} **#{settings(dir, mode, workers: 2, **extra).inspect})
      RUBY
      pid = spawn(Gem.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script,
        chdir: dir, out: "#{dir}/out.log", err: [:child, :out])
      wait_for("the child to be ready", timeout: 15) { output(dir).lines.include?("spec: ready\n") }
      yield pid, dir
    ensure
      reap(pid) if pid
    end
  end

  # As #launch, with an app whose requests never leave the handler; yields
  # once one is in it, with its #request_in_background thread.
  def launch_with_stuck_request(mode, **extra)
    launch(mode, 'Ractor.shareable_proc { |_env| File.write("in-handler", ""); sleep }', **extra) do |pid, dir|
      client = request_in_background(dir)
      wait_for("the request to reach the handler") { File.exist?("#{dir}/in-handler") }
      yield pid, dir, client
    end
  end

  def reap(pid)
    return if Process.wait(pid, Process::WNOHANG)

    Process.kill("KILL", pid)
    Process.wait(pid)
  rescue Errno::ECHILD
    nil # reaped already
  end

  # The child's exit status, or nil while it still runs `within` seconds on.
  def exit_status(pid, within: 10)
    deadline = monotonic + within
    until (status = Process.wait2(pid, Process::WNOHANG)&.last)
      return if monotonic > deadline

      sleep 0.01
    end
    status
  end

  def output(dir) = File.read("#{dir}/out.log")

  def left_at_exit(dir) = output(dir)[/^left at exit: (.*)\.$/, 1]

  it "completes in-flight requests while draining" do
    server = Kino::Server.new(sleeper_app(0.4), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    # Measured from the request's start, not from after the sleep: on a
    # loaded box the sleep can overshoot by more than the handler's
    # remaining time, and the point is only that shutdown outlasts the
    # request.
    started = monotonic
    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1 # request is now in flight

    server.shutdown(timeout: 5)
    elapsed = monotonic - started

    response = client.value
    expect(response.code).to eq("200")
    expect(response.body).to eq("finished")
    expect(elapsed).to be >= 0.4, "shutdown should have waited for the in-flight request"
  end

  it "refuses new connections while draining" do
    server = Kino::Server.new(sleeper_app(0.6), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1
    shutdown_thread = Thread.new { server.shutdown(timeout: 5) }
    sleep 0.15 # accept loop is now stopped, drain in progress

    expect do
      Net::HTTP.start("127.0.0.1", port, open_timeout: 0.5) { |http| http.get("/") }
    end.to raise_error(SystemCallError)

    expect(client.value.code).to eq("200")
    shutdown_thread.join
  end

  it "enforces the deadline on stuck handlers and frees their clients" do
    server = Kino::Server.new(sleeper_app(10), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1

    elapsed = timed { server.shutdown(timeout: 0.5) }

    expect(elapsed).to be < 2.5, "shutdown must return by deadline + epsilon, took #{elapsed.round(2)}s"
    expect(client.value.code).to eq("500"), "the stuck request's client must be freed with a 500"
  end

  it "is idempotent" do
    server = Kino::Server.new(->(_env) { [200, {}, []] }, workers: 1, threads: 1, mode: :threaded).start
    server.shutdown
    expect { server.shutdown }.not_to raise_error
  end

  each_mode do |mode|
    it "makes a second shutdown wait for the one under way, whose drain it leaves alone" do
      boot(mode, slow_app) do |server, dir|
        client = request_in_background(dir)
        wait_for("the request in flight") { server.stats[:in_flight] == 1 }
        first = Thread.new { server.shutdown(timeout: 5) }
        wait_for("the first shutdown to stop accepting") { refuses_connections?("#{dir}/kino.sock") }

        server.shutdown(timeout: 0)

        expect(File.exist?("#{dir}/kino.pid")).to be(false), "the second shutdown returned before the first was done"
        expect(client.value).to start_with("HTTP/1.1 200")
        expect(first.join(5)).to eq(first)
      end
    end

    it "keeps serving, scaling included, after a shutdown that fails before it stops accepting" do
      boot(mode, slow_app, max_workers: 2) do |server, dir|
        expect { server.shutdown(timeout: "soon") }.to raise_error(TypeError)

        # Queue pressure still grows the pool.
        burst = Array.new(3) { request_in_background(dir) }
        wait_for("the pool to grow") { server.stats[:scale_ups] >= 1 }
        expect(burst.map(&:value)).to all(start_with("HTTP/1.1 200"))
        server.shutdown
        expect(File.exist?("#{dir}/kino.pid")).to be(false)
      end
    end

    # A signal's shutdown runs on a thread of its own, beside the main
    # thread blocked in Server.run, so these boot a real process.
    describe "on a signal" do
      {"" => false, " under a worker_loop" => true}.each do |name, worker_loop|
        it "exits 0 only once the whole shutdown is done#{name}" do
          launch(mode, worker_loop:) do |pid, dir|
            expect(Dir.children(dir)).to include("kino.pid", "kino.sock", "control.sock")
            client = request_in_background(dir)
            answer = client.join(5) && client.value
            expect(answer).to start_with("HTTP/1.1 200"), "wanted a 200 within 5 s, got #{answer.inspect}\n#{output(dir)}"
            Process.kill("TERM", pid)

            expect(exit_status(pid)&.exitstatus).to eq(0), output(dir)
            expect(left_at_exit(dir)).to eq(""), "the process began to exit before its shutdown was done:\n#{output(dir)}"
          end
        end
      end

      it "exits 1 with the error, printed once, when the shutdown fails" do
        launch(mode) do |pid, dir|
          # Removing the pidfile, the shutdown's last step, now fails.
          File.unlink("#{dir}/kino.pid")
          Dir.mkdir("#{dir}/kino.pid")
          Process.kill("TERM", pid)

          expect(exit_status(pid)&.exitstatus).to eq(1), output(dir)
          expect(output(dir).scan("(Errno::EISDIR)").size).to eq(1), output(dir)
          expect(left_at_exit(dir)).to eq("kino.pid"), "every step before the failing one should have run"
        end
      end

      it "force-exits on a second signal while the drain still waits" do
        launch_with_stuck_request(mode, shutdown_timeout: 30) do |pid, dir|
          Process.kill("TERM", pid)
          wait_for("the child to log that it drains") { output(dir).include?("signal again to force exit") }
          Process.kill("TERM", pid)

          expect(exit_status(pid)&.exitstatus).to eq(1), output(dir)
        end
      end

      it "answers a hijacked long poll it still holds with a 503 before the process exits" do
        # A long poll with no news: its worker thread keeps the socket.
        long_poll = "Ractor.shareable_proc do |env| " \
          "worker = Thread.current; " \
          "held = worker.thread_variable_get(:held) || worker.thread_variable_set(:held, []); " \
          "held << env[\"rack.hijack\"].call; File.write(\"held\", \"\"); [-1, {}, []] end"
        launch(mode, long_poll) do |pid, dir|
          poll = UNIXSocket.open("#{dir}/kino.sock")
          poll.write("GET /poll HTTP/1.1\r\nHost: kino\r\nUpgrade: chat\r\nConnection: Upgrade\r\n\r\n")
          wait_for("the poll to be held", timeout: 15) { File.exist?("#{dir}/held") }
          Process.kill("TERM", pid)

          expect(exit_status(pid)&.exitstatus).to eq(0), output(dir)
          expect(poll.read).to start_with("HTTP/1.1 503 "), "the poll got no 503 before the process exited"
        ensure
          poll&.close
        end
      end
    end
  end

  # A ractor cannot be killed, and a stuck one once kept the process from
  # exiting at all; the threaded deadline is pinned in-process.
  it "answers a request stuck past the deadline with a 500 on a signal, then exits by the deadline (ractor mode)" do
    launch_with_stuck_request(:ractor, shutdown_timeout: 0.5) do |pid, dir, client|
      Process.kill("TERM", pid)

      expect(exit_status(pid, within: 5)&.exitstatus).to eq(0), output(dir)
      expect(client.value).to start_with("HTTP/1.1 500")
      expect(left_at_exit(dir)).to eq("")
    end
  end

  describe "backpressure" do
    it "returns 503 on queue overflow without hanging anyone" do
      app = sleeper_app(0.3)
      opts = {workers: 1, threads: 1, mode: :threaded, queue_depth: 1, queue_timeout: 0.1}

      with_server(app, **opts) do |host, port|
        responses = Array.new(6) do
          Thread.new { Net::HTTP.get_response(host, "/", port).code }
        end.map(&:value)

        expect(responses.size).to eq(6)
        expect(responses).to include("200"), "the in-flight request should finish"
        expect(responses).to include("503"), "overflow should be rejected, not queued forever"
        expect(responses.tally.keys.sort).to eq(%w[200 503])
      end
    end
  end
end
