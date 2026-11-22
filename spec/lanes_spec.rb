# frozen_string_literal: true

require "rack"

# Experimental lane dispatch: per-worker queues, awake-preferring
# assignment, work stealing. Must behave identically to the shared queue
# for correctness; only the scheduling differs.
RSpec.describe "lane dispatch" do
  def shareable_app
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/slow" then sleep 0.2
                        [200, {"content-type" => "text/plain"}, ["slow done"]]
      when "/second" then sleep 1
                          [200, {"content-type" => "text/plain"}, ["second done"]]
      else [200, {"content-type" => "text/plain"}, ["lane ok"]]
      end
    end
  end

  it "serves requests under rack-lint with lanes enabled (threaded)" do
    app = Rack::Lint.new(->(_env) { [200, {"content-type" => "text/plain"}, ["lint ok"]] })

    with_server(app, lanes: true, workers: 2, threads: 2) do |host, port|
      10.times do
        expect(Net::HTTP.get_response(host, "/x", port).body).to eq("lint ok")
      end
    end
  end

  each_mode do |mode|
    it "answers fast requests at once while another worker is deep in a slow one" do
      fast_beside_slow(mode)
    end
  end

  def fast_beside_slow(mode)
    # Two workers: the idle one parked as it answered the request before,
    # so its next park tick, when it would steal, is a whole tick away.
    with_server(shareable_app, mode: mode, lanes: true, workers: 2, threads: 1) do |_, port, server|
      get(port, "/")
      slow = Thread.new { get(port, "/second") }
      wait_for("the slow request to be 20 ms in") do
        server.stats[:worker_status].any? { |worker| worker[:in_flight] > 0 && worker[:busy_ms] >= 20 }
      end

      # How long each fast request waited for a worker, as the server
      # counts it, so a client the host holds up does not count.
      waits = Array.new(10) do
        before = server.stats[:queue_time][:sum_seconds]
        expect(get(port, "/")).to eq("lane ok")
        server.stats[:queue_time][:sum_seconds] - before
      end

      expect(slow.value).to eq("second done")
      # Tens of microseconds each when they go to the idle worker, or a
      # scheduler quantum on a busy host (10 ms, on two CPUs shared with
      # four busy processes). One that queues behind the slow request
      # waits for the idle worker's park tick, 30 to 60 ms, and so do
      # nine of the ten at least, in every run with the pass-over gone.
      late = waits.count { |wait| wait > 0.025 }
      expect(late).to be <= 1, "fast requests waited behind the slow one: #{waits.map { |t| (t * 1000).round(2) }} ms"
    end
  end

  each_mode do |mode|
    it_in_both_loops "answers each of a few clients' requests once while idle workers wake and steal" do
      app = Ractor.shareable_proc { |env| [200, {"content-type" => "text/plain"}, [env["PATH_INFO"]]] }
      # Through lane dispatch: inline, a connection stays on its worker and nothing steals.
      lane_path = server_defaults.key?(:worker_loop) ? {_inline: false} : {}
      with_server(app, **server_defaults, **lane_path, mode:, lanes: true, workers: 4, threads: 1) do |_, port, server|
        answers = Array.new(4) do |client|
          Thread.new do
            Net::HTTP.start("127.0.0.1", port, read_timeout: 5) do |http|
              Array.new(50) { |i| http.get("/#{client}/#{i}").body }
            end
          end
        end.map(&:value)

        expect(answers).to eq(Array.new(4) { |client| Array.new(50) { |i| "/#{client}/#{i}" } })
        expect(server.stats[:served]).to eq(200)
      end
    end
  end

  it "drains cleanly on shutdown" do
    server = Kino::Server.new(shareable_app, mode: :ractor, lanes: true,
      workers: 2, threads: 1).start
    port = server.port
    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/slow", port) }
    sleep 0.05

    server.shutdown(timeout: 5)

    expect(client.value.code).to eq("200")
  end

  # The line and the retry each wait their own way; the mode changes
  # neither.
  [false, true].each do |worker_loop|
    it "answers requests waiting for lane room when the shutdown starts, within the drain " \
      "(#{worker_loop ? "a worker_loop" : "its own loop"})" do
      drain_with_requests_waiting(:ractor, worker_loop)
    end
  end

  # A drain starts by closing the listener; a connection caught in its
  # backlog as it closes is reset.
  def refuses_connections?(port)
    TCPSocket.new("127.0.0.1", port).close
    false
  rescue Errno::ECONNREFUSED, Errno::ECONNRESET
    true
  end

  # One worker held by a request, 4 more in its lane and 2 waiting for
  # room when the shutdown starts; then the worker is let go. A
  # worker_loop serves nothing inline here, as this is lane dispatch.
  def drain_with_requests_waiting(mode, worker_loop)
    FileGate.closed do |gate|
      released = gate.path
      app = Ractor.shareable_proc do |env|
        Fiber.blocking { sleep 0.005 until File.exist?(released) } if env["PATH_INFO"] == "/hold"
        [200, {"content-type" => "text/plain"}, ["ok"]]
      end
      opts = worker_loop ? {worker_loop: async_loop(4), _inline: false} : {}
      server = Kino::Server.new(app, mode: mode, lanes: true, workers: 1, threads: 1, **opts).start
      port = server.port
      begin
        holder = Thread.new { get(port, "/hold") }
        wait_for("the worker to take the holding request") do
          server.stats[:worker_status].sum { |worker| worker[:in_flight] } == 1
        end
        clients = Array.new(6) do |n|
          Thread.new { get(port, "/?#{n}") }.tap do
            wait_for("request #{n} at the server") { server.stats[:in_flight] == n + 2 }
          end
        end

        shutdown = Thread.new { server.shutdown(timeout: 10) }
        wait_for("the drain to start") { refuses_connections?(port) }
        gate.open

        expect(clients.map(&:value)).to all(eq("ok"))
        expect(holder.value).to eq("ok")
      ensure
        gate.open
        shutdown ? shutdown.join : server.shutdown
      end
    end
  end
end
