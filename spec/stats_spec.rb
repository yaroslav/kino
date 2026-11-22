# frozen_string_literal: true

require "json"
require "net/http"

RSpec.describe "server stats" do
  let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  it "echoes configuration before start" do
    server = Kino::Server.new(ok_app, mode: :threaded, workers: 2, threads: 3)
    stats = server.stats

    expect(stats).to include(mode: :threaded, workers: 2, threads: 3, lanes: false,
      batch: 1, respawns: 0)
    expect(stats).not_to have_key(:served)
  end

  it "counts served requests and reports live queue state" do
    with_server(ok_app) do |host, port, server|
      5.times { Net::HTTP.get_response(host, "/", port) }

      stats = server.stats
      expect(stats[:served]).to eq(5)
      expect(stats[:rejected]).to eq(0)
      expect(stats[:queued]).to eq(0)
      expect(stats[:in_flight]).to eq(0)
      expect(stats).to have_key(:worker_status)
      expect(stats[:worker_status]).to be_an(Array)
      expect(stats[:queue_time]).to include(:count, :sum_seconds)
    end
  end

  it "counts rejected requests under overload" do
    slow = lambda do |_env|
      sleep 0.3
      [200, {"content-type" => "text/plain"}, ["done"]]
    end

    with_server(slow, workers: 1, threads: 1, queue_depth: 1, queue_timeout: 0.1) do |host, port, server|
      responses = Array.new(6) do
        Thread.new { Net::HTTP.get_response(host, "/", port).code }
      end.map(&:value)

      expect(responses).to include("503")
      expect(server.stats[:rejected]).to eq(responses.count("503"))
      expect(server.stats[:served]).to eq(responses.count("200"))
    end
  end

  it "reports per-lane depths in lane mode" do
    with_server(ok_app, lanes: true, workers: 2, threads: 2) do |host, port, server|
      Net::HTTP.get_response(host, "/", port)
      stats = server.stats

      expect(stats[:lane_depths]).to eq([0, 0, 0, 0])
      expect(stats[:served]).to eq(1)
    end
  end

  it "formats a stats line for the USR1 handler" do
    line = Kino::CLI.stats_line({mode: :ractor, served: 42,
      worker_status: [{index: 0, served: 1, in_flight: 0, busy_ms: 0}]})

    expect(line).to start_with("stats ")
    expect(line).to include("mode=:ractor")
    expect(line).to include("served=42")
    expect(line).not_to include("worker_status")
  end

  describe "with WebSockets" do
    include_context "native WebSocket server"

    each_mode do |mode|
      # One socket at a time (websocket_max 1): an echo that sends a
      # message and closes, with a handshake refused meanwhile, then a
      # client that never answers a ping. The other causes are the native
      # layer's to tell apart.
      it "counts sockets opened and closed by cause, refused handshakes, and traffic, as /stats does" do
        opts = {mode:, websocket_max: 1, websocket_ping_interval: 0.5, control_bind: "127.0.0.1:0"}
        with_ws_server(ws_app(EchoSocket), **opts) do |port, server|
          before = server.stats[:websockets]
          closed = ->(what) { wait_for(what) { server.stats[:websockets][:open] == before[:open] } }

          echo = connect(port)
          expect(refusal(port)&.status).to eq(503)
          echo.send_text("hi")
          expect(echo.read_answering_pings).to eq([WsClient::TEXT, "hi"])
          echo.close
          closed.call("the echo's close")
          connect(port) # and never a word, not even a pong
          closed.call("the silent client dropped")
          # A dropped socket's slot lets go of its request just after the count.
          wait_for("the workers to settle") { server.stats[:worker_status].all? { |row| row[:in_flight].zero? } }

          stats = server.stats
          websockets = stats[:websockets]
          since = ->(key) { websockets[key] - before[key] }
          by_cause = lambda do |key|
            websockets[key].to_h { |cause, count| [cause, count - before[key][cause]] }.reject { |_, count| count.zero? }
          end
          expect(%i[opened messages_received messages_sent].to_h { |key| [key, since.call(key)] })
            .to eq(opened: 2, messages_received: 1, messages_sent: 1)
          expect(by_cause.call(:handshakes_refused)).to eq(websocket_max: 1)
          expect(by_cause.call(:closed)).to eq(normal: 1, dead_peer: 1)
          expect(%i[received_bytes sent_bytes].map(&since)).to all(be_positive)

          json = JSON.parse(Net::HTTP.get("127.0.0.1", "/stats", server.control_port), symbolize_names: true)
          expect(stats.keys).to eq(json.keys - %i[state version])
          expect(json[:websockets]).to eq(websockets)
          expect(json.slice(:wakes, :hijacked_connections, :worker_status))
            .to eq(stats.slice(:wakes, :hijacked_connections, :worker_status))
        end
      end
    end

    each_mode do |mode|
      it "reports a worker held up in its wake hook as busy, as quarantine reads it, and counts the wake" do
        FileGate.closed do |gate|
          reports = self.reports.port
          boot = Ractor.shareable_proc do |_worker_id|
            waker = Kino::Worker.on_wake do
              reports << :on_wake
              gate.wait
            end
            reports << waker
          end
          with_ws_server(ws_app(EchoSocket), mode: mode, workers: 1, after_worker_boot: boot) do |_port, server|
            waker = self.reports.next(awaiting: "the worker's waker")
            worker = -> { server.stats[:worker_status].first }

            waker.wake

            expect(self.reports.next(awaiting: "on_wake")).to eq(:on_wake)
            wait_for("the worker in its wake hook to report busy") { worker.call in {in_flight: 1, busy_ms: 50..} }
            gate.open
            wait_for("the worker done with its wake hook to report idle") { worker.call in {in_flight: 0, busy_ms: 0} }
            expect(server.stats[:wakes]).to eq(1)
          ensure
            gate.open
          end
        end
      end
    end
  end

  it "reads respawns from the native layer" do
    with_server(ok_app) do |_host, _port, server|
      id = server_id(server)
      Kino::Native.record_respawn(id)
      expect(server.stats[:respawns]).to eq(1)
    end
  end
end
