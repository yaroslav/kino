# frozen_string_literal: true

require "net/http"

# Raises in the callback it names and echoes otherwise; on_close
# reports [:close, socket id] first.
RaisingSocket = Data.define(:port, :failing) do
  def on_open(_ws) = fail_in(:on_open)

  def on_message(ws, data) = fail_in(:on_message) || ws.write(data)

  def on_close(ws)
    port << [:close, ws.id]
    fail_in(:on_close)
  end

  def fail_in(callback)
    raise "#{callback} failed" if callback == failing
  end
end

# Reports its on_open and on_close. With an `input`, its on_open first
# starts a thread that waits to read the handshake's request body.
BodyWaitingSocket = Data.define(:port, :input) do
  # Starts a thread of the app's reading `input`; returns once it waits
  # for the body, which the client never sends.
  def self.wait_reading(input)
    reader = Thread.new { input.read }
    Thread.pass until reader.stop?
  end

  def on_open(_ws)
    BodyWaitingSocket.wait_reading(input) if input
    port << :open
  end

  def on_close(_ws) = port << :close
end

RSpec.describe "native WebSocket failures" do
  include_context "native WebSocket server"

  # The handler raises in the callback the path names.
  def raising_app
    reported = reports.port
    Ractor.shareable_proc do |env|
      env["rack.upgrade"] = RaisingSocket.new(reported, env["PATH_INFO"].delete_prefix("/").to_sym)
      [101, {}, []]
    end
  end

  it "logs a callback's exception once, hands it to on_error with the socket and the callback, and drops that " \
    "socket alone" do
    reported = reports.port
    on_error = Ractor.shareable_proc { |error, env| reported << [:error, error.message, env, env.frozen?] }

    errors = nil
    log = capture_native_stderr do
      with_ws_server(raising_app, workers: 1, on_error: on_error) do |port|
        bystander = connect(port, path: "/on_close")
        expect { connect(port, path: "/on_open") }.to raise_error(WsClient::Refused) { |refused|
          expect(refused.status).to eq(500)
          expect(refused.headers).to include("connection" => "close")
        }
        raising = connect(port, path: "/on_message")
        raising.send_text("hi")
        expect(raising.read).to be_nil, "the raising socket must be dropped, without a close frame"
        bystander.send_text("still here")
        expect(bystander.read).to eq([WsClient::TEXT, "still here"])
        bystander.close

        seen = Array.new(6) { reports.next(awaiting: "on_error and on_close") }
        errors, closes = seen.partition { |report| report.first == :error }
        expect(errors.map { |_, message, env, frozen| [message, env.keys, env["kino.callback"], frozen] }).to eq(
          %w[on_open on_message on_close].map { |callback| ["#{callback} failed", %w[kino.callback kino.websocket], callback, true] }
        )
        # Each socket that raised, the one whose on_open did included, is
        # dropped, and its on_close runs.
        expect(closes.map(&:last)).to match_array(errors.map { |_, _, env| env["kino.websocket"] })
      end
    end

    expect(log.scan(/websocket #(\d+) (on_\w+) · RuntimeError: \2 failed \(spec\/websocket_failures_spec\.rb:\d+/))
      .to eq(errors.map { |_, _, env| [env["kino.websocket"].to_s, env["kino.callback"]] }),
        "each failure is logged once, naming its socket, with where it raised: #{log}"
  end

  each_mode do |mode|
    it "sends the 500 of a handshake whose on_open raised before on_error runs" do
      FileGate.closed do |gate|
        reports = self.reports.port
        on_error = Ractor.shareable_proc do |error, _env|
          reports << error.message
          gate.wait(WsClient::DEADLINE * 2) # past the client's patience
        end

        capture_native_stderr do
          with_ws_server(raising_app, mode: mode, workers: 1, on_error: on_error) do |port|
            expect { connect(port, path: "/on_open") }.to raise_error(WsClient::Refused) { |refused|
              expect(refused.status).to eq(500)
            }
            expect(self.reports.next(awaiting: "on_error")).to eq("on_open failed")
          ensure
            gate.open
          end
        end
      end
    end
  end

  each_mode do |mode|
    it "answers 500 to a switch while another of the app's threads waits on the request body" do
      reported = reports.port
      on_error = Ractor.shareable_proc { |error, _env| reported << [:error, error.message] }
      busy = "another of the app's threads is waiting on this request"
      # The handshake declares a body it never sends. A thread of the app's
      # waits to read it from the start (/before_accept), from on_open
      # (/in_on_open), or while the app answers 101 with a callable for the
      # connection (/callable).
      app = Ractor.shareable_proc do |env|
        next [200, {"content-type" => "text/plain"}, ["plain"]] unless env["rack.upgrade?"] == :websocket

        input = env["rack.input"]
        case env["PATH_INFO"]
        when "/in_on_open"
          env["rack.upgrade"] = BodyWaitingSocket.new(reported, input)
        when "/callable"
          BodyWaitingSocket.wait_reading(input)
          next [101, {"upgrade" => "websocket", "connection" => "Upgrade"}, ->(io) { io.close }]
        else
          BodyWaitingSocket.wait_reading(input)
          env["rack.upgrade"] = BodyWaitingSocket.new(reported, nil)
        end
        [101, {}, []]
      end

      log = capture_native_stderr do
        with_ws_server(app, mode: mode, workers: 1, on_error: on_error) do |port, server|
          {"/before_accept" => [], "/in_on_open" => %i[open close], "/callable" => []}.each do |path, callbacks|
            expect { connect(port, path: path, headers: {"content-length" => "5"}) }
              .to raise_error(WsClient::Refused) { |refused| expect(refused.status).to eq(500) }

            seen = Array.new(callbacks.size + 1) { reports.next(awaiting: "#{path}: on_error and the callbacks") }
            error, = seen.reject { |report| callbacks.include?(report) }
            expect(error).to match([:error, a_string_including(busy)]), "#{path}: #{seen}"
            expect(seen - [error]).to eq(callbacks), path
          end
          id = server_id(server)
          wait_for("the socket to stop counting") { Kino::Native.queue_stats(id)[2].zero? }
          expect(server.stats[:worker_status].sum { |row| row[:websockets_open] }).to eq(0)
          expect(server.stats[:respawns]).to eq(0)
          expect(Net::HTTP.get("127.0.0.1", "/", port)).to eq("plain")
        end
      end

      expect(log.scan(busy).size).to eq(3), log
    end
  end

  # Sets rack.upgrade, then fails by path: /raise raises, /bad-header
  # answers with a header the 101 cannot carry, and /close-raises with a
  # body whose close raises, which the 101 waits for. Bodies report their
  # close.
  it "answers 500 to a handshake whose app fails after setting rack.upgrade, and closes its body" do
    reported = reports.port
    app = Ractor.shareable_proc do |env|
      env["rack.upgrade"] = EchoSocket
      case env["PATH_INFO"]
      when "/raise" then raise "app failed after setting rack.upgrade"
      when "/bad-header" then [101, {"x-bad" => "line\nbreak"}, ReportedBody.new(reported, false)]
      else [101, {}, ReportedBody.new(reported, true)]
      end
    end

    log = capture_native_stderr do
      with_ws_server(app) do |port|
        %w[/raise /bad-header /close-raises].each do |path|
          expect { connect(port, path:) }.to raise_error(WsClient::Refused) { |refused| expect(refused.status).to eq(500) }
        end
        expect(Array.new(2) { reports.next(awaiting: "the bodies' close") }).to eq(%i[body_closed body_closed])
      end
    end

    ["app failed after setting rack.upgrade", "invalid response", "body close failed"].each do |failure|
      expect(log.scan(failure).size).to eq(1), log
    end
  end

  it "hands on_error the error of a refused handshake's body that fails to close", :quiet do
    reported = reports.port
    on_error = Ractor.shareable_proc { |error, _env| reported << [:error, error.class] }
    app = Ractor.shareable_proc do |env|
      env["rack.upgrade"] = EchoSocket
      [101, {}, (env["PATH_INFO"] == "/refused") ? ReportedBody.new(reported, true) : []]
    end

    with_ws_server(app, websocket_max: 1, on_error: on_error) do |port|
      connect(port)
      expect { connect(port, path: "/refused") }.to raise_error(WsClient::Refused) { |refused|
        expect(refused.status).to eq(503)
      }
      expect(Array.new(2) { reports.next(awaiting: "the body's close and on_error") })
        .to eq([:body_closed, [:error, RuntimeError]])
    end
  end
end
