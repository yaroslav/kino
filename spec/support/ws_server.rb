# frozen_string_literal: true

# What handlers report from worker ractors, received on a Ractor::Port.
class HandlerReports
  attr_reader :port

  def initialize
    @port = Ractor::Port.new
    @queue = Thread::Queue.new
    @thread = Thread.new do
      while (report = @port.receive) != :done
        @queue << report
      end
    end
  end

  # The next report; `awaiting` names it in the failure if none comes.
  def next(timeout: WsClient::DEADLINE, awaiting: "a report")
    @queue.pop(timeout: timeout) or raise "the handler sent no #{awaiting} within #{timeout}s"
  end

  # True while nothing waits to be taken.
  def empty? = @queue.empty?

  def stop
    @port << :done
    @thread.join
  end
end

# The example's HandlerReports, made on first use and stopped after it.
module Reporting
  def reports = @reports ||= HandlerReports.new
end

RSpec.configure do |config|
  config.include Reporting
  config.after { @reports&.stop }
end

# Sends every message back as it came: text as text, binary as binary.
module EchoSocket
  def self.on_message(ws, data)
    (data.encoding == Encoding::BINARY) ? ws.write_binary(data) : ws.write(data)
  end
end

# An error whose report fails: reading its message raises in turn.
class UnreportableError < StandardError
  def message = raise("the error's message raised")
end

# An error whose message raises an UnreportableError.
class DeeplyUnreportableError < StandardError
  def message = raise(UnreportableError)
end

# Echoes, except that "boom" raises, "unreportable" raises an
# UnreportableError and "deeply unreportable" a DeeplyUnreportableError.
module FragileSocket
  def self.on_message(ws, data)
    raise "boom from socket #{ws.id}" if data == "boom"
    raise UnreportableError if data == "unreportable"
    raise DeeplyUnreportableError if data == "deeply unreportable"

    ws.write(data)
  end
end

# A response body that reports its close; `failing` makes close raise
# after reporting.
ReportedBody = Data.define(:port, :failing) do
  def each = nil

  def close
    port << :body_closed
    raise "body close failed" if failing
  end
end

# Servers and clients for native WebSocket specs. Handlers run inside
# worker ractors, so each is a module or a frozen object holding only
# shareable state.
RSpec.shared_context "native WebSocket server" do
  # Accepts every handshake with `handler` and the 101 `headers`; any
  # other request gets a plain 200.
  def ws_app(handler, headers = {})
    accept_headers = Ractor.make_shareable(headers)
    Ractor.shareable_proc do |env|
      if env["rack.upgrade?"] == :websocket
        env["rack.upgrade"] = handler
        [101, accept_headers, []]
      else
        [200, {"content-type" => "text/plain"}, ["plain"]]
      end
    end
  end

  # Native WebSockets need lanes; two single-thread ractors unless a
  # spec says otherwise. Clients still open are dropped before shutdown.
  def with_ws_server(app, **opts)
    opts = {lanes: true, mode: :ractor, workers: 2, threads: 1}.merge(server_defaults, opts)
    with_server(app, **opts) do |_host, port, server|
      yield port, server
    ensure
      clients.each(&:disconnect)
    end
  end

  def connect(port, **opts)
    WsClient.open("127.0.0.1", port, **opts).tap { |client| clients << client }
  end

  # The handshake's refusal, or nil if the socket opened.
  def refusal(port)
    connect(port)
    nil
  rescue WsClient::Refused => refused
    refused
  end

  def clients = @clients ||= []
end
