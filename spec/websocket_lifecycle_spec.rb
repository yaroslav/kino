# frozen_string_literal: true

require "net/http"
require "timeout"

# Reports [callback, socket id, worker] and echoes; "hold" keeps its
# worker busy for a second before the echo. The worker is named by its
# ractor in :ractor mode and by its thread in :threaded mode.
LifecycleSocket = Data.define(:port) do
  def on_open(ws) = port << [:open, ws.id, worker]

  def on_message(ws, data)
    Kino.sleep(1) if data == "hold"
    ws.write(data)
  end

  def on_close(ws) = port << [:close, ws.id, worker]

  def worker = Ractor.current.name || Thread.current.name
end

# What a drain asks of a handler: on_shutdown reports [:shutdown,
# socket id] and says goodbye; on_close reports [:close, socket id,
# what a write returned], then raises.
DrainedSocket = Data.define(:port) do
  def on_shutdown(ws)
    port << [:shutdown, ws.id]
    ws.write("goodbye")
  end

  def on_close(ws)
    port << [:close, ws.id, ws.write("too late")]
    raise "on_close of socket #{ws.id} failed"
  end
end

# On any message, writes 1 MiB frames until a write fails (at most 64),
# and reports how many went through.
QueueFillingSocket = Data.define(:port) do
  def on_message(ws, _data)
    frame = "x" * 1_048_576
    port << (1..64).take_while { ws.write(frame) }.size
  end
end

# on_close takes `pause` seconds, then reports [:close, socket id].
SlowCloseSocket = Data.define(:port, :pause) do
  def on_close(ws)
    Kino.sleep(pause)
    port << [:close, ws.id]
  end
end

# Reports [:open, socket id, its worker thread], and answers each
# message with it and the thread that ran on_message; "crash" raises an
# Exception, which takes that thread down.
ThreadBoundSocket = Data.define(:port) do
  def on_open(ws) = port << [:open, ws.id, Thread.current.object_id]

  def on_message(ws, data)
    raise Exception, "socket #{ws.id} took its thread down" if data == "crash" # rubocop:disable Lint/RaiseException

    ws.write("#{data} on #{Thread.current.object_id}")
  end
end

# Echoes; "wedge" holds its worker in on_message until `gate` opens.
WedgingSocket = Data.define(:gate) do
  def on_message(ws, data)
    gate.wait if data == "wedge"
    ws.write(data)
  end
end

RSpec.describe "WebSocket lifecycle" do
  include_context "native WebSocket server"

  # Shutdown on a thread of its own, whose value is how long it took.
  def shutting_down(server, timeout)
    Thread.new { timed { server.shutdown(timeout: timeout) } }
  end

  # A handshake waits while a crashed worker is replaced, but one that
  # reached the dead worker's lane just before its cleanup is answered
  # 500: retry until a worker serves it.
  def connect_when_served(port)
    deadline = monotonic + WsClient::DEADLINE
    begin
      connect(port)
    rescue WsClient::Refused
      raise if monotonic > deadline

      sleep 0.05
      retry
    end
  end

  # A drain with no deadline at all: it waits for its sockets, however
  # long they take; the Timeout fails a drain that never ends.
  [{mode: :ractor}, {mode: :threaded}, {mode: :ractor, io_shards: true}].each do |opts|
    it "drains each socket: on_shutdown, whose write goes first, a 1001 spread over a second, and on_close, " \
      "then returns once the clients answered (#{opts})" do
      log = Timeout.timeout(20) do
        capture_native_stderr do
          with_ws_server(ws_app(DrainedSocket.new(port: reports.port)), **opts) do |port, server|
            sockets = Array.new(4) { connect(port) }

            started = monotonic
            shutdown = shutting_down(server, Float::INFINITY)
            received = sockets.map do |client|
              Thread.new do
                frames = [client.read, client.read]
                arrived = monotonic - started
                sleep 0.3 # the client takes its time to answer the close
                client.disconnect
                [frames, arrived]
              end
            end.map(&:value)
            expect(shutdown.join(5)).to be_truthy, "the drain outlived its clients"

            expect(received.map(&:first)).to all(eq([[WsClient::TEXT, "goodbye"], [WsClient::CLOSE, [1001].pack("n")]]))
            arrivals = received.map(&:last).sort
            # Four turns, 0.25 s apart: the last one's is a floor the server sleeps for, where
            # the first 1001 waits on a worker and may come late.
            expect(arrivals.last).to be >= 0.5, "the 1001s came together: #{arrivals}"
            expect(arrivals.last).to be < 1.5, "the last 1001 came after the spread: #{arrivals}"
            expect(shutdown.value).to be >= 0.3, "the shutdown did not wait for the clients"
            said = Array.new(8) { reports.next(awaiting: "on_shutdown and on_close") }.group_by { |_, id| id }
            expect(said.size).to eq(4)
            said.each { |id, events| expect(events).to eq([[:shutdown, id], [:close, id, false]]) }
          end
        end
      end

      expect(log.scan(/on_close of socket \d+ failed/).size).to eq(4), log
    end
  end

  each_mode do |mode|
    it "spreads the drain's 1001s over half its deadline, and returns by it past clients that never answer the " \
      "close and one that stopped reading" do
      with_ws_server(ws_app(QueueFillingSocket.new(port: reports.port)), mode:) do |port, server|
        silent = Array.new(4) { connect(port) }
        stalled = connect(port)
        stalled.send_text("fill")
        reports.next(awaiting: "the count of writes that went through")
        # It closes without reading: the server's writer is stuck behind
        # megabytes the client will never take.
        stalled.send_frame(WsClient::CLOSE, [1000].pack("n"))

        started = monotonic
        shutdown = shutting_down(server, 0.4)
        arrivals = silent.map { |client| Thread.new { [client.read, monotonic - started] } }.map(&:value)

        # Over 0.2 s, the last at 0.15 s; over a second, it would come at
        # 0.75 s.
        expect(arrivals.map(&:first)).to all(eq([WsClient::CLOSE, [1001].pack("n")]))
        expect(arrivals.map(&:last).max).to be < 0.5, "the 1001s took more than half the drain: #{arrivals}"
        expect(shutdown.value).to be < 2.5, "shutdown must return by deadline + epsilon, took #{shutdown.value.round(2)}s"
      end
    end
  end

  # Thirty sockets on one worker, whose on_close takes 50 ms. The 1001s go
  # out over a second, faster than the worker runs on_close, so the queue
  # closes with more than a run of closes still in its worker's inbox.
  each_mode do |mode|
    it "runs on_close for every socket when the drain leaves their worker behind" do
      count = 30
      app = ws_app(SlowCloseSocket.new(port: reports.port, pause: 0.05))
      with_ws_server(app, mode:, workers: 1) do |port, server|
        sockets = Array.new(count) { connect(port) }

        shutdown = shutting_down(server, 10)
        sockets.map { |client| Thread.new { client.await_close.tap { client.disconnect } } }.each(&:join)
        elapsed = shutdown.value

        ran = Array.new(count) do |index|
          reports.next(timeout: 1, awaiting: "on_close #{index + 1} of #{count} once the drain had returned")
        end
        expect(ran.map(&:last).uniq.size).to eq(count)
        expect(elapsed).to be_between(count * 0.05, 10),
          "shutdown took #{elapsed.round(2)}s: it must wait for every on_close, and no longer than its deadline"
      end
    end
  end

  each_mode do |mode|
    # One worker of two threads; four sockets spread over both. A crash in
    # one socket's callback takes down its thread alone.
    it "closes the sockets of a worker thread that crashed with 1011, keeps its sibling's, and serves new ones " \
      "on its replacement" do
      exits = Thread::Queue.new
      exited = ->(_worker, error) { exits << error&.message }
      log = capture_native_stderr do
        app = ws_app(ThreadBoundSocket.new(port: reports.port))
        with_ws_server(app, mode: mode, workers: 1, threads: 2, on_worker_exit: exited) do |port|
          sockets = Array.new(4) { connect(port) }
          threads = Array.new(4) { reports.next(awaiting: "on_open")[2] }
          expect(threads.uniq.size).to eq(2), "the sockets did not spread over both threads: #{threads}"

          sockets.first.send_text("crash")

          sockets.zip(threads).each do |client, thread|
            if thread == threads.first
              expect(client.await_close).to eq(1011)
            else
              client.send_text("still here")
              expect(client.read).to eq([WsClient::TEXT, "still here on #{thread}"]),
                "a socket of the thread that lived, served by the thread that accepted it"
            end
          end
          expect(exits.pop(timeout: WsClient::DEADLINE)).to match(/took its thread down/)
          fresh = connect_when_served(port)
          fresh.send_text("hello again")
          expect(fresh.read.last).to start_with("hello again on ")
        end
      end

      expect(log.scan("took its thread down (").size).to eq(1), log
    end

    # The worker never comes back from on_message: its sockets close as a
    # crashed worker's do, and new ones go to its replacement.
    it "closes a wedged worker's sockets with 1011 once quarantine gives it up" do
      FileGate.closed do |gate|
        with_ws_server(ws_app(WedgingSocket.new(gate:)), mode: mode, workers: 1, quarantine_timeout: 0.2) do |port, server|
          wedger = connect(port)
          bystander = connect(port)

          wedger.send_text("wedge")

          expect(wedger.await_close).to eq(1011)
          expect(bystander.await_close).to eq(1011)
          expect(server.stats[:quarantined]).to eq(1)
          fresh = connect_when_served(port)
          fresh.send_text("hello again")
          expect(fresh.read).to eq([WsClient::TEXT, "hello again"])
        ensure
          gate.open
        end
      end
    end

    it "keeps a worker that owns open sockets through scale-down and retires it once they close" do
      app = ws_app(LifecycleSocket.new(port: reports.port))
      with_ws_server(app, mode: mode, workers: 1, threads: 1, max_workers: 2, scale_down_after: 0.2) do |port, server|
        first = connect(port)
        _, _, first_worker = reports.next(awaiting: "on_open")

        # Requests queue behind the held worker, so the pool grows; the new
        # worker owns no socket yet, so it takes the next one.
        first.send_text("hold")
        requests = Array.new(2) { Thread.new { Net::HTTP.get("127.0.0.1", "/", port) } }
        wait_for("the pool to grow") { server.stats[:scale_ups] == 1 }
        expect(requests.map(&:value)).to eq(%w[plain plain])
        extra = connect(port)
        _, extra_id, extra_worker = reports.next(awaiting: "on_open")
        expect(extra_worker).not_to eq(first_worker)
        expect(first.read).to eq([WsClient::TEXT, "hold"])

        # Both workers sit idle for five scale-down windows, each owning a
        # socket: neither retires.
        sleep 1
        expect(server.stats).to include(active_workers: 2, scale_downs: 0)

        expect(extra.close).to eq(1000)
        expect(reports.next(awaiting: "on_close")).to eq([:close, extra_id, extra_worker])
        wait_for("the worker without sockets to retire") { server.stats[:active_workers] == 1 }
        expect(server.stats[:scale_downs]).to eq(1)
        first.send_text("still here")
        expect(first.read).to eq([WsClient::TEXT, "still here"]), "the retirement must leave the other worker's socket open"
      end
    end

    # A socket that opens after the scaler's sample found its worker idle
    # meets the retirement itself.
    it "runs on_close for a retiring worker's sockets, closed with 1001, before the worker leaves" do
      events = reports.port
      exited = ->(_index, cause) { events << [:exit, cause] }
      app = ws_app(LifecycleSocket.new(port: events))
      with_ws_server(app, mode: mode, workers: 1, threads: 1, on_worker_exit: exited) do |port, server|
        client = connect(port)
        _, id, worker = reports.next(awaiting: "on_open")

        retire_slot(server, 0)

        expect(client.await_close).to eq(1001)
        client.disconnect
        expect(reports.next(awaiting: "on_close")).to eq([:close, id, worker])
        expect(reports.next(awaiting: "the worker's exit")).to eq([:exit, nil])
      end
    end
  end
end
