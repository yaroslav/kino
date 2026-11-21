# frozen_string_literal: true

require "net/http"

RSpec.describe "worker wake hook" do
  include_context "native WebSocket server"

  # Boots each worker the way an app with background work does: the
  # worker thread registers a hook, keeps its waker for the app (/wake
  # and /slow use it), and starts a waker thread that wakes the worker
  # for each message sent to its port, as a receiver thread with news
  # for the worker would. The hook reports [:woke, its thread id], then
  # raises with `:raise`. Reports [:booted, worker thread id, the waker
  # thread's port].
  def booting_with_hook(hook = :report)
    reports = self.reports.port
    Ractor.shareable_proc do |_worker_id|
      waker = Kino::Worker.on_wake do
        reports << [:woke, Thread.current.object_id]
        raise "wake hook failed" if hook == :raise
      end
      Ractor[:spec_waker] = waker
      wakes = Ractor::Port.new
      Thread.new { waker.wake while wakes.receive }
      reports << [:booted, Thread.current.object_id, wakes]
    end
  end

  # Reports [:request, path, thread id] as each request starts. /wake
  # wakes its worker from a new thread and answers with what the wake
  # returned; /slow wakes it that way, then keeps working a while and
  # reports [:finished, "/slow"] before answering.
  def waking_app
    reports = self.reports.port
    Ractor.shareable_proc do |env|
      path = env["PATH_INFO"]
      reports << [:request, path, Thread.current.object_id]
      waker = Ractor[:spec_waker]
      case path
      when "/wake" then [200, {}, [Thread.new { waker.wake }.value.to_s]]
      when "/slow"
        Thread.new { waker.wake }.join
        sleep 0.1
        reports << [:finished, path]
        [200, {}, ["slow"]]
      else
        [200, {}, ["ok"]]
      end
    end
  end

  # One worker ractor, so every report comes from it. Yields the port
  # and the first boot report.
  def with_wake_server(boot: booting_with_hook, **opts)
    with_ws_server(waking_app, workers: 1, after_worker_boot: boot, **opts) do |port|
      yield port, reports.next(awaiting: "boot report")
    end
  end

  # Boots each worker the way an app that hands its waker out does: the
  # hook reports [:woke, worker id, whether it ran on the thread that
  # registered it], and the boot reports [:booted, worker id, waker].
  def handing_out_wakers
    reports = self.reports.port
    Ractor.shareable_proc do |worker_id|
      registered_on = Thread.current.object_id
      waker = Kino::Worker.on_wake { reports << [:woke, worker_id, Thread.current.object_id == registered_on] }
      reports << [:booted, worker_id, waker]
    end
  end

  # The next boot report's [worker id, waker], past any wake reports.
  def next_waker
    loop do
      report = reports.next(awaiting: "a boot report with a waker")
      return report.drop(1) if report.first == :booted
    end
  end

  it "never runs the hook alongside a request" do
    with_wake_server do |port, (_, worker_thread, _)|
      get(port, "/slow")

      expect(Array.new(3) { reports.next }).to eq([
        [:request, "/slow", worker_thread],
        [:finished, "/slow"],
        [:woke, worker_thread]
      ])
    end
  end

  it "logs a hook exception with its backtrace, hands it to on_error, and keeps serving" do
    reports = self.reports.port
    on_error = Ractor.shareable_proc { |error, env| reports << [:on_error, error.message, env] }

    log = capture_native_stderr do
      with_wake_server(boot: booting_with_hook(:raise), on_error: on_error) do |port, (_, worker_thread, wakes)|
        wakes << :wake
        expect(self.reports.next(awaiting: "report from the wake hook")).to eq([:woke, worker_thread])
        expect(self.reports.next(awaiting: "on_error call"))
          .to eq([:on_error, "wake hook failed", {"kino.callback" => "on_wake"}])

        expect(get(port, "/next")).to eq("ok")
        served = self.reports.next
        expect(served).to eq([:request, "/next", worker_thread]),
          "the worker whose hook raised must keep serving; next report: #{served.inspect}"
      end
    end

    expect(log.scan(/on_wake hook · RuntimeError: wake hook failed \(spec\/worker_wake_spec\.rb:\d+/).size).to eq(1), log
  end

  each_mode do |mode|
    it "hands out frozen, shareable wakers, each waking its own worker's hook on that worker's thread from " \
      "another Ractor, until the worker exits" do
      with_ws_server(waking_app, mode:, workers: 1, threads: 2, after_worker_boot: handing_out_wakers) do |_port, server|
        wakers = Array.new(2) { next_waker }
        wakers.each do |worker_id, waker|
          expect(waker).to be_frozen
          expect(Ractor.shareable?(waker)).to be(true)
          expect(Ractor.new(waker) { |w| w.wake }.value).to be(true)
          expect(reports.next(awaiting: "report from the wake hook")).to eq([:woke, worker_id, true]),
            "waking worker #{worker_id} must run its own hook on its own thread"
        end

        server.shutdown

        expect(wakers.map { |_, waker| [waker.wake, Ractor.new(waker) { |w| w.wake }.value] }).to all(eq([false, false]))
      end
    end

    it "has a retired worker's waker return false, also once its slot went to a new worker" do
      FileGate.closed do |gate|
        app = Ractor.shareable_proc do |env|
          case env["PATH_INFO"]
          when "/slow" then Kino.sleep(0.3)
          when "/held" then gate.wait
          end
          [200, {}, ["ok"]]
        end
        requests = ->(port, path, count) { Array.new(count) { Thread.new { Net::HTTP.get("127.0.0.1", path, port) } } }
        exits = Thread::Queue.new
        with_ws_server(app, mode: mode, workers: 1, max_workers: 2, scale_down_after: 0.2,
          after_worker_boot: handing_out_wakers, on_worker_exit: ->(*) { exits << :exited }) do |port, server|
          wakers = [next_waker].to_h
          requests.call(port, "/slow", 4).each(&:join)
          wakers.store(*next_waker)
          wait_for("the pool to shrink") { server.stats[:scale_downs] == 1 }
          retired_id, retired = wakers.find { |_id, waker| !waker.wake }
          expect(retired).not_to be_nil, "the retired worker's waker still wakes it"
          exits.pop(timeout: WsClient::DEADLINE) or raise "the retired worker never left"

          # Two requests held at the gate keep both workers busy, so the new
          # one cannot retire before its waker is checked.
          held = requests.call(port, "/held", 2)
          worker_id, waker = next_waker

          expect(worker_id).to eq(retired_id), "the new worker must take over the retired one's slot"
          expect(waker.wake).to be(true)
          expect(retired.wake).to be(false), "a waker of the worker that left must not wake its successor"
        ensure
          gate.open
          held&.each(&:join)
        end
      end
    end
  end

  it "warns once per server without lanes, and its wakers wake nothing" do
    log = capture_native_stderr do
      with_ws_server(waking_app, lanes: false, workers: 2, threads: 2, after_worker_boot: booting_with_hook) do |port|
        4.times { reports.next(awaiting: "a boot report from each worker thread") }
        expect(get(port, "/wake")).to eq("false")
      end
    end

    expect(log.scan("on_wake").size).to eq(1), log
    expect(log).to include("lanes true")
  end

  it "refuses a hook from a thread that is not a worker" do
    expect { Kino::Worker.on_wake { nil } }.to raise_error(ArgumentError, /worker thread/)
  end
end
