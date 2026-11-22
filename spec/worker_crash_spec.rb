# frozen_string_literal: true

require "net/http"

RSpec.describe Kino::CrashBackoff do
  def boom = RuntimeError.new("boom").tap { |error| error.set_backtrace(["app.rb:3:in 'explode'"]) }

  it "doubles the delay from 0.1 s up to 5 s while the worker keeps crashing within a second of starting" do
    backoff = described_class.new("worker-0", 0.0)
    delays = []
    capture_native_stderr do
      now = 0.0
      8.times do
        delays << backoff.crashed(boom, now + 0.5)
        now += 0.5 + delays.last
        backoff.started(now)
      end
    end

    expect(delays).to eq([0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0])
  end

  it "respawns at once, and counts afresh, a worker that served a request or lived a second, however it crashed" do
    backoff = described_class.new("worker-0", 0.0)
    log = capture_native_stderr do
      expect(backoff.crashed(boom, 0.1)).to eq(0.1)
      backoff.started(0.2)
      expect(backoff.crashed(boom, 0.3, served: true)).to eq(0.0)
      backoff.started(0.3)
      expect(backoff.crashed(boom, 0.4)).to eq(0.1)
      backoff.started(0.5)
      expect(backoff.crashed(boom, 1.5)).to eq(0.0)
      backoff.started(1.5)
      expect(backoff.crashed(boom, 1.6)).to eq(0.1)
    end

    expect(log.lines.grep(/worker-0 crashed/)).to match([
      /worker-0 crashed within a second of starting; respawning in 0\.1 s · RuntimeError: boom \(app\.rb:3/,
      /worker-0 crashed; respawning at once · RuntimeError: boom/,
      /worker-0 crashed within a second of starting; respawning in 0\.1 s · RuntimeError: boom/,
      /worker-0 crashed; respawning at once · RuntimeError: boom/,
      /worker-0 crashed within a second of starting; respawning in 0\.1 s · RuntimeError: boom/
    ])
  end

  it "logs the first crash of a run with its backtrace and each later one in a line" do
    backoff = described_class.new("worker-0", 0.0)
    log = capture_native_stderr do
      3.times { |n| backoff.crashed(boom, n * 0.1) }
      backoff.crashed(boom, 0.4, respawn: false)
    end

    expect(log.scan("    app.rb:3:in 'explode'").size).to eq(1), log
    expect(log).to include("worker-0 crashed within a second of starting again, 2 times in a row " \
      "(RuntimeError: boom); respawning in 0.2 s")
    expect(log).to include("again, 4 times in a row (RuntimeError: boom)\n")
  end
end

# A hard crash: an Exception that no rescue in the worker catches, or a
# stack overflow.
RSpec.describe "worker crashes" do
  def crashing_app
    Ractor.shareable_proc do |env|
      raise Exception, "crashed at /boom" if env["PATH_INFO"] == "/boom" # rubocop:disable Lint/RaiseException
      if env["PATH_INFO"] == "/deep"
        deep = ->(depth) { deep.call(depth + 1) + 1 }
        deep.call(0)
      end

      [200, {"content-type" => "text/plain"}, ["ok"]]
    end
  end

  def get_response(port, path)
    Net::HTTP.start("127.0.0.1", port, read_timeout: 5) { |http| http.get(path) }
  end

  # On "/crash", once `gate` (if any) opens, crashes its worker thread,
  # or with `ractor: true` takes its whole ractor down.
  def gated_crash_app(gate, ractor: false)
    Ractor.shareable_proc do |env|
      if env["PATH_INFO"] == "/crash"
        gate&.wait(10)
        raise Exception, "crashed" unless ractor # rubocop:disable Lint/RaiseException

        Thread.main.raise(Exception, "took its ractor down")
        Kino.sleep(5)
      end
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end
  end

  def slot_rows(server) = Kino::WorkerRow.all(server_id(server))

  def slot_row(server, id) = slot_rows(server).find { |row| row.slot == id }

  # Whether slot `id` went back to work: off the bank to a new worker,
  # neither retired nor quarantined.
  def back_at_work?(server, id)
    row = slot_row(server, id)
    !row.retired && !row.quarantined
  end

  # Tells the pool to retire the worker of the one request in flight,
  # held at its gate; returns the pool.
  def retire_busy_worker(server)
    # The server counts a request in flight before a worker has taken it.
    busy = nil
    wait_for("a worker holding the request") do
      busy = server.stats[:worker_status].find { |worker| worker[:in_flight].positive? }
    end
    pool = server.instance_variable_get(:@pool)
    slot = busy[:index]
    wait_for("the busy worker listed as retirable") { pool.groups.any? { |_index, slots| slots.include?(slot) } }
    index = pool.groups.find { |_index, slots| slots.include?(slot) }.first
    expect(pool.retire(index)).to be(true)
    pool
  end

  each_mode do |mode|
    [{}, {lanes: true}].each do |lanes|
      it "500s an Exception and a stack overflow at once, logs each once, replaces the workers on their slots, " \
        "and holds up no shutdown#{" (lanes)" unless lanes.empty?}" do
        exits = Thread::Queue.new
        server = nil
        log = capture_native_stderr do
          server = Kino::Server.new(crashing_app, mode: mode, workers: 2, threads: 1, shutdown_timeout: 30,
            on_worker_exit: ->(_worker, error) { exits << error&.class }, **lanes).start
          waiting = Thread.new { server.wait }

          expect(get_response(server.port, "/boom").code).to eq("500")
          expect(get_response(server.port, "/deep").code).to eq("500")
          expect(Array.new(2) { exits.pop(timeout: 5) }).to contain_exactly(Exception, SystemStackError)
          wait_for("both crashed workers replaced") { server.stats[:respawns] == 2 }
          expect(Array.new(4) { get_response(server.port, "/").code }).to all(eq("200"))
          wait_for("nothing in flight") { server.stats[:in_flight].zero? }
          expect(server.stats[:worker_status].sum { |worker| worker[:in_flight] }).to eq(0),
            "a phantom in_flight on a crashed worker's slot"
          expect(server.stats[:worker_status].size).to eq(2), "the replacements took slots of their own"

          took = timed { server.shutdown }
          expect(took).to be < 5, "shutdown_timeout is 30 s; the shutdown took #{took.round(2)} s"
          expect(waiting.join(5)).to be_truthy, "Server#wait must return, not raise the crash"
        end

        expect(log.scan("Exception: crashed at /boom (").size).to eq(1), log
        expect(log).not_to include("terminated with exception", "stuck ractor workers")
      ensure
        server&.shutdown(timeout: 1)
      end
    end

    # The handler waits on a body the client never sends; the shutdown's
    # interrupt ends that wait, and its ensure then crashes the worker.
    it "replaces no worker that crashes while the pool drains" do
      app = Ractor.shareable_proc do |env|
        env["rack.input"].read
        [200, {}, []]
      ensure
        raise Exception, "crashed on the way out" # rubocop:disable Lint/RaiseException
      end
      server = client = nil
      log = capture_native_stderr do
        server = Kino::Server.new(app, mode: mode, workers: 1, threads: 1).start
        client = TCPSocket.new("127.0.0.1", server.port)
        client.write("POST / HTTP/1.1\r\nHost: kino\r\nContent-Length: 10\r\n\r\n")
        wait_for("the request in flight") { server.stats[:in_flight] == 1 }

        expect(timed { server.shutdown(timeout: 0.3) }).to be < 3
        expect(client.read).to start_with("HTTP/1.1 500")
      end

      expect(log).to include("crashed on the way out")
      expect(log).not_to include("respawning")
    ensure
      client&.close
      server&.shutdown(timeout: 1)
    end

    it "backs off respawning each worker thread that crashes as it boots, at its own pace, " \
      "and stops waiting at shutdown" do
      threads = 2
      # Each thread boots five times, 0.1, 0.2, 0.4 and 0.8 s apart; its fifth crash starts a 1.6 s wait.
      rounds = 5
      exits = Thread::Queue.new
      boots = HandlerReports.new
      booted = boots.port
      boot = Ractor.shareable_proc do |_id|
        booted << Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise Exception, "boot failed" # rubocop:disable Lint/RaiseException
      end
      shutdown_took = nil
      log = capture_native_stderr do
        server = Kino::Server.new(crashing_app, mode: mode, workers: 1, threads: threads, after_worker_boot: boot,
          on_worker_exit: ->(_worker, error) { exits << error }).start
        wait_for("each thread's crash number #{rounds}") { exits.size >= rounds * threads }
        shutdown_took = timed { server.shutdown(timeout: 10) }
      end
      crashes = Array.new(exits.size) { exits.pop }.compact.size
      boot_times = []
      boot_times << boots.next until boots.empty?
      boots.stop

      expect(crashes).to be_between(rounds * threads, (rounds + 1) * threads)
      expect(shutdown_took).to be < 1, "shutdown waited #{shutdown_took.round(2)} s for a respawn"
      %w[worker-0/thread-1 worker-0/thread-2].each do |lineage|
        first = log.scan("#{lineage} crashed within a second of starting; respawning in 0.1 s · " \
          "Exception: boot failed (")
        expect(first.size).to eq(1), "#{lineage}: #{log}"
        counts = log.scan(/#{lineage} crashed within a second of starting again, (\d+) times in a row/).flatten
        expect(counts.map(&:to_i)).to eq((2..counts.size + 1).to_a), "#{lineage}: #{log}"
      end
      expect(log.scan("Exception: boot failed (").size).to eq(threads), log
      # Paced each by its own crashes, the threads boot again together; one waiting behind its
      # sibling's delay would trail it by that delay. Measured on the fifth boots, whose delay,
      # the longest, is the time since the fourth: oversleeping stretches both alike.
      fourth, fifth = boot_times.sort.each_slice(threads).first(rounds).last(2)
      delay = fifth.min - fourth.min
      expect(fifth.max - fifth.min).to be < delay / 2,
        "boots #{boot_times.sort.map { |t| (t - boot_times.min).round(2) }}"
    end

    it "respawns at once, on its own slot, a young worker that crashed on a request, since it booted fine" do
      log = capture_native_stderr do
        with_server(crashing_app, mode: mode, workers: 1, threads: 1) do |_host, port, server|
          started = monotonic
          codes = Array.new(10) { get_response(port, "/boom").code }
          expect(get_response(port, "/").body).to eq("ok")

          expect(codes).to eq(%w[500] * 10)
          expect(monotonic - started).to be < 1.5, "a crash on a request was held back"
          expect(server.stats).to include(respawns: 10)
          expect(server.stats[:worker_status].size).to eq(1), "the crashes grew the slot table"
        end
      end

      expect(log.scan("crashed; respawning at once · Exception: crashed at /boom (").size).to eq(10), log
    end

    it "leaves alone the worker that took over a crashed worker's slot when a retirement or quarantine meant " \
      "for the crashed one comes late", :quiet do
      with_server(crashing_app, mode: mode, workers: 1, threads: 1) do |_host, port, server|
        id = server_id(server)
        expect(get_response(port, "/").body).to eq("ok")
        sampled = slot_rows(server).first

        get_response(port, "/boom")
        expect(get_response(port, "/").body).to eq("ok")
        expect(slot_rows(server).map(&:slot)).to eq([sampled.slot]), "the replacement took the same slot"

        expect(Kino::Native.retire_slot(id, sampled.slot, sampled.generation)).to be(false)
        expect(server.instance_variable_get(:@pool).replace(sampled.slot, sampled.generation)).to be(false)
        expect(get_response(port, "/").body).to eq("ok")
        expect(server.stats[:worker_status]).to contain_exactly(include(quarantined: false, retired: false))
      end
    end

    # A ractor dies, rather than one of its threads, when the handler
    # raises into the ractor's main thread.
    ((mode == :ractor) ? %w[thread ractor] : %w[thread]).each do |dying|
      it "does not replace again a worker that quarantine replaced, when its #{dying} dies later" do
        FileGate.closed do |gate|
          exits = Thread::Queue.new
          log = capture_native_stderr do
            with_server(gated_crash_app(gate, ractor: dying == "ractor"), mode: mode, workers: 1, threads: 1,
              quarantine_timeout: 0.2, on_worker_exit: ->(_worker, error) { exits << error if error }) do |_host, port, server|
              wedged = Thread.new { get_response(port, "/crash") }
              wait_for("the wedged worker's quarantine") { server.stats[:quarantined] == 1 }
              expect(get_response(port, "/").body).to eq("ok")
              slots = server.stats[:worker_status].size

              gate.open
              expect(wedged.value.code).to eq("500")
              expect(exits.pop(timeout: 5)&.message).to eq((dying == "ractor") ? "took its ractor down" : "crashed")
              sleep 0.3 # past the first respawn delay
              expect(server.stats).to include(respawns: 0, active_workers: 1)
              expect(server.stats[:worker_status].size).to eq(slots), "a respawn took a slot of its own"
              expect(get_response(port, "/").body).to eq("ok")
            ensure
              gate.open
            end
          end

          expect(log).to match(/worker-\d crashed[^·\n]* · Exception: (crashed|took its ractor down) \(/)
          expect(log).not_to include("respawning")
        end
      end
    end

    it "gives a crashed thread's slot back to the bank when quarantine replaces its worker before the crash " \
      "is recovered", :quiet do
      held = Thread::Queue.new
      resume = Thread::Queue.new
      with_server(crashing_app, mode: mode, workers: 1, threads: 2) do |_host, port, server|
        pool = server.instance_variable_get(:@pool)
        # The pool's recovery of the crash waits just before it forgets
        # the crashed slots, until `resume` opens.
        allow(pool).to receive(:forget_slots).and_wrap_original do |forget, index, ids|
          held << ids
          resume.pop
          forget.call(index, ids)
        end

        expect(get_response(port, "/boom").code).to eq("500")
        crashed = held.pop(timeout: 5)&.first
        expect(crashed).not_to be_nil, "the crash's recovery never started"
        sibling = slot_rows(server).find { |row| row.slot != crashed }
        # In ractor mode the sibling is a slot of the crashed thread's
        # own ractor: the replacement reads the ractor's slots while
        # the crashed one is still among them.
        expect(pool.replace(sibling.slot, sibling.generation)).to be(true)
        resume << :go

        # The crashed thread's respawn takes the slot from the bank in
        # threaded mode; in ractor mode, its worker replaced, a new one.
        pool.grow
        wait_for("the crashed thread's slot handed out again") { back_at_work?(server, crashed) }
        expect(get_response(port, "/").body).to eq("ok")
      ensure
        resume << :go
      end
    end

    it "does not replace a worker thread that crashes while the pool retires its worker" do
      FileGate.closed do |gate|
        log = capture_native_stderr do
          with_server(gated_crash_app(gate), mode: mode, workers: 2, threads: 1) do |_host, port, server|
            held = Thread.new { get_response(port, "/crash") }
            pool = retire_busy_worker(server)

            gate.open
            expect(held.value.code).to eq("500")
            wait_for("the crash logged") { server.stats[:in_flight].zero? }
            sleep 0.3 # past the first respawn delay
            expect(server.stats).to include(respawns: 0)
            expect(pool.active_count).to eq(1)
            expect(get_response(port, "/").body).to eq("ok")
          ensure
            gate.open
          end
        end

        expect(log).to match(/worker-\d crashed · Exception: crashed \(/)
        expect(log).not_to include("respawning")
      end
    end

    if mode == :ractor
      it "500s what a dying worker ractor held, and respawns it at once on its own slot once it served" do
        app = gated_crash_app(nil, ractor: true)
        exits = Thread::Queue.new
        log = capture_native_stderr do
          with_server(app, mode: mode, workers: 1, threads: 1,
            on_worker_exit: ->(_worker, error) { exits << error if error }) do |_host, port, server|
            expect(get_response(port, "/").body).to eq("ok")
            slots = server.stats[:worker_status].map { |worker| worker[:index] }

            expect(get_response(port, "/crash").code).to eq("500")
            expect(exits.pop(timeout: 5)&.message).to eq("took its ractor down")
            wait_for("the dead ractor respawned") { server.stats[:respawns] == 1 }
            expect(get_response(port, "/").body).to eq("ok")
            expect(server.stats[:worker_status].map { |worker| worker[:index] }).to eq(slots)
          end
        end

        expect(log).to include("worker-0 crashed; respawning at once · Exception: took its ractor down (")
      end

      it "backs off respawning a worker ractor that dies as it boots, and stops waiting at shutdown" do
        # It boots five times, at least 0.1, 0.2, 0.4 and 0.8 s apart; its fifth death starts a 1.6 s wait.
        delays = [0.1, 0.2, 0.4, 0.8]
        boots = HandlerReports.new
        booted = boots.port
        boot = Ractor.shareable_proc do |_id|
          booted << Process.clock_gettime(Process::CLOCK_MONOTONIC)
          Thread.main.raise(Exception, "ractor boot failed")
          Kino.sleep(5)
        end
        exits = Thread::Queue.new
        shutdown_took = nil
        log = capture_native_stderr do
          server = Kino::Server.new(crashing_app, mode: mode, workers: 1, threads: 1, after_worker_boot: boot,
            on_worker_exit: ->(_worker, error) { exits << error }).start
          wait_for("the ractor's death number #{delays.size + 1}", timeout: 20) { exits.size > delays.size }
          shutdown_took = timed { server.shutdown(timeout: 10) }
        end
        crashes = Array.new(exits.size) { exits.pop }.compact
        boots.stop # every report is through once this returns
        boot_times = []
        boot_times << boots.next until boots.empty?

        expect(crashes.size).to be_between(delays.size + 1, delays.size + 2), log
        expect(crashes.map(&:message).uniq).to eq(["ractor boot failed"])
        # Each delay is a floor on the time between two boots, however late a slow machine runs.
        gaps = boot_times.sort.each_cons(2).map { |earlier, later| later - earlier }
        delays.zip(gaps).each do |delay, gap|
          expect(gap).to be >= delay, "boots #{gaps.map { |g| g.round(2) }} apart, not backing off by #{delays}"
        end
        expect(shutdown_took).to be < 1, "shutdown waited #{shutdown_took.round(2)} s for a respawn"
      end

      it "gives a worker ractor's slots back to the bank when it dies while quarantine replaces it", :quiet do
        forgotten = Thread::Queue.new
        with_server(gated_crash_app(nil, ractor: true), mode: mode, workers: 1, threads: 1) do |_host, port, server|
          pool = server.instance_variable_get(:@pool)
          expect(get_response(port, "/").body).to eq("ok")
          sampled = slot_rows(server).first
          allow(pool).to receive(:forget_slots).and_wrap_original do |forget, *dead|
            forget.call(*dead).tap { forgotten << dead }
          end
          # The ractor dies, its thread leaves the slot and the slot is
          # forgotten, after the replacement read the ractor's slots
          # and before it quarantines them.
          allow(pool).to receive(:supervise).and_wrap_original do |supervise, index|
            expect(get_response(port, "/crash").code).to eq("500")
            expect(forgotten.pop(timeout: 5)).not_to be_nil, "the dead ractor's slots were never forgotten"
            wait_for("the dead ractor's thread to leave its slot") do
              slot_row(server, sampled.slot).generation != sampled.generation
            end
            supervise.call(index)
          end
          expect(pool.replace(sampled.slot, sampled.generation)).to be(true)
          allow(pool).to receive(:supervise).and_call_original

          pool.grow
          wait_for("the dead ractor's slot handed out again") { back_at_work?(server, sampled.slot) }
          expect(get_response(port, "/").body).to eq("ok")
        end
      end

      it "respawns a worker ractor that dies while the pool retires it" do
        FileGate.closed do |gate|
          log = capture_native_stderr do
            with_server(gated_crash_app(gate, ractor: true), mode: mode, workers: 2, threads: 1) do |_host, port, server|
              held = Thread.new { get_response(port, "/crash") }
              pool = retire_busy_worker(server)
              expect(pool.active_count).to eq(1)

              gate.open
              expect(held.value.code).to eq("500")
              wait_for("the dead ractor respawned") { server.stats[:respawns] == 1 }
              expect(pool.active_count).to eq(2)
              expect(get_response(port, "/").body).to eq("ok")
            ensure
              gate.open
            end
          end

          expect(log).to match(/worker-\d crashed; respawning at once · Exception: took its ractor down \(/)
        end
      end
    end

    it "hands on_worker_exit a crash that cannot leave its ractor as a Kino::Error naming it" do
      exits = Thread::Queue.new
      app = Ractor.shareable_proc do |env|
        raise UncopyableCrash, "crashed at #{env["PATH_INFO"]}" if env["PATH_INFO"] == "/uncopyable"

        [200, {}, ["ok"]]
      end
      log = capture_native_stderr do
        with_server(app, mode: mode, workers: 1, threads: 1,
          on_worker_exit: ->(_worker, error) { exits << error }) do |_host, port|
          get_response(port, "/uncopyable")

          error = exits.pop(timeout: 5)
          if mode == :ractor
            expect(error).to be_a(Kino::Error)
            expect(error.message).to eq("UncopyableCrash: crashed at /uncopyable")
          else
            expect(error).to be_a(UncopyableCrash)
          end
          expect(error.backtrace.first).to include("worker_crash_spec.rb")
          expect(get_response(port, "/").body).to eq("ok")
        end
      end

      expect(log).to include("crashed at /uncopyable (spec/worker_crash_spec.rb:")
    end
  end
end

# A crash that cannot be copied out of a ractor: it holds a Proc.
class UncopyableCrash < Exception # rubocop:disable Lint/InheritException
  def initialize(message)
    super
    @callback = proc {}
  end
end
