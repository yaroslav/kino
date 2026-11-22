# frozen_string_literal: true

require "net/http"
require "timeout"

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
    pool = pool_of(server)
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
        client = client_socket(server.port)
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
        booted << Kino.monotonic
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
        expect(pool_of(server).replace(sampled.slot, sampled.generation)).to be(false)
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
        pool = pool_of(server)
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

      it "counts one respawn when its ractor's main thread is killed as a crashed thread's replacement is on its way" do
        app = Ractor.shareable_proc do |env|
          raise Exception, "crashed at /boom" if env["PATH_INFO"] == "/boom" # rubocop:disable Lint/RaiseException
          Thread.main.kill.then { Kino.sleep(5) } if env["PATH_INFO"] == "/kill-main"
          [200, {}, ["ok"]]
        end
        capture_native_stderr do
          with_server(app, mode: mode, workers: 1, threads: 2) do |_host, port, server|
            expect(get(port, "/")).to eq("ok")
            # The crashed thread's slot leaves the bank, then is held on its way to the thread.
            pool = pool_of(server)
            claimed, resume, answered = Array.new(3) { Thread::Queue.new }
            allow(pool).to receive(:claim_slot).and_wrap_original do |claim, *args, &block|
              claim.call(*args, &block).tap do |id|
                claimed << id
                resume.pop(timeout: 5)
              end
            end
            allow(pool).to receive(:answer).and_wrap_original do |answer, *args|
              answer.call(*args)
            ensure
              answered << true
            end

            expect(get_response(port, "/boom").code).to eq("500")
            expect(claimed.pop(timeout: 5)).not_to be_nil, "no slot was claimed for the crashed thread"
            expect(get_response(port, "/kill-main").code).to eq("500")
            wait_for("the killed ractor respawned") { get(port, "/") == "ok" }
            resume << :go
            expect(answered.pop(timeout: 5)).to be(true)
            expect(server.stats[:respawns]).to eq(1), "the replacement thread never ran, yet counted"
          ensure
            resume&.close
          end
        end
      end

      it "backs off respawning a worker ractor that dies as it boots, and stops waiting at shutdown" do
        # It boots five times, at least 0.1, 0.2, 0.4 and 0.8 s apart; its fifth death starts a 1.6 s wait.
        delays = [0.1, 0.2, 0.4, 0.8]
        boots = HandlerReports.new
        booted = boots.port
        boot = Ractor.shareable_proc do |_id|
          booted << Kino.monotonic
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
          pool = pool_of(server)
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

# Another of the app's threads interrupts the worker while it waits for
# its next request: Thread#wakeup only wakes it (as #run does, which
# also yields to it), and #raise raises there, out of the take, as
# anywhere in the worker.
RSpec.describe "Ruby interrupts into an idle worker" do
  # GET /poke/<how> leaves a thread behind that, each time the worker
  # waits for its next request, pokes it (wakeup: 5 times, 20 ms apart;
  # raise, a RuntimeError, and crash, an Exception: once, a few
  # milliseconds into the wait), then reports :poked.
  def poking_app(reports)
    Ractor.shareable_proc do |env|
      how = env["PATH_INFO"][%r{\A/poke/(\w+)\z}, 1]
      if how
        worker = Thread.current
        Thread.new do
          pokes = %w[raise crash].include?(how) ? 1 : 5
          pokes.times do |poke|
            sleep 0.02 if poke.positive?
            sleep 0.002 while worker.alive? && worker.status != "sleep"
            break unless worker.alive?

            case how
            when "wakeup" then worker.wakeup
            when "raise"
              sleep 0.005
              worker.raise(RuntimeError, "raised into an idle worker")
            when "crash"
              sleep 0.005
              worker.raise(Exception, "crashed an idle worker")
            end
          end
          reports << :poked
        end
      end
      [200, {}, ["ok"]]
    end
  end

  # One worker on one thread, serving poking_app (it reports to `reports`); yields the server
  # and the worker exits (on_worker_exit's errors).
  def with_poked_server(mode, lanes)
    exits = Thread::Queue.new
    server = Kino::Server.new(poking_app(reports.port), mode: mode, workers: 1, threads: 1, **lanes,
      on_worker_exit: ->(_worker, error) { exits << error }).start
    yield server, exits
  ensure
    server&.shutdown(timeout: 1)
  end

  each_mode do |mode|
    [{lanes: false}, {lanes: true}].each do |lanes|
      context "#{lanes[:lanes] ? "with" : "without"} lanes" do
        it "keeps serving after Thread#wakeup on an idle worker, and drains promptly" do
          with_poked_server(mode, lanes) do |server, exits|
            expect(get(server.port, "/poke/wakeup")).to eq("ok")
            expect(reports.next(awaiting: "report of its pokes")).to eq(:poked)

            expect(Array.new(3) { get(server.port, "/") }).to all(eq("ok"))
            expect(Array.new(exits.size) { exits.pop }).to be_empty, "no worker may leave"
            expect(server.stats).to include(respawns: 0)

            # Woken once more, so the drain meets a worker that has taken nothing since.
            expect(get(server.port, "/poke/wakeup")).to eq("ok")
            expect(reports.next(awaiting: "report of its pokes")).to eq(:poked)
            client = client_socket(server.port)
            client.write("GET / HTTP/1.1\r\nHost: kino\r\n\r\n")
            wait_for("the request at the server") do
              stats = server.stats
              stats[:served] + stats[:queued] + stats[:in_flight] + stats.fetch(:lane_depths, []).sum > 5
            end

            took = timed { server.shutdown(timeout: 3) }
            expect(took).to be < 1, "the drain took #{took.round(2)} s"
            expect(client.read).to start_with("HTTP/1.1 200")
          ensure
            client&.close
          end
        end

        # An idle worker waits in a take that rode no answer (its last
        # answer found nothing waiting, and closed its body first), so a
        # StandardError raised out of it crashes the worker, as out of any
        # take that rode no answer; the replacement answers the request
        # sent right after it, which the wait the raise woke put back.
        {"raise" => [RuntimeError, "raised into an idle worker"],
         "crash" => [Exception, "crashed an idle worker"]}.each do |how, (error_class, message)|
          it "respawns an idle worker that Thread#raise crashed with #{error_class}, whose replacement answers " \
            "the request sent right after it" do
            log = capture_native_stderr do
              with_poked_server(mode, lanes) do |server, exits|
                expect(get(server.port, "/poke/#{how}")).to eq("ok")
                expect(reports.next(awaiting: "report of its raise")).to eq(:poked)
                client = client_socket(server.port)
                client.write("GET / HTTP/1.1\r\nHost: kino\r\n\r\n")

                answer = client.wait_readable(2) ? client.readpartial(4096) : "no answer within 2 s"
                expect(answer).to start_with("HTTP/1.1 200")
                error = exits.pop(timeout: 5)
                expect([error.class, error&.message]).to eq([error_class, message])
                wait_for("the replacement counted") { server.stats[:respawns] == 1 }
                expect(server.stats[:served]).to eq(2), "the request was taken more than once"
                expect(Array.new(2) { get(server.port, "/") }).to all(eq("ok"))
                expect(server.stats[:respawns]).to eq(1)
              ensure
                client&.close
              end
            end

            expect(log).to include("crashed").and(include("#{error_class}: #{message}"))
          end
        end
      end
    end
  end
end

# Another of the app's threads interrupts the worker while it waits on
# its client, for the rest of a request body or for room for the next
# chunk of a response: Thread#wakeup only wakes it, and Thread#raise,
# which Timeout.timeout uses, raises in the app as anywhere.
RSpec.describe "Ruby interrupts into a worker waiting on its client" do
  # POST /read reads the body under Timeout.timeout(0.3): 200 with the
  # body, or 408 once the read timed out (with ?hijack, written through
  # rack.hijack); with ?wake, without a timeout, the worker is woken 5
  # times as it reads.
  # GET /write streams 256 KiB chunks under Timeout.timeout(0.3) until
  # it times out; with ?wake, 64 chunks while the worker is woken 5
  # times. Each reports what happened: :timed_out, :hijacked, :woke.
  def client_app(reports)
    Ractor.shareable_proc do |env|
      worker = Thread.current
      wake = lambda do
        Thread.new do
          5.times do
            sleep 0.002 while worker.alive? && worker.status != "sleep"
            worker.wakeup if worker.alive?
            sleep 0.02
          end
          reports << :woke
        end
      end
      wake.call if env["QUERY_STRING"] == "wake"
      case env["PATH_INFO"]
      when "/read"
        next [200, {}, [env["rack.input"].read]] if env["QUERY_STRING"] == "wake"

        begin
          [200, {}, [Timeout.timeout(0.3) { env["rack.input"].read }]]
        rescue Timeout::Error
          reports << :timed_out
          next [408, {}, ["timed out"]] unless env["QUERY_STRING"] == "hijack"

          io = env["rack.hijack"].call
          io.write("HTTP/1.1 408 Request Timeout\r\ncontent-length: 9\r\nconnection: close\r\n\r\ntimed out")
          io.close
          reports << :hijacked
          [200, {}, []]
        end
      when "/write"
        piece = "x" * 262_144
        body = Object.new
        if env["QUERY_STRING"] == "wake"
          body.define_singleton_method(:each) { |&write| 64.times { write.call(piece) } }
        else
          body.define_singleton_method(:each) do |&write|
            Timeout.timeout(0.3) { loop { write.call(piece) } }
          rescue Timeout::Error
            reports << :timed_out
          end
        end
        [200, {}, body]
      else
        [200, {}, ["ok"]]
      end
    end
  end

  # One worker on one thread, serving client_app (it reports to `reports`); yields the server.
  def with_client_server(mode, **opts)
    server = Kino::Server.new(client_app(reports.port), mode: mode, workers: 1, threads: 1, **opts).start
    yield server
  ensure
    server&.shutdown(timeout: 1)
  end

  # A request on a connection of its own, its body cut short at `sent`.
  def send_request(server, head, sent = "")
    client_socket(server.port).tap { |socket| socket.write("#{head}\r\nHost: kino\r\n\r\n#{sent}") }
  end

  # The response on `socket`, its body de-chunked: [status, body].
  def response(socket)
    status, _headers, body = read_response(socket)
    [status[%r{\AHTTP/1\.1 (\d+)}, 1], body]
  end

  each_mode do |mode|
    it "answers what the app makes of a Timeout.timeout around a body read, and keeps serving" do
      with_client_server(mode) do |server|
        client = send_request(server, "POST /read HTTP/1.1\r\nContent-Length: 10", "12345")
        expect(reports.next(awaiting: "report of the timeout")).to eq(:timed_out)

        expect(response(client)).to eq(["408", "timed out"])
        expect(Array.new(2) { get(server.port, "/") }).to all(eq("ok"))
      ensure
        client&.close
      end
    end

    it "leaves a request whose body read Timeout.timeout cut to the app, which can take its connection over" do
      with_client_server(mode) do |server|
        head = "POST /read?hijack HTTP/1.1\r\nUpgrade: chat\r\nConnection: Upgrade\r\nContent-Length: 10"
        client = send_request(server, head, "12345")
        expect(reports.next(awaiting: "report of the timeout")).to eq(:timed_out)

        expect(response(client)).to eq(["408", "timed out"])
        expect(reports.next(awaiting: "report of the hijack")).to eq(:hijacked)
        expect(get(server.port, "/")).to eq("ok")
      ensure
        client&.close
      end
    end

    it "does not fail a body read that Thread#wakeup interrupts" do
      with_client_server(mode) do |server|
        client = send_request(server, "POST /read?wake HTTP/1.1\r\nContent-Length: 10", "12345")
        expect(reports.next(awaiting: "report of the wakes")).to eq(:woke)
        client.write("67890")

        expect(response(client)).to eq(["200", "1234567890"])
      ensure
        client&.close
      end
    end

    it "ends the response cleanly when Timeout.timeout cuts a streamed write the client is slow to take" do
      with_client_server(mode) do |server|
        client = send_request(server, "GET /write HTTP/1.1")
        expect(reports.next(awaiting: "report of the timeout")).to eq(:timed_out)

        status, body = response(client)
        expect(status).to eq("200")
        expect(body.bytesize % 262_144).to eq(0)
        expect(get(server.port, "/")).to eq("ok")
      ensure
        client&.close
      end
    end

    it "does not fail a streamed write that Thread#wakeup interrupts" do
      with_client_server(mode) do |server|
        client = send_request(server, "GET /write?wake HTTP/1.1")
        expect(reports.next(awaiting: "report of the wakes")).to eq(:woke)

        status, body = response(client)
        expect(status).to eq("200")
        expect(body.bytesize).to eq(64 * 262_144)
      ensure
        client&.close
      end
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

# Thread#kill ends a worker's thread with no exception for any rescue to
# see. Done by the app (in :ractor mode from a thread inside the worker's
# Ractor), it crashes the worker all the same: what the worker held is
# answered at once, and a new worker takes its place.
RSpec.describe "a worker thread the app kills" do
  def killed = "the worker's thread was killed"

  # GET /kill leaves a thread behind that kills the worker thread serving
  # it once that thread waits: for its next request, or under a
  # worker_loop, with each of its jobs waiting. With ?held, the request
  # itself waits 5 s, and is killed in that wait, never answered by the
  # app. /kill-main kills its worker Ractor's main thread instead, and
  # waits 5 s. /sleep waits 2 s (a fiber's wait under a worker_loop);
  # anything else is "ok".
  def killing_app
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/kill"
        worker = Thread.current
        Thread.new do
          sleep 0.002 while worker.alive? && worker.status != "sleep"
          sleep 0.005
          worker.kill
        end
        sleep 5 if env["QUERY_STRING"] == "held"
        [200, {}, ["ok"]]
      when "/kill-main"
        Thread.main.kill
        sleep 5
        [200, {}, ["not killed"]]
      when "/sleep"
        sleep 2
        [200, {}, ["slept"]]
      else
        [200, {}, ["ok"]]
      end
    end
  end

  # killing_app on `workers` workers of `threads` threads; yields the
  # port, the server and its worker exits (on_worker_exit's errors), and
  # returns what the server logged.
  def with_killing_server(mode, workers: 1, threads: 1, **opts)
    exits = Thread::Queue.new
    capture_native_stderr do
      with_server(killing_app, mode: mode, workers: workers, threads: threads, **opts,
        on_worker_exit: ->(_worker, error) { exits << error }) do |_, port, server|
        await_inline(server, port, workers * threads) if opts[:worker_loop]
        yield port, server, exits
      end
    end
  end

  # GET /die waits at `gate`, then kills its worker's thread (`how`
  # :kill) or raises an exception that is no StandardError; /sleep waits
  # 3 s; anything else is "ok".
  def dying_app(gate, how)
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/die"
        gate.wait(10)
        Thread.current.kill if how == :kill
        raise Exception, "died at its gate" # rubocop:disable Lint/RaiseException
      when "/sleep"
        sleep 3
        [200, {}, ["slept"]]
      else
        [200, {}, ["ok"]]
      end
    end
  end

  # A worker Ractor of 2 threads under a worker_loop, whose main thread a
  # request kills while another is in flight, then served for a second:
  # what came of it, as a JSON line.
  def main_kill_scenario
    <<~RUBY
      #{scenario_prelude}
      app = Ractor.shareable_proc do |env|
        case env["PATH_INFO"]
        when "/kill-main"
          Thread.main.kill
          sleep 5
          [200, {}, ["not killed"]]
        when "/sleep"
          sleep 2
          [200, {}, ["slept"]]
        else
          [200, {}, ["ok"]]
        end
      end
      worker_loop = Ractor.shareable_proc do |worker|
        Sync { |task| worker.serve(concurrency: 8) { |job| task.async { job.call } } }
      end
      opts = {mode: :ractor, workers: 1, threads: 2, worker_loop: worker_loop}
      exits = Thread::Queue.new
      server = Kino::Server.new(app, **opts, on_worker_exit: ->(_worker, error) { exits << error }).start
      get = lambda do |path, within|
        response = Net::HTTP.start("127.0.0.1", server.port, read_timeout: within, max_retries: 0) { |http| http.get(path) }
        [response.code, response.body]
      rescue Net::ReadTimeout
        ["no answer within \#{within} s"]
      end
      until server.stats[:worker_status].size == 2 && get.call("/", 1) == %w[200 ok]
        sleep 0.01
      end
      slots = server.stats[:worker_status].map { it[:index] }
      sleeping = Thread.new { get.call("/sleep", 5) }
      sleep 0.01 until server.stats[:in_flight] == 1
      started = Kino.monotonic
      killed = get.call("/kill-main", 2)
      took = Kino.monotonic - started
      error = exits.pop(timeout: 5)
      answers = Array.new(10) { sleep 0.1; get.call("/", 1) }
      rows = server.stats[:worker_status]
      result = {
        killed: killed, took: took, slept: sleeping.value, error: [error.class.name, error&.message],
        answers: answers, later: Array.new(exits.size) { exits.pop.inspect }, respawns: server.stats[:respawns],
        retired: rows.map { it[:retired] }, taken: rows.map { it[:index] }, slots: slots
      }
      server.shutdown
      puts JSON.generate(result)
    RUBY
  end

  each_mode do |mode|
    # Killed idle (waiting for its next request), then killed in a
    # request it holds, never answered by the app.
    it "replaces a worker killed idle or in a request, as a crash, answers what it held at once, and serves on" do
      log = with_killing_server(mode) do |port, server, exits|
        expect(get_response(port, "/kill", within: 1)).to have_attributes(code: "200", body: "ok")
        error = exits.pop(timeout: 5)
        20.times do |n|
          answer = get_response(port, "/", within: 1)
          expect([answer.code, answer.body]).to eq(%w[200 ok]), "request #{n + 1} after the kill got #{answer.inspect}"
        end
        expect([error.class, error&.message]).to eq([Kino::Error, killed]), "on_worker_exit got #{error.inspect}"

        answer = nil
        took = timed { answer = get_response(port, "/kill?held", within: 2) }
        expect(answer.code).to eq("500")
        expect(took).to be < 0.5, "the 500 came #{took.round(2)} s after the request"
        expect(Array.new(3) { get(port, "/", within: 1) }).to all(eq("ok"))
        expect(server.stats).to include(respawns: 2)
        took = timed { server.shutdown }
        expect(took).to be < 1, "the shutdown took #{took.round(2)} s"
      end

      expect(log.scan("crashed; respawning at once · Kino::Error: #{killed}").size).to eq(2), log
    end

    it "crashes a worker_loop worker killed: what it held is answered at once, its idle connection goes to hyper, " \
      "and a new worker serves" do
      with_killing_server(mode, worker_loop: async_loop(8)) do |port, server, _exits|
        idle = client_socket(port)
        idle.write("GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        expect(read_response(idle).last).to eq("ok")
        inline = client_socket(port)
        inline.write("GET /sleep HTTP/1.1\r\nhost: x\r\n\r\n")
        # Not served inline: through hyper and the worker's lane.
        queued = client_socket(port)
        queued.write("GET /sleep HTTP/1.0\r\n\r\n")
        wait_for("both requests in flight") { server.stats[:in_flight] == 2 }

        # Its connection ends with its answer, before the kill: the idle
        # one alone is left on the worker for hyper.
        expect(raw_exchange(port, "GET /kill HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n"))
          .to start_with("HTTP/1.1 200 OK\r\n")
        statuses = nil
        took = timed { statuses = [inline, queued].map { |socket| read_response(socket, within: 1).first } }
        expect(statuses).to all(match(%r{\AHTTP/1\.[01] 500 }))
        expect(took).to be < 0.5, "the 500s came #{took.round(2)} s after the kill"
        wait_for("no inline connection counted on the killed worker", timeout: 1) do
          server.stats[:inline][:connections].zero?
        end
        expect(server.stats[:worker_status].sum { it[:in_flight] }).to eq(0)

        # Its replacement serves from here on.
        wait_for("the killed worker replaced", timeout: 1) { server.stats[:respawns] == 1 }
        expect(get_response(port, "/", within: 1)).to have_attributes(code: "200", body: "ok")
        # Its client kept the idle connection, hyper's from then on.
        idle.write("GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        expect(read_response(idle, within: 1).first).to eq("HTTP/1.1 200 OK")
        # The HTTP/1.0 request's, and the idle connection's at the kill.
        expect(server.stats[:inline][:handbacks]).to eq(2)
        expect(server.stats).to include(respawns: 1)
        took = timed { server.shutdown }
        expect(took).to be < 1, "the shutdown took #{took.round(2)} s"
      ensure
        [idle, inline, queued].compact.each(&:close)
      end
    end

    it "counts no crash for the worker threads a process exit ends without a shutdown" do
      status, _, output = run_bounded(<<~RUBY)
        #{scenario_prelude}
        app = Ractor.shareable_proc { |_env| [200, {}, ["ok"]] }
        server = Kino::Server.new(app, mode: :#{mode}, workers: 2, threads: 2,
          on_worker_exit: ->(worker, error) { $stderr.puts "on_worker_exit(\#{worker}, \#{error.inspect})" }).start
        $stderr.puts "served: \#{Net::HTTP.get(URI("http://127.0.0.1:\#{server.port}/"))}"
      RUBY

      expect(status).to be_success, output
      expect(output).to include("served: ok")
      expect(output).not_to include("crashed")
      expect(output).not_to include("replacement thread")
      expect(output.scan(/on_worker_exit\(\d+, (.*)\)/).flatten.uniq - ["nil"]).to be_empty, output
    end

    it "turns away with a 503, counted as rejected, a dead worker's queued request that finds no lane room " \
      "within queue_timeout, as a new arrival is" do
      FileGate.closed do |gate|
        log = capture_native_stderr do
          with_server(dying_app(gate, :raise), mode: mode, workers: 2, threads: 1, lanes: true,
            queue_timeout: 0.3) do |_, port, server|
            # No replacement makes room: the pool's recovery of the crash
            # waits until the queued requests are answered.
            pool = pool_of(server)
            resume = Thread::Queue.new
            allow(pool).to receive(:forget_slots).and_wrap_original do |forget, *args|
              resume.pop(timeout: 5)
              forget.call(*args)
            end
            # A worker's own count: the server counts a request in flight
            # before a worker has taken it, and a worker yet to take one
            # takes the next ones too.
            holding = -> { server.stats[:worker_status].map { |worker| worker[:in_flight] } }
            busy = Thread.new { get_response(port, "/sleep") }
            wait_for("one worker busy") { holding.call.sum == 1 }
            # Its client outwaits every wait below, so one that times out
            # fails as itself.
            dying = Thread.new { get_response(port, "/die", within: 12) }
            wait_for("the other worker at its gate") { holding.call == [1, 1] }
            # Three behind each: the busy worker's lane has room for one
            # of the dying worker's, and the other two wait for more.
            queued = Array.new(6) do |n|
              Thread.new { [get_response(port, "/#{n}").code, monotonic] }
            end
            depths = -> { server.stats[:lane_depths] }
            wait_for("three queued behind each worker", detail: -> { depths.call.inspect }) { depths.call == [3, 3] }

            opened = monotonic
            gate.open
            expect(dying.value.code).to eq("500")
            turned_away = queued.map(&:value).select { |answer| answer.first == "503" }
            resume << :go
            expect(turned_away.size).to eq(2), "#{turned_away.size} of the queued requests got a 503"
            late = turned_away.map { |_, at| (at - opened).round(2) }.max
            expect(late).to be_between(0.3, 1.5), "the 503s came #{late} s after the worker died"
            expect(server.stats[:rejected]).to eq(2)
            expect(busy.value).to have_attributes(code: "200", body: "slept")
          ensure
            gate.open
            resume&.close
            [busy, dying, *queued].compact.each(&:join)
          end
        end
        expect(log).to include("crashed")
      end
    end

    it "answers within 0.5 s a request sent after an idle worker was killed, its sibling busy" do
      with_killing_server(mode, workers: 2, lanes: true) do |port, server, exits|
        busy = Thread.new { get_response(port, "/sleep") }
        # A worker's own count: the server counts a request in flight
        # before a worker has taken it, and the worker killed next could
        # take it then.
        wait_for("the sibling busy") do
          server.stats[:worker_status].any? { |worker| worker[:in_flight].positive? }
        end
        expect(get_response(port, "/kill", within: 1)).to have_attributes(code: "200", body: "ok")
        exits.pop(timeout: 5)

        answer = nil
        took = timed { answer = get_response(port, "/", within: 2) }
        expect(answer).to have_attributes(code: "200", body: "ok")
        expect(took).to be < 0.5, "the answer came #{took.round(2)} s after the request"
        expect(busy.value).to have_attributes(code: "200", body: "slept")
      ensure
        busy&.join
      end
    end

    if mode == :ractor
      # In a Ruby of its own, bounded: a VM that faults or wedges as a
      # Ractor ends fails here, by name.
      # One thread in Kino's loop is "500s what a dying worker ractor
      # held" above.
      it "replaces a worker Ractor whose main thread was killed, as one crash, for good " \
        "(a worker_loop of 2 threads, a request in flight beside it)" do
        status, out, log = run_bounded(main_kill_scenario)
        expect(status).to be_success, log
        result = JSON.parse(out.lines.last)

        expect(result["killed"].first).to eq("500")
        expect(result["took"]).to be < 0.5, "the 500 came #{result["took"].round(2)} s after the request"
        expect(result["slept"].first).to eq("500")
        # The old Ractor's threads end before its replacement takes
        # their slots: for a second on, each request is answered.
        expect(result["answers"]).to all(eq(%w[200 ok]))
        expect(result["error"]).to eq(["Kino::Error", "the worker ractor's main thread was killed"])
        expect(result["later"]).to be_empty, "on_worker_exit fired again: #{result["later"]}"
        expect(result["respawns"]).to eq(1)
        expect(result["retired"]).to all(be(false))
        expect(result["taken"]).to eq(result["slots"]),
          "the replacement took slots #{result["taken"]}, not the killed Ractor's #{result["slots"]}"
        expect(log.scan(/worker-\d crashed/).size).to eq(1), log
      end
    else
      it "counts no crash for a worker thread the shutdown kills past its deadline" do
        log = with_killing_server(mode) do |port, server, exits|
          stuck = Thread.new { get_response(port, "/sleep") }
          wait_for("the request in flight") { server.stats[:in_flight] == 1 }

          server.shutdown(timeout: 0.3)
          expect(stuck.value.code).to eq("500")
          wait_for("the killed thread's exit") { !exits.empty? }
          expect(exits.pop).to be_nil, "a straggler the shutdown killed went to on_worker_exit as a crash"
        end

        expect(log).not_to include("crashed")
      end
    end
  end
end

# A body that tells what the server does with it, complete (ProbeBody::Complete) or streamed
# (ProbeBody::Streamed). Its close fails with `on_close` :fails, as a middleware's after-response
# hook that raises would; else it reports :closed on `port`, after a :closing and a wait at
# `on_close` when that is a gate: an app's after-response work that hangs until the gate opens.
ProbeBody = Data.define(:chunks, :port, :on_close) do
  def initialize(chunks:, port: nil, on_close: nil) = super

  def close
    raise "the body's close failed" if on_close == :fails

    if on_close
      port << :closing
      on_close.wait(10)
    end
    port << :closed
  end
end

# Its chunks cannot be read when they are :unreadable (to_ary raises), or hold what is no String.
class ProbeBody::Complete < ProbeBody
  def to_ary = chunks.equal?(:unreadable) ? raise("the body's to_ary failed") : chunks
end

class ProbeBody::Streamed < ProbeBody
  def each(&) = chunks.each(&)
end

RSpec.describe "a request that rode the fused answer of one whose body fails to close" do
  # GET /first waits at `gate`, then answers with a body whose close
  # fails; anything else is "second".
  def failing_close_app(gate)
    body = Ractor.make_shareable(ProbeBody::Complete.new(chunks: ["first"], on_close: :fails))
    Ractor.shareable_proc do |env|
      gate.wait(10) if env["PATH_INFO"] == "/first"
      [200, {}, (env["PATH_INFO"] == "/first") ? body : ["second"]]
    end
  end

  each_mode do |mode|
    it "is served, and the close failure is the first request's, reported once" do
      errors = reports.port
      on_error = Ractor.shareable_proc { |e, env| errors << [e.message, env["PATH_INFO"]] }
      FileGate.closed do |gate|
        log = capture_native_stderr do
          with_server(failing_close_app(gate), mode: mode, workers: 1, threads: 1, on_error: on_error) do |_, port, server|
            first = Thread.new { get_response(port, "/first") }
            wait_for("the first request in flight") { server.stats[:in_flight] == 1 }
            second = client_socket(port)
            second.write("GET /second HTTP/1.1\r\nHost: kino\r\n\r\n")
            wait_for("the second request queued behind it") { server.stats[:queued] == 1 }

            gate.open
            expect(first.value.body).to eq("first")
            answer = second.wait_readable(2) ? read_response(second) : ["no answer within 2 s"]
            expect(answer.first).to eq("HTTP/1.1 200 OK")
            expect(answer.last).to eq("second")
            expect(reports.next(awaiting: "on_error")).to eq(["the body's close failed", "/first"])
            expect(server.stats[:respawns]).to eq(0), "nothing crashed"
          ensure
            gate.open
            first&.join
            second&.close
          end
        end
        expect(log.scan("the body's close failed").size).to eq(1), log
        expect(reports).to be_empty, "on_error fired more than once"
      end
    end
  end
end

RSpec.describe "a body whose answer raises before it is out" do
  # Each way an answer goes out: the fused respond-and-take, a complete
  # answer sent apart from the take (an after_request_complete hook is
  # set), and a streamed one; with the body each takes, and whether the
  # server has the hook.
  ways = {
    "fused answer" => [ProbeBody::Complete, false],
    "complete answer sent before its hook" => [ProbeBody::Complete, true],
    "streamed answer" => [ProbeBody::Streamed, false]
  }

  each_mode do |mode|
    ways.each do |way, (kind, hooked)|
      # /bad's head cannot be built, so sending it raises, as the fused
      # take does when a Thread#raise comes back with it; /good's goes
      # out. The closes are counted once the server is shut down, when
      # every way has closed every body it took.
      it "closes the body of a #{way} once, whether its head can be built or not", :quiet do
        body = Ractor.make_shareable(kind.new(chunks: ["body"], port: reports.port))
        app = Ractor.shareable_proc do |env|
          (env["PATH_INFO"] == "/bad") ? [200, {"x-bad" => "a\nb"}, body] : [200, {}, body]
        end
        opts = hooked ? {after_request_complete: Ractor.shareable_proc { |_env, _status| }} : {}
        with_server(app, mode: mode, workers: 1, threads: 1, **opts) do |_, port, _server|
          expect(get_response(port, "/bad").code).to eq("500")
          expect(reports.next(timeout: 2, awaiting: "close of the body it could not send")).to eq(:closed)
          expect(Array.new(2) { get(port, "/good") }).to all(eq("body"))
        end

        expect(Array.new(2) { reports.next(timeout: 2, awaiting: "close of a body it sent") }).to all(eq(:closed))
        expect(reports).to be_empty, "a body closed more than once"
      end
    end
  end
end

RSpec.describe "a body whose close fails after its answer did" do
  ways = {
    "complete answer sent before its hook" => [ProbeBody::Complete, true],
    "streamed answer" => [ProbeBody::Streamed, false]
  }

  each_mode do |mode|
    ways.each do |way, (kind, hooked)|
      # The answer's head cannot be built, so sending it raises; then
      # the body's close raises too.
      it "reports both failures of a #{way}, the answer's not lost behind the close's", :quiet do
        errors = reports.port
        on_error = Ractor.shareable_proc { |e, _env| errors << e.message }
        body = Ractor.make_shareable(kind.new(chunks: ["body"], on_close: :fails))
        app = Ractor.shareable_proc { |_env| [200, {"x-bad" => "a\nb"}, body] }
        opts = hooked ? {after_request_complete: Ractor.shareable_proc { |_env, _status| }} : {}
        with_server(app, mode: mode, workers: 1, threads: 1, on_error: on_error, **opts) do |_, port, _server|
          expect(get_response(port, "/").code).to eq("500")
          reported = Array.new(2) { reports.next(timeout: 2, awaiting: "both failures reported") }
          expect(reported).to include("the body's close failed")
          expect(reported.uniq.size).to eq(2), "on_error got #{reported}"
        end
        expect(reports).to be_empty, "more failures reported than happened"
      end
    end
  end
end

# Its chunks are joined by the same Ruby in either mode: ractor mode pins it.
RSpec.describe "a complete body whose chunks cannot be read" do
  [[false, :to_ary], [true, :chunk]].each do |hooked, fails|
    it "is closed once, and its client gets a 500 (#{hooked ? "sent before its hook" : "fused"}, " \
      "#{(fails == :to_ary) ? "to_ary raises" : "a chunk is no String"})", :quiet do
      chunks = (fails == :to_ary) ? :unreadable : ["a", 1]
      body = Ractor.make_shareable(ProbeBody::Complete.new(chunks: chunks, port: reports.port))
      app = Ractor.shareable_proc { |env| (env["PATH_INFO"] == "/bad") ? [200, {}, body] : [200, {}, ["ok"]] }
      opts = hooked ? {after_request_complete: Ractor.shareable_proc { |_env, _status| }} : {}
      with_server(app, mode: :ractor, workers: 1, threads: 1, **opts) do |_, port, _server|
        expect(get_response(port, "/bad").code).to eq("500")
        expect(reports.next(timeout: 2, awaiting: "close of the body it could not read")).to eq(:closed)
        expect(get(port, "/")).to eq("ok")
      end
      expect(reports).to be_empty, "the body closed more than once"
    end
  end
end

RSpec.describe "a fused answer's body" do
  each_mode do |mode|
    it "closes before its next request, on a worker that has nothing to take next", :quiet do
      body = Ractor.make_shareable(ProbeBody::Complete.new(chunks: ["answered"], port: reports.port))
      app = Ractor.shareable_proc { |_env| [200, {}, body] }
      with_server(app, mode: mode, workers: 1, threads: 1) do |_, port, _server|
        2.times do
          expect(get(port, "/")).to eq("answered")
          # With no next request, a body that closes only at the next take never does.
          expect(reports.next(timeout: 2, awaiting: "close of the body with no request behind it")).to eq(:closed)
        end
      end
      expect(reports).to be_empty, "a body closed more than once"
    end

    # /stuck's body reports when its close begins, then waits at `gate`;
    # anything else answers "sibling".
    it "keeps its worker busy while it closes, so a close that hangs reads as a wedge and requests go to " \
      "a sibling", :quiet do
      FileGate.closed do |gate|
        stuck = Ractor.make_shareable(ProbeBody::Complete.new(chunks: ["answered"], port: reports.port, on_close: gate))
        app = Ractor.shareable_proc { |env| (env["PATH_INFO"] == "/stuck") ? [200, {}, stuck] : [200, {}, ["sibling"]] }
        with_server(app, mode: mode, workers: 2, threads: 1, lanes: true) do |_, port, server|
          expect(get(port, "/stuck", within: 1)).to eq("answered")
          expect(reports.next(awaiting: "the stuck close to begin")).to eq(:closing)

          wait_for("the worker in the close to read as busy", timeout: 2) do
            server.stats[:worker_status].any? { |row| row[:in_flight] == 1 && row[:busy_ms].positive? }
          end
          expect(get(port, "/", within: 1)).to eq("sibling")
        ensure
          gate.open
        end
        expect(reports.next(awaiting: "the close to end")).to eq(:closed)
      end
    end
  end
end
