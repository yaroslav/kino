# frozen_string_literal: true

require "json"
require "tmpdir"

RSpec.describe "elastic pool" do
  let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def wait_for(timeout = 5)
    deadline = monotonic + timeout
    sleep 0.02 until yield || monotonic > deadline
  end

  # Slow enough that a burst keeps the queue non-empty across several
  # scaler ticks (100 ms), fast enough that specs stay short.
  def slow_shareable_app
    Ractor.shareable_proc do |env|
      Kino.sleep(0.4) if env["PATH_INFO"] == "/slow"
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end
  end

  def burst(host, port, count, path = "/slow")
    Array.new(count) { Thread.new { Net::HTTP.get_response(host, path, port) } }
  end

  describe "configuration" do
    it "loads max_workers and scale_down_after from the DSL" do
      Dir.mktmpdir do |dir|
        path = File.join(dir, "kino.rb")
        File.write(path, "workers 2\nmax_workers 8\nscale_down_after 10\n")
        config = Kino::Configuration.new.load_file(path)

        expect(config[:max_workers]).to eq(8)
        expect(config[:scale_down_after]).to eq(10)
      end
    end

    it "defaults to a fixed pool" do
      config = Kino::Configuration.new

      expect(config[:max_workers]).to be_nil
      expect(config[:scale_down_after]).to be_nil
    end

    it "rejects a ceiling below the floor" do
      expect { Kino::Server.new(ok_app, mode: :threaded, workers: 4, max_workers: 2) }
        .to raise_error(ArgumentError, /max_workers \(2\) must be at least workers \(4\)/)
    end

    it "rejects a non-positive scale_down_after" do
      expect { Kino::Server.new(ok_app, mode: :threaded, workers: 1, max_workers: 2, scale_down_after: 0) }
        .to raise_error(ArgumentError, /scale_down_after/)
    end
  end

  describe "stats" do
    it "reports the pool bounds and live count before and after start" do
      server = Kino::Server.new(ok_app, mode: :threaded, workers: 1, threads: 1, max_workers: 3)
      expect(server.stats).to include(workers: 1, max_workers: 3, active_workers: 1,
        scale_ups: 0, scale_downs: 0)

      server.start
      expect(server.stats).to include(workers: 1, max_workers: 3, active_workers: 1,
        scale_ups: 0, scale_downs: 0)
      expect(server.stats[:worker_status]).to all(include(retired: false))
    ensure
      server&.shutdown(timeout: 0.2)
    end

    it "reports the floor as the ceiling for a fixed pool" do
      server = Kino::Server.new(ok_app, mode: :threaded, workers: 2, threads: 1)

      expect(server.stats).to include(max_workers: 2, active_workers: 2)
    end
  end

  describe "ractor mode" do
    it "grows under queue pressure, shrinks back to the floor when idle, and recycles slots" do
      server = Kino::Server.new(slow_shareable_app, mode: :ractor, workers: 1, threads: 1,
        max_workers: 3, scale_down_after: 0.3).start
      host, port = "127.0.0.1", server.port

      requests = burst(host, port, 4)
      wait_for { server.stats[:active_workers] == 3 }
      expect(server.stats).to include(active_workers: 3, scale_ups: 2)
      requests.each { |t| expect(t.value.code).to eq("200") }

      wait_for { server.stats[:active_workers] == 1 }
      expect(server.stats).to include(active_workers: 1, scale_downs: 2)
      slots = server.stats[:worker_status].length

      requests = burst(host, port, 4)
      wait_for { server.stats[:active_workers] == 3 }
      requests.each { |t| expect(t.value.code).to eq("200") }
      # Retired slots were handed to the new workers: the registry did not grow.
      expect(server.stats[:worker_status].length).to eq(slots)
    ensure
      server&.shutdown(timeout: 1)
    end

    it "fires on_worker_exit with a nil cause for each retired worker" do
      exits = Queue.new
      server = Kino::Server.new(slow_shareable_app, mode: :ractor, workers: 1, threads: 1,
        max_workers: 3, scale_down_after: 0.3,
        on_worker_exit: ->(index, cause) { exits << [index, cause] }).start
      host, port = "127.0.0.1", server.port

      burst(host, port, 4).each(&:join)
      wait_for { server.stats[:active_workers] == 1 }

      retired = Array.new(2) { exits.pop(timeout: 2) }
      expect(retired.map(&:last)).to eq([nil, nil])
      expect(retired.map(&:first)).to all(be_a(Integer))
    ensure
      server&.shutdown(timeout: 1)
    end
  end

  describe "threaded mode" do
    let(:slow_app) do
      lambda do |env|
        sleep 0.4 if env["PATH_INFO"] == "/slow"
        [200, {"content-type" => "text/plain"}, ["ok"]]
      end
    end

    it "grows under queue pressure, shrinks back to the floor when idle, and recycles slots" do
      server = Kino::Server.new(slow_app, mode: :threaded, workers: 1, threads: 1,
        max_workers: 3, scale_down_after: 0.3).start
      host, port = "127.0.0.1", server.port

      requests = burst(host, port, 4)
      wait_for { server.stats[:active_workers] == 3 }
      expect(server.stats).to include(active_workers: 3, scale_ups: 2)
      requests.each { |t| expect(t.value.code).to eq("200") }

      wait_for { server.stats[:active_workers] == 1 }
      expect(server.stats).to include(active_workers: 1, scale_downs: 2)
      slots = server.stats[:worker_status].length

      requests = burst(host, port, 4)
      wait_for { server.stats[:active_workers] == 3 }
      requests.each { |t| expect(t.value.code).to eq("200") }
      expect(server.stats[:worker_status].length).to eq(slots)
    ensure
      server&.shutdown(timeout: 1)
    end

    it "grows and retires whole workers when a worker is several threads" do
      server = Kino::Server.new(slow_app, mode: :threaded, workers: 1, threads: 2,
        max_workers: 2, scale_down_after: 0.3).start
      host, port = "127.0.0.1", server.port

      requests = burst(host, port, 6)
      wait_for { server.stats[:active_workers] == 2 }
      expect(server.stats[:worker_status].length).to eq(4) # two workers x two slots
      requests.each { |t| expect(t.value.code).to eq("200") }

      wait_for { server.stats[:active_workers] == 1 }
      expect(server.stats[:worker_status].count { |w| w[:retired] }).to eq(2)
    ensure
      server&.shutdown(timeout: 1)
    end

    it "fires after_worker_boot for grown workers and on_worker_exit with nil for retired ones" do
      boots = Queue.new
      exits = Queue.new
      server = Kino::Server.new(slow_app, mode: :threaded, workers: 1, threads: 1,
        max_workers: 3, scale_down_after: 0.3,
        after_worker_boot: ->(id) { boots << id },
        on_worker_exit: ->(id, cause) { exits << [id, cause] }).start
      host, port = "127.0.0.1", server.port

      burst(host, port, 4).each(&:join)
      wait_for { server.stats[:active_workers] == 1 }

      expect(boots.size).to eq(3)
      retired = Array.new(2) { exits.pop(timeout: 2) }
      expect(retired.map(&:last)).to eq([nil, nil])
    ensure
      server&.shutdown(timeout: 1)
    end

    it "stays within the floor and the ceiling" do
      server = Kino::Server.new(slow_app, mode: :threaded, workers: 2, threads: 1,
        max_workers: 3, scale_down_after: 0.2).start
      host, port = "127.0.0.1", server.port

      requests = burst(host, port, 8)
      peak = 0
      until requests.none?(&:alive?)
        peak = [peak, server.stats[:active_workers]].max
        sleep 0.02
      end
      expect(peak).to eq(3)

      wait_for { server.stats[:active_workers] == 2 }
      sleep 0.5 # well past scale_down_after: still at the floor
      expect(server.stats[:active_workers]).to eq(2)
    ensure
      server&.shutdown(timeout: 1)
    end
  end

  describe "lane dispatch" do
    it "loses no request while the pool grows and shrinks underneath the dispatcher" do
      server = Kino::Server.new(slow_shareable_app, mode: :ractor, lanes: true, workers: 1, threads: 1,
        max_workers: 4, scale_down_after: 0.2).start
      host, port = "127.0.0.1", server.port

      3.times do
        burst(host, port, 6).each { |t| expect(t.value.code).to eq("200") }
        # Past scale_down_after: workers retire while these are dispatched.
        sleep 0.35
        10.times { expect(Net::HTTP.get_response(host, "/", port).code).to eq("200") }
      end

      expect(server.stats[:scale_ups]).to be >= 3
      expect(server.stats[:scale_downs]).to be >= 1
      expect(server.stats[:rejected]).to eq(0)
    ensure
      server&.shutdown(timeout: 1)
    end
  end

  describe "control plane" do
    it "reports the pool in /stats and /metrics" do
      server = Kino::Server.new(ok_app, mode: :threaded, workers: 1, threads: 1, max_workers: 3,
        control_bind: "127.0.0.1:0").start

      stats = JSON.parse(Net::HTTP.get_response("127.0.0.1", "/stats", server.control_port).body)
      expect(stats).to include("workers" => 1, "max_workers" => 3, "active_workers" => 1,
        "scale_ups" => 0, "scale_downs" => 0)
      expect(stats["worker_status"]).to all(include("retired" => false))

      metrics = Net::HTTP.get_response("127.0.0.1", "/metrics", server.control_port).body
      expect(metrics).to include("kino_max_workers 3", "kino_active_workers 1",
        "kino_scale_ups_total 0", "kino_scale_downs_total 0")
    ensure
      server&.shutdown(timeout: 0.2)
    end
  end

  describe "the Ruby scheduler cap" do
    # Ruby runs non-main ractors' Ruby code on at most RUBY_MAX_CPU native
    # threads (default 8); a pool past that gains no CPU parallelism.
    def with_max_cpu(value)
      previous = ENV["RUBY_MAX_CPU"]
      ENV["RUBY_MAX_CPU"] = value
      yield
    ensure
      ENV["RUBY_MAX_CPU"] = previous
    end

    def start_and_stop(**opts)
      app = Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] }
      capture_native_stderr do
        server = Kino::Server.new(app, mode: :ractor, threads: 1, **opts).start
        server.shutdown(timeout: 0.2)
      end
    end

    it "warns at start when a ractor pool's ceiling exceeds the cap" do
      err = with_max_cpu("2") { start_and_stop(workers: 1, max_workers: 3) }

      expect(err).to include("RUBY_MAX_CPU")
      expect(err).to include("3")
    end

    it "stays quiet when the pool fits under the cap" do
      err = with_max_cpu("2") { start_and_stop(workers: 1, max_workers: 2) }

      expect(err).not_to include("RUBY_MAX_CPU")
    end
  end
end
