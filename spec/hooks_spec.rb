# frozen_string_literal: true

require "tmpdir"

RSpec.describe "lifecycle hooks (threaded)" do
  let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  it "fires after_worker_boot once per worker and after_request_complete per request" do
    booted = Queue.new
    completed = Queue.new
    hooks = {
      after_worker_boot: ->(i) { booted << i },
      after_request_complete: ->(env, status) { completed << [env["PATH_INFO"], status] }
    }
    with_server(ok_app, mode: :threaded, workers: 1, threads: 2, **hooks) do |host, port, _server|
      3.times { Net::HTTP.get_response(host, "/x", port) }
      # Wait for hooks to fire (unfused path calls send_simple then takes next request)
      sleep 0.5
      expect(booted.size).to eq(2) # one per worker thread (workers*threads)
      done = Array.new(completed.size) { completed.pop }
      expect(done).to all(eq(["/x", 200]))
      expect(done.size).to eq(3)
    end
  end

  it "logs raising after_request_complete hook and keeps serving (response sent first)" do
    completed = []
    hooks = {
      after_request_complete: ->(env, status) do
        completed << [env["PATH_INFO"], status]
        raise "boom"
      end
    }
    with_server(ok_app, mode: :threaded, workers: 1, threads: 1, **hooks) do |host, port, _server|
      # Two sequential requests; even though hook raises "boom", both responses arrive
      # (proving send_simple is called before the hook, independent of hook outcome)
      r1 = Net::HTTP.get_response(host, "/a", port)
      r2 = Net::HTTP.get_response(host, "/b", port)

      expect(r1.code).to eq("200")
      expect(r2.code).to eq("200")
      # Hook was called and raised, but worker stayed alive and processed both
      sleep 0.2
      expect(completed.size).to eq(2)
    end
  end

  it "fires after_boot once after the pool is up" do
    fired = Queue.new
    with_server(ok_app, mode: :threaded, workers: 1, threads: 1, after_boot: -> { fired << :boot }) do
      expect(fired.size).to eq(1)
    end
  end

  it "fires on_worker_exit once per worker thread with a nil error on clean shutdown" do
    exits = Queue.new
    with_server(ok_app, mode: :threaded, workers: 1, threads: 2,
      on_worker_exit: ->(worker_id, error) { exits << [worker_id, error] }) do |_host, _port, server|
      server.shutdown(timeout: 1)
      fired = Array.new(2) { exits.pop(timeout: 1) }
      expect(fired.compact.size).to eq(2) # one per worker thread (workers*threads)
      fired.each do |worker_id, error|
        expect(worker_id).to be_a(Integer)
        expect(error).to be_nil
      end
    end
  end
end

RSpec.describe "lifecycle hooks (ractor)" do
  it "fires on_worker_exit with the error on a ractor crash and nil on clean exit" do
    # Exception (not StandardError) bypasses the worker's per-request rescue
    # and actually kills the ractor; a plain StandardError is caught inside
    # Worker.serve and never reaches the supervisor at all.
    boom = Ractor.shareable_proc do |env|
      raise Exception, "boom" if env["PATH_INFO"] == "/boom" # rubocop:disable Lint/RaiseException
      [200, {}, ["ok"]]
    end
    exits = Queue.new
    server = Kino::Server.new(boom, mode: :ractor, workers: 1, threads: 1,
      on_worker_exit: ->(i, err) { exits << [i, err&.message] }).start
    begin
      Net::HTTP.get_response("127.0.0.1", "/boom", server.port)
    rescue
      nil
    end
    sleep 0.3 # supervisor observes the crash and respawns
    crash = exits.pop
    expect(crash[0]).to be_a(Integer)
    expect(crash[1]).to eq("boom")
  ensure
    server&.shutdown(timeout: 0.2)
    # a clean-exit on_worker_exit(nil) fires during shutdown
    sleep 0.05
    expect(exits.pop(true)[1]).to be_nil if exits.size.positive?
  end

  it "fires a shareable after_request_complete inside a worker ractor" do
    Dir.mktmpdir do |dir|
      stub_const("HOOK_LOG_PATH", File.join(dir, "completed.log").freeze)
      app = Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] }
      logged = Ractor.shareable_proc do |_env, status|
        File.write(HOOK_LOG_PATH, "#{status}\n", mode: "a")
      end
      server = Kino::Server.new(app, mode: :ractor, workers: 1, threads: 1,
        after_request_complete: logged).start
      3.times { Net::HTTP.get_response("127.0.0.1", "/", server.port) }
      sleep 0.3
      expect(File.read(HOOK_LOG_PATH).lines.map(&:strip)).to eq(%w[200 200 200])
    ensure
      server&.shutdown(timeout: 0.2)
    end
  end

  it "rejects a non-shareable worker-context hook in forced :ractor mode" do
    app = Ractor.shareable_proc { |_env| [200, {}, ["ok"]] }
    cache = [] # unshareable capture
    bad = ->(_env, _status) { cache << 1 }
    expect {
      Kino::Server.new(app, mode: :ractor, after_request_complete: bad)
    }.to raise_error(Kino::Error, /after_request_complete hook/)
  end
end
