# frozen_string_literal: true

# Phase 4: graceful shutdown and backpressure. Ordering is asserted through
# observable effects (response codes, return timing), never bare sleeps.
RSpec.describe "graceful shutdown" do
  def sleeper_app(duration)
    lambda do |_env|
      sleep duration
      [200, {"content-type" => "text/plain"}, ["finished"]]
    end
  end

  it "completes in-flight requests while draining" do
    server = Kino::Server.new(sleeper_app(0.4), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1 # request is now in flight

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.shutdown(timeout: 5)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    response = client.value
    expect(response.code).to eq("200")
    expect(response.body).to eq("finished")
    expect(elapsed).to be >= 0.2, "shutdown should have waited for the in-flight request"
  end

  it "refuses new connections while draining" do
    server = Kino::Server.new(sleeper_app(0.6), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1
    shutdown_thread = Thread.new { server.shutdown(timeout: 5) }
    sleep 0.15 # accept loop is now stopped, drain in progress

    expect do
      Net::HTTP.start("127.0.0.1", port, open_timeout: 0.5) { |http| http.get("/") }
    end.to raise_error(SystemCallError)

    expect(client.value.code).to eq("200")
    shutdown_thread.join
  end

  it "enforces the deadline on stuck handlers and frees their clients" do
    server = Kino::Server.new(sleeper_app(10), workers: 1, threads: 1, mode: :threaded).start
    port = server.port

    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", port) }
    sleep 0.1

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.shutdown(timeout: 0.5)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    expect(elapsed).to be < 2.5, "shutdown must return by deadline + epsilon, took #{elapsed.round(2)}s"
    expect(client.value.code).to eq("500"), "the stuck request's client must be freed with a 500"
  end

  it "is idempotent" do
    server = Kino::Server.new(->(_env) { [200, {}, []] }, workers: 1, threads: 1, mode: :threaded).start
    server.shutdown
    expect { server.shutdown }.not_to raise_error
  end

  describe "backpressure" do
    it "returns 503 on queue overflow without hanging anyone" do
      app = sleeper_app(0.3)
      opts = {workers: 1, threads: 1, mode: :threaded, queue_depth: 1, queue_timeout: 0.1}

      with_server(app, **opts) do |host, port|
        responses = Array.new(6) do
          Thread.new { Net::HTTP.get_response(host, "/", port).code }
        end.map(&:value)

        expect(responses.size).to eq(6)
        expect(responses).to include("200"), "the in-flight request should finish"
        expect(responses).to include("503"), "overflow should be rejected, not queued forever"
        expect(responses.tally.keys.sort).to eq(%w[200 503])
      end
    end
  end
end
