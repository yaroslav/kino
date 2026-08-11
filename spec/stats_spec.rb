# frozen_string_literal: true

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
    line = Kino::CLI.stats_line({mode: :ractor, served: 42})

    expect(line).to include("Kino stats:")
    expect(line).to include("mode=:ractor")
    expect(line).to include("served=42")
  end

  it "reads respawns from the native layer" do
    with_server(ok_app) do |_host, _port, server|
      id = server.instance_variable_get(:@id)
      Kino::Native.record_respawn(id)
      expect(server.stats[:respawns]).to eq(1)
    end
  end
end
