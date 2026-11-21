# frozen_string_literal: true

require "rack"

# Experimental lane dispatch: per-worker queues, awake-preferring
# assignment, work stealing. Must behave identically to the shared queue
# for correctness; only the scheduling differs.
RSpec.describe "lane dispatch" do
  def shareable_app
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/slow" then sleep 0.2
                        [200, {"content-type" => "text/plain"}, ["slow done"]]
      when "/second" then sleep 1
                          [200, {"content-type" => "text/plain"}, ["second done"]]
      else [200, {"content-type" => "text/plain"}, ["lane ok"]]
      end
    end
  end

  it "serves requests under rack-lint with lanes enabled (threaded)" do
    app = Rack::Lint.new(->(_env) { [200, {"content-type" => "text/plain"}, ["lint ok"]] })

    with_server(app, lanes: true, workers: 2, threads: 2) do |host, port|
      10.times do
        expect(Net::HTTP.get_response(host, "/x", port).body).to eq("lint ok")
      end
    end
  end

  each_mode do |mode|
    it "answers fast requests at once while another worker is deep in a slow one" do
      fast_beside_slow(mode)
    end
  end

  def fast_beside_slow(mode)
    with_server(shareable_app, mode: mode, lanes: true, workers: 4, threads: 1) do |host, port, server|
      Net::HTTP.get_response(host, "/", port)
      slow = Thread.new { Net::HTTP.get_response(host, "/second", port) }
      wait_for("the slow request to be 20 ms in") do
        server.stats[:worker_status].any? { |worker| worker[:in_flight] > 0 && worker[:busy_ms] >= 20 }
      end

      fast_times = 10.times.map do
        timed { Net::HTTP.get_response(host, "/", port) }
      end

      expect(slow.value.body).to eq("second done")
      # About 1 ms each when they go to an idle worker; one that queues
      # behind the slow request waits for a sibling's 50 ms park tick, so
      # 40 ms tells the two apart with room for a busy host.
      expect(fast_times.max).to be < 0.04,
        "fast requests waited behind the slow one: #{fast_times.map { |t| (t * 1000).round(1) }} ms"
    end
  end

  it "drains cleanly on shutdown" do
    server = Kino::Server.new(shareable_app, mode: :ractor, lanes: true,
      workers: 2, threads: 1).start
    port = server.port
    client = Thread.new { Net::HTTP.get_response("127.0.0.1", "/slow", port) }
    sleep 0.05

    server.shutdown(timeout: 5)

    expect(client.value.code).to eq("200")
  end
end
