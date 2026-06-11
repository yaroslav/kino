# frozen_string_literal: true

require "rack"

# Experimental lane dispatch: per-worker queues, awake-preferring
# assignment, work stealing. Must behave identically to the shared queue
# for correctness; only the scheduling differs.
RSpec.describe "lane dispatch" do
  def shareable_app
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/boom" then raise Exception, "hard crash" # rubocop:disable Lint/RaiseException
      when "/slow" then sleep 0.2
                        [200, {"content-type" => "text/plain"}, ["slow done"]]
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

  it "serves in ractor mode with lanes and recovers from crashes" do
    with_server(shareable_app, mode: :ractor, lanes: true, workers: 2, threads: 1) do |host, port, server|
      expect(Net::HTTP.get_response(host, "/", port).body).to eq("lane ok")
      expect(Net::HTTP.get_response(host, "/boom", port).code).to eq("500")

      recovered = nil
      20.times do
        recovered = begin
          Net::HTTP.get_response(host, "/", port)
        rescue
          nil
        end
        break if recovered&.code == "200"
        sleep 0.1
      end
      expect(recovered&.code).to eq("200")
      expect(server.stats[:respawns]).to eq(1)
    end
  end

  it "steals work so a slow lane neighbor doesn't block fast requests" do
    with_server(shareable_app, mode: :ractor, lanes: true, workers: 2, threads: 1) do |host, port|
      slow = Thread.new { Net::HTTP.get_response(host, "/slow", port) }
      sleep 0.05 # slow request is now occupying one of the two lanes

      fast_times = 5.times.map do
        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Net::HTTP.get_response(host, "/", port)
        Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
      end

      expect(slow.value.body).to eq("slow done")
      expect(fast_times.max).to be < 0.15, "fast requests stuck behind a slow lane"
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
