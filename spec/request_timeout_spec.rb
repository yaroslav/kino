# frozen_string_literal: true

RSpec.describe "request timeout" do
  let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  it "returns 504 when the app exceeds the deadline, without waiting for it" do
    slow = lambda do |_env|
      sleep 2
      [200, {"content-type" => "text/plain"}, ["late"]]
    end

    with_server(slow, request_timeout: 0.2) do |host, port, server|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      response = Net::HTTP.get_response(host, "/", port)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      expect(response.code).to eq("504")
      expect(response.body).to include("Gateway Timeout")
      expect(elapsed).to be < 1.5 # 504 arrived near the 0.2s deadline, not after the 2s handler
      expect(server.stats[:timeouts]).to eq(1)
    end
  end

  it "keeps serving after a timeout; the late response is dropped" do
    slow_once = lambda do |env|
      sleep 0.5 if env["PATH_INFO"] == "/slow"
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end

    with_server(slow_once, request_timeout: 0.1) do |host, port, server|
      expect(Net::HTTP.get_response(host, "/slow", port).code).to eq("504")
      sleep 0.6 # let the stuck handler finish and fire its no-op late send

      expect(Net::HTTP.get_response(host, "/", port).code).to eq("200")
      expect(server.stats[:timeouts]).to eq(1)
    end
  end

  it "is off by default: slow responses arrive intact" do
    slow = lambda do |_env|
      sleep 0.3
      [200, {"content-type" => "text/plain"}, ["worth the wait"]]
    end

    with_server(slow) do |host, port, server|
      response = Net::HTTP.get_response(host, "/", port)

      expect(response.code).to eq("200")
      expect(response.body).to eq("worth the wait")
      expect(server.stats[:timeouts]).to eq(0)
    end
  end
end
