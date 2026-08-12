# frozen_string_literal: true

require "json"

RSpec.describe "queue time" do
  let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  it "reports queue_time count and sum in server.stats and /metrics" do
    with_server(ok_app, mode: :threaded, workers: 1, threads: 2, control_bind: "127.0.0.1:0") do |host, port, server|
      5.times { Net::HTTP.get_response(host, "/", port) }

      qt = server.stats[:queue_time]
      expect(qt[:count]).to eq(5)
      expect(qt[:sum_seconds]).to be >= 0.0

      body = Net::HTTP.get_response("127.0.0.1", "/metrics", server.control_port).body
      expect(body).to include("# TYPE kino_request_queue_seconds histogram")
      expect(body).to match(/kino_request_queue_seconds_count 5/)
      expect(body).to match(/kino_request_queue_seconds_bucket\{le="\+Inf"\} 5/)
    end
  end

  it "shows larger waits under saturation than when idle" do
    slow = lambda do |_env|
      sleep 0.1
      [200, {"content-type" => "text/plain"}, ["done"]]
    end
    with_server(slow, mode: :threaded, workers: 1, threads: 1, control_bind: "127.0.0.1:0") do |host, port, server|
      # One worker, so concurrent requests queue behind the 0.1s handler.
      threads = Array.new(4) { Thread.new { Net::HTTP.get_response(host, "/", port) } }
      threads.each(&:join)

      qt = server.stats[:queue_time]
      expect(qt[:count]).to eq(4)
      # With 4 requests serialized behind a 0.1s handler on 1 worker, later
      # ones waited hundreds of ms, so the mean wait is clearly above zero.
      expect(qt[:sum_seconds]).to be > 0.1
    end
  end
end
