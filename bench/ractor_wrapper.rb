# frozen_string_literal: true

require "etc"

# A Rack wrapper that ships each request to a pool of worker ractors and
# waits for the answer: the natural first experiment for trying Ractors on
# whatever server you already run.
#
# It exists in this benchmark to measure what the Rack-level hop itself
# costs, independent of any server: the env must be reduced to a shareable
# subset, the request is copied main-ractor -> worker and the response
# copied back, and a server thread waits out the round trip. Kino's
# architecture is one answer to avoiding those costs (dispatch below the
# Rack contract); the numbers here are the motivation, not a knock on the
# host servers.
class RactorPool
  def initialize(app, workers: Integer(ENV.fetch("RACTOR_POOL", Etc.nprocessors)))
    @workers = Array.new(workers) do
      Ractor.new(app) do |wrapped|
        loop do
          payload, reply_port = Ractor.receive
          status, headers, body = wrapped.call(payload)
          chunks = []
          body.each { |c| chunks << c }
          body.close if body.respond_to?(:close)
          reply_port << [status, headers, chunks]
        end
      end
    end
    @counter = 0
    @lock = Mutex.new
  end

  def call(env)
    worker = @lock.synchronize { @workers[@counter = (@counter + 1) % @workers.size] }
    # Only the shareable subset of env survives the hop (no IO objects).
    payload = {
      "REQUEST_METHOD" => env["REQUEST_METHOD"].to_s,
      "PATH_INFO" => env["PATH_INFO"].to_s,
      "QUERY_STRING" => env["QUERY_STRING"].to_s
    }
    reply_port = Ractor::Port.new
    worker.send([payload, reply_port])
    reply_port.receive
  rescue Ractor::ClosedError, Ractor::Error
    # A worker died (an exception escaped the app). Fail this request
    # loudly instead of hanging every later request routed to the corpse.
    [500, {"content-type" => "text/plain"}, ["ractor pool worker died\n"]]
  end
end
