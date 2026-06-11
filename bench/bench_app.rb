# frozen_string_literal: true

# The benchmark app, shared verbatim by Puma and Kino (both modes).
# Ractor-shareable: captures nothing.
module BenchWork
  # Plain method, like real app code: JIT-friendly in every ractor.
  # (A self-referential lambda here defeats YJIT entirely; see README.)
  def self.fib(n) = (n < 2) ? n : fib(n - 1) + fib(n - 2)
end

BENCH_APP = Ractor.shareable_proc do |env|
  case env["PATH_INFO"]
  when "/plaintext"
    [200, {"content-type" => "text/plain"}, ["Hello, World!"]]
  when "/10k"
    [200, {"content-type" => "application/octet-stream"}, ["x" * 10_240]]
  when "/cpu"
    # ~1-2ms of pure-Ruby CPU work: the workload ractors exist for.
    [200, {"content-type" => "text/plain"}, [BenchWork.fib(20).to_s]]
  when "/io"
    # Simulated downstream call: the workload threads exist for.
    sleep 0.005
    [200, {"content-type" => "text/plain"}, ["io done"]]
  when "/io_native"
    # Same wait via the server's OS-clock sleep where available (Kernel
    # sleep wakes coarsely inside ractors); plain sleep elsewhere.
    # respond_to? matters: bundler evaluates kino.gemspec in every process
    # of this repo, which defines the Kino module with just VERSION in it.
    (defined?(Kino) && Kino.respond_to?(:sleep)) ? Kino.sleep(0.005) : sleep(0.005)
    [200, {"content-type" => "text/plain"}, ["io done"]]
  else
    [200, {"content-type" => "text/plain"}, ["ok"]]
  end
end
