# frozen_string_literal: true

# Boots Kino for benchmarking:
#   ruby bench/kino_server.rb <ractor|threaded|ractor-lanes|ractor-shards> [port] [workers] [threads] [tokio_threads] [io_threads]
Warning[:experimental] = false

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "kino"
require_relative "bench_app"
require "etc"

variant = ARGV[0] || "ractor"
mode = variant.start_with?("ractor") ? :ractor : :threaded
lanes = variant.end_with?("-lanes")
io_shards = variant.end_with?("-shards")
port = Integer(ARGV[1] || 9292)
workers = Integer(ARGV[2] || Etc.nprocessors)
threads = Integer(ARGV[3] || 3)
# "-" is the skip-this-positional placeholder.
opt_int = ->(arg) { Integer(arg) if arg && arg != "-" }
tokio_threads = opt_int.call(ARGV[4])
io_threads = opt_int.call(ARGV[5])
log_requests = ENV["LOG_REQUESTS"] == "1" # for the logging-cost study
# TLS and HTTP/2 toggles for the h2 study (bench/h2.sh).
tls = if ENV["KINO_TLS_CERT"] && ENV["KINO_TLS_KEY"]
  {cert: ENV["KINO_TLS_CERT"], key: ENV["KINO_TLS_KEY"]}
end
http2 = ENV["KINO_HTTP2"] != "0"

puts "Kino #{Kino::VERSION}: mode=#{mode} lanes=#{lanes} io_shards=#{io_shards} port=#{port} " \
     "workers=#{workers} threads=#{threads} tokio_threads=#{tokio_threads || "auto"} " \
     "io_threads=#{io_threads || "auto"} tls=#{!tls.nil?} http2=#{http2}"
Kino::Server.run(BENCH_APP, port: port, workers: workers, threads: threads,
  mode: mode, lanes: lanes, io_shards: io_shards, tokio_threads: tokio_threads,
  io_threads: io_threads,
  log_requests: log_requests, tls: tls, http2: http2)
