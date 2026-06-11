# frozen_string_literal: true

# Boots Kino (threaded mode) with per-request APP logging, for the
# logging-cost study in doc/benchmarks.md:
#   ruby bench/logging_server.rb <kino|shared> [port] [workers] [threads]
# kino:   one line per request through Kino::Logger (native async sink)
# shared: one line per request through a shared ::Logger (mutex + write)
Warning[:experimental] = false

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "kino"
require_relative "bench_app"
require "etc"
require "logger"

variant = ARGV[0] || "kino"
port = Integer(ARGV[1] || 9292)
workers = Integer(ARGV[2] || Etc.nprocessors)
threads = Integer(ARGV[3] || 3)

path = "/tmp/kino-bench-app-#{variant}.log"
File.delete(path) if File.exist?(path)
logger = (variant == "kino") ? Kino::Logger.new(path) : Logger.new(path)

app = lambda do |env|
  logger.info("handled #{env["REQUEST_METHOD"]} #{env["PATH_INFO"]}")
  BENCH_APP.call(env)
end

puts "Kino #{Kino::VERSION}: app logging via #{variant} logger -> #{path}"
Kino::Server.run(app, port: port, workers: workers, threads: threads,
  mode: :threaded)
