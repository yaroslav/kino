# frozen_string_literal: true

Warning[:experimental] = false

require_relative "bench_app"
require_relative "ractor_wrapper"

run RactorPool.new(BENCH_APP)
