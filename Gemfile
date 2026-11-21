# frozen_string_literal: true

source "https://rubygems.org"

# Gem dependencies (runtime and development) live in kino.gemspec.
gemspec

gem "irb"

group :development do
  # Specs run worker_loop under Async; Kino itself never requires it.
  gem "async", "~> 2.0"
end

# Benchmark baselines (bench/run.sh).
gem "puma", "~> 8.0"
gem "falcon", "~> 0.52"
