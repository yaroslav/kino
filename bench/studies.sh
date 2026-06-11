#!/usr/bin/env bash
# The follow-up studies behind doc/benchmarks.md, runnable in one session
# right after bench/run.sh: CPU recipe, topology sweep, /io worker scaling,
# logging costs, and memory footprint.
# Usage: bench/studies.sh [duration_seconds] [concurrency]
set -u
cd "$(dirname "$0")/.."

DURATION="${1:-8}"
CONCURRENCY="${2:-64}"
PORT=9293
CORES=$(getconf _NPROCESSORS_ONLN)

export RUBY_YJIT_ENABLE=1

bench_url() {
  local url="http://127.0.0.1:$PORT$1"
  if command -v hey >/dev/null; then
    hey -z "${DURATION}s" -c "$CONCURRENCY" "$url" 2>/dev/null | awk '/Requests\/sec/ {print $2}'
  elif command -v wrk >/dev/null; then
    wrk -d "${DURATION}s" -c "$CONCURRENCY" -t 8 "$url" 2>/dev/null | awk '/Requests\/sec:/ {print $2}'
  else
    ab -k -t "$DURATION" -n 2000000 -c "$CONCURRENCY" "$url" 2>/dev/null \
      | awk '/Requests per second/ {print $4}'
  fi
}

wait_ready() {
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server did not come up" >&2
  return 1
}

wait_port_free() {
  for _ in $(seq 1 100); do
    curl -sf -m 1 "http://127.0.0.1:$PORT/" >/dev/null 2>&1 || return 0
    sleep 0.1
  done
  echo "FATAL: something is still serving on :$PORT" >&2
  exit 1
}

stop_target() {
  local pid=$1
  pkill -TERM -P "$pid" 2>/dev/null
  kill -TERM "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  sleep 0.5
  if command -v lsof >/dev/null; then
    lsof -t -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | sort -u | xargs -r kill -9 2>/dev/null
  elif command -v fuser >/dev/null; then
    fuser -k -9 "$PORT/tcp" 2>/dev/null
  fi
  pkill -9 -f "kino_server.rb|logging_server.rb|falcon serve" 2>/dev/null
  wait_port_free
}

# run_case <label> <endpoint> -- <server command...>
run_case() {
  local label=$1 endpoint=$2
  shift 3
  wait_port_free
  "$@" >"/tmp/kino-study-$label.log" 2>&1 &
  local pid=$!
  if ! wait_ready; then
    echo "$label: FAILED TO START"
    stop_target "$pid"
    return 1
  fi
  local rps
  rps=$(bench_url "$endpoint")
  printf "%-34s %-12s %12s req/s\n" "$label" "$endpoint" "$rps"
  sleep 1
  stop_target "$pid"
}

# rss_case <label> -- <server command...>: serve 5s of load, report RSS of
# the whole process tree (master + forks for puma).
rss_case() {
  local label=$1
  shift 2
  wait_port_free
  "$@" >"/tmp/kino-study-$label.log" 2>&1 &
  local pid=$!
  if ! wait_ready; then
    stop_target "$pid"
    return 1
  fi
  ab -k -t 5 -n 2000000 -c "$CONCURRENCY" "http://127.0.0.1:$PORT/plaintext" >/dev/null 2>&1
  local rss_kb
  rss_kb=$( (echo "$pid"; pgrep -P "$pid"; pgrep -P "$(pgrep -P "$pid" | head -1)" 2>/dev/null) \
    | sort -u | xargs -r ps -o rss= -p 2>/dev/null | awk '{s+=$1} END {print s}')
  printf "%-34s %12s MB RSS\n" "$label" "$((rss_kb / 1024))"
  stop_target "$pid"
}

echo "=== CPU recipe (workers=cores, threads 1, tokio_threads 1) ==="
run_case "puma-cpu-reference" /cpu -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
run_case "kino-ractor-default-8x3" /cpu -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
run_case "kino-ractor-recipe-8x1-tokio1" /cpu -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1
run_case "kino-recipe-plaintext-cost" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1
run_case "kino-recipe-io-cost" /io -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1

echo "=== Topology sweep (ractor mode, /plaintext) ==="
run_case "kino-ractor-8x3" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
run_case "kino-ractor-8x1" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1
run_case "kino-ractor-16x1" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" $((CORES * 2)) 1

echo "=== /io worker scaling (ractor mode) ==="
run_case "kino-ractor-32x1-io" /io -- bundle exec ruby bench/kino_server.rb ractor "$PORT" 32 1
run_case "kino-ractor-32x1-io_native" /io_native -- bundle exec ruby bench/kino_server.rb ractor "$PORT" 32 1
run_case "puma-io-reference" /io -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
run_case "puma-io_native-reference" /io_native -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru

echo "=== Logging costs (/plaintext) ==="
run_case "kino-threaded-no-log" /plaintext -- bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
run_case "kino-threaded-access-log" /plaintext -- env LOG_REQUESTS=1 bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
run_case "kino-ractor-no-log" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
run_case "kino-ractor-access-log" /plaintext -- env LOG_REQUESTS=1 bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
run_case "app-log-kino-logger" /plaintext -- bundle exec ruby bench/logging_server.rb kino "$PORT" "$CORES" 3
run_case "app-log-shared-logger" /plaintext -- bundle exec ruby bench/logging_server.rb shared "$PORT" "$CORES" 3

echo "=== Memory (RSS after 5s of /plaintext load) ==="
rss_case "kino-ractor-8x3" -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
rss_case "kino-threaded-8x3" -- bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
rss_case "puma-cluster" -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
