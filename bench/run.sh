#!/usr/bin/env bash
# Benchmark Kino (ractor + threaded) against Puma on identical apps.
# Usage: bench/run.sh [duration_seconds] [concurrency]
# Prefers hey, then wrk, then ab.
set -u
cd "$(dirname "$0")/.."

DURATION="${1:-5}"
CONCURRENCY="${2:-64}"
PORT=9293
CORES=$(getconf _NPROCESSORS_ONLN)
ENDPOINTS=(/plaintext /10k /cpu /io /io_native)

# YJIT for every server: that's what production runs.
export RUBY_YJIT_ENABLE=1

bench_url() {
  local url=$1
  if command -v hey >/dev/null; then
    hey -z "${DURATION}s" -c "$CONCURRENCY" "$url" 2>/dev/null | awk '/Requests\/sec/ {print $2}'
  elif command -v wrk >/dev/null; then
    wrk -d "${DURATION}s" -c "$CONCURRENCY" -t 8 "$url" 2>/dev/null | awk '/Requests\/sec/ {print $2}'
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

# A leftover server (e.g. one that binds with SO_REUSEPORT, like falcon)
# would silently split traffic with the next target and poison every
# number after it. Refuse to start until the port is genuinely dead.
wait_port_free() {
  for _ in $(seq 1 100); do
    curl -sf -m 1 "http://127.0.0.1:$PORT/" >/dev/null 2>&1 || return 0
    sleep 0.1
  done
  echo "FATAL: something is still serving on :$PORT — aborting" >&2
  exit 1
}

stop_target() {
  local pid=$1
  # TERM the wrapper and its children (bundle exec → server → forks).
  pkill -TERM -P "$pid" 2>/dev/null
  kill -TERM "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  sleep 0.5
  # Backstop: kill whoever still LISTENS on the port. Forked workers
  # (falcon's show up as plain "ruby") dodge name-based pkill entirely.
  if command -v lsof >/dev/null; then
    lsof -t -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | sort -u | xargs kill -9 2>/dev/null
  elif command -v fuser >/dev/null; then
    fuser -k -9 "$PORT/tcp" 2>/dev/null
  fi
  pkill -9 -f "kino_server.rb|falcon serve" 2>/dev/null
  wait_port_free
}

run_target() {
  local name=$1; shift
  echo "=== $name ==="
  wait_port_free
  "$@" >/tmp/kino-bench-$name.log 2>&1 &
  local pid=$!
  if ! wait_ready; then
    stop_target "$pid"
    return 1
  fi
  for endpoint in "${ENDPOINTS[@]}"; do
    rps=$(bench_url "http://127.0.0.1:$PORT$endpoint")
    printf "%-12s %12s req/s\n" "$endpoint" "$rps"
    sleep 2  # let the loopback stack settle; macOS stalls under back-to-back runs
  done
  stop_target "$pid"
}

for variant in ractor ractor-lanes ractor-shards threaded; do
  run_target "kino-$variant" bundle exec ruby bench/kino_server.rb "$variant" "$PORT" "$CORES" 3
done
run_target "puma" bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru

# "I already run Puma/Falcon, can't I just wrap my app in ractors?"
# Puma: single process, 10 threads, pool of 10 ractors (parallelism must
# come from the ractors). Falcon: its usual per-core forking, one ractor
# per fork (the fiber reactor blocks on Port#receive, so a bigger pool
# can't help it).
run_target "puma-ractor-wrap"   env RACTOR_POOL="$CORES" bundle exec puma -q -t 10:10 -p "$PORT" bench/wrapped.ru
run_target "falcon-ractor-wrap" env RACTOR_POOL=1 bundle exec falcon serve --bind "http://127.0.0.1:$PORT" --count "$CORES" --config bench/wrapped.ru
