#!/usr/bin/env bash
# The follow-up studies behind doc/benchmarks.md, runnable in one session
# right after bench/run.sh: CPU recipe, topology sweep, /io worker scaling,
# logging costs, and memory footprint.
# Usage: bench/studies.sh [duration_seconds] [concurrency] [section]
#   section: all (default) | cpu | topology | io | logging | memory
set -u
cd "$(dirname "$0")/.."

DURATION="${1:-8}"
CONCURRENCY="${2:-64}"
ONLY="${3:-all}"
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

# proc_tree <root-pid>: the root and every descendant (puma master + all its
# forked workers, however the tree is nested), one PID per line.
proc_tree() {
  local pid=$1 child
  echo "$pid"
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    proc_tree "$child"
  done
}

# sum_rss <pids...>: resident set summed across the tree, in kB. RSS counts
# every shared (copy-on-write) page once PER process, so a fork cluster's
# total double-counts whatever the workers share with the master.
sum_rss() {
  ps -o rss= -p "$(echo "$*" | tr ' ' ',')" 2>/dev/null | awk '{s+=$1} END {print s+0}'
}

# sum_pss <pids...>: proportional set summed across the tree, in kB, or -1
# where /proc is absent (macOS). PSS splits each shared page across the
# processes mapping it, so the cluster total reflects unique physical memory:
# the fair comparison against a single-process server.
sum_pss() {
  [[ -d /proc ]] || { echo -1; return; }
  local total=0 p src v
  for p in "$@"; do
    src="/proc/$p/smaps_rollup"
    [[ -r $src ]] || src="/proc/$p/smaps"
    [[ -r $src ]] || continue
    v=$(awk '/^Pss:/ {s+=$2} END {print s+0}' "$src")
    total=$((total + v))
  done
  echo "$total"
}

# mem_case <label> -- <server command...>: serve 5s of /plaintext load, then
# report both RSS and PSS of the whole process tree.
mem_case() {
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
  local pids rss_kb pss_kb pss_col
  pids=$(proc_tree "$pid")
  rss_kb=$(sum_rss $pids)
  pss_kb=$(sum_pss $pids)
  if [[ ${pss_kb:-0} -ge 0 ]]; then
    pss_col="$((pss_kb / 1024)) MB"
  else
    pss_col="n/a"
  fi
  printf "%-26s %7s MB RSS %10s PSS\n" "$label" "$((rss_kb / 1024))" "$pss_col"
  stop_target "$pid"
}

section_cpu() {
  echo "=== CPU recipe (workers=cores, threads 1, tokio_threads 1) ==="
  run_case "puma-cpu-reference" /cpu -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
  run_case "kino-ractor-default-8x3" /cpu -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
  run_case "kino-ractor-recipe-8x1-tokio1" /cpu -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1
  run_case "kino-recipe-plaintext-cost" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1
  run_case "kino-recipe-io-cost" /io -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1 1
}

section_topology() {
  echo "=== Topology sweep (ractor mode, /plaintext) ==="
  run_case "kino-ractor-8x3" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
  run_case "kino-ractor-8x1" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 1
  run_case "kino-ractor-16x1" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" $((CORES * 2)) 1
}

section_io() {
  echo "=== /io worker scaling (ractor mode) ==="
  run_case "kino-ractor-32x1-io" /io -- bundle exec ruby bench/kino_server.rb ractor "$PORT" 32 1
  run_case "kino-ractor-32x1-io_native" /io_native -- bundle exec ruby bench/kino_server.rb ractor "$PORT" 32 1
  run_case "puma-io-reference" /io -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
  run_case "puma-io_native-reference" /io_native -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
}

section_logging() {
  echo "=== Logging costs (/plaintext) ==="
  run_case "kino-threaded-no-log" /plaintext -- bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
  run_case "kino-threaded-access-log" /plaintext -- env LOG_REQUESTS=1 bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
  run_case "kino-ractor-no-log" /plaintext -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
  run_case "kino-ractor-access-log" /plaintext -- env LOG_REQUESTS=1 bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
  run_case "app-log-kino-logger" /plaintext -- bundle exec ruby bench/logging_server.rb kino "$PORT" "$CORES" 3
  run_case "app-log-shared-logger" /plaintext -- bundle exec ruby bench/logging_server.rb shared "$PORT" "$CORES" 3
}

section_memory() {
  echo "=== Memory (RSS and PSS after 5s of /plaintext load) ==="
  mem_case "kino-ractor-8x3" -- bundle exec ruby bench/kino_server.rb ractor "$PORT" "$CORES" 3
  mem_case "kino-threaded-8x3" -- bundle exec ruby bench/kino_server.rb threaded "$PORT" "$CORES" 3
  mem_case "puma-cluster" -- bundle exec puma -q -w "$CORES" -t 3:3 -p "$PORT" bench/config.ru
}

case "$ONLY" in
  all)      section_cpu; section_topology; section_io; section_logging; section_memory ;;
  cpu)      section_cpu ;;
  topology) section_topology ;;
  io)       section_io ;;
  logging)  section_logging ;;
  memory)   section_memory ;;
  *) echo "unknown section: $ONLY (want: all|cpu|topology|io|logging|memory)" >&2; exit 1 ;;
esac
