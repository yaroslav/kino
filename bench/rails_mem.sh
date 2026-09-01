#!/usr/bin/env bash
# RSS + PSS for the Rails hello-world example: Kino :threaded (one process)
# vs a Puma cluster with and without preload_app!. Run inside the
# kino-rails-bench container. PSS (proportional set size) is the fair metric
# for a fork cluster: it splits each copy-on-write shared page across the
# workers mapping it, so preload's shared framework is not counted N times.
set -u
BENCH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP="$(cd "$BENCH/../examples/rails-hello" && pwd)"
cd "$APP"
export RUBY_YJIT_ENABLE=1 RAILS_ENV=production RACK_ENV=production
PORT=9293
export WORKERS=$(nproc)
THREADS=5
echo "workers=$WORKERS threads=$THREADS"
awk '/MemTotal|MemAvailable/ {print "  "$0}' /proc/meminfo

wait_ready() {
  for _ in $(seq 1 400); do
    curl -sf "http://127.0.0.1:$PORT/" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "NOT UP" >&2; return 1
}

tree_pids() { local pid=$1 c; echo "$pid"; for c in $(pgrep -P "$pid" 2>/dev/null); do tree_pids "$c"; done; }
field() { awk -v k="$1:" '$1==k {s+=$2} END {print s+0}' "$2"; }

report() {
  local label=$1 root=$2
  local pids; pids=$(tree_pids "$root" | sort -u)
  local n; n=$(echo $pids | wc -w)
  local rss=0 pss=0 shc=0 shd=0 prc=0 prd=0 p f
  for p in $pids; do
    f=/proc/$p/smaps_rollup
    [[ -r $f ]] || continue
    rss=$((rss + $(field Rss "$f")))
    pss=$((pss + $(field Pss "$f")))
    shc=$((shc + $(field Shared_Clean "$f")))
    shd=$((shd + $(field Shared_Dirty "$f")))
    prc=$((prc + $(field Private_Clean "$f")))
    prd=$((prd + $(field Private_Dirty "$f")))
  done
  printf "%-24s procs=%-2s RSS=%5sMB PSS=%5sMB | shared=%5sMB private=%5sMB\n" \
    "$label" "$n" $((rss/1024)) $((pss/1024)) $(((shc+shd)/1024)) $(((prc+prd)/1024))
  local psrss smempss
  psrss=$(ps -o rss= -p "$(echo $pids | tr ' ' ,)" 2>/dev/null | awk '{s+=$1} END {print int(s/1024)}')
  smempss=$(smem -H -c "pid pss" 2>/dev/null \
    | awk -v ids="|$(echo $pids | tr ' ' '|')|" '{ if (index(ids,"|"$1"|")) s+=$2 } END {print int(s/1024)}')
  echo "    cross-check: ps-RSS=${psrss}MB  smem-PSS=${smempss:-n/a}MB"
}

run_report() {
  local label=$1; shift
  "$@" >/tmp/srv.log 2>&1 &
  local pid=$!
  if ! wait_ready; then echo "$label: FAILED TO START"; tail -8 /tmp/srv.log; fi
  ab -k -t 5 -c 64 -n 2000000 "http://127.0.0.1:$PORT/" >/dev/null 2>&1
  [[ -d /proc/$pid ]] && report "$label" "$pid"
  pkill -TERM -P "$pid" 2>/dev/null; kill -TERM "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  pkill -9 -f "puma|ruby" 2>/dev/null; sleep 2
}

echo "=== Rails hello-world (edge Rails, production, ${WORKERS}x${THREADS}) ==="
run_report "kino-threaded"      bundle exec ruby -e 'require "kino"; require "./app"; Kino::Server.run(HelloApp, bind: "127.0.0.1", port: 9293, workers: Integer(ENV.fetch("WORKERS")), threads: 5, mode: :threaded)'
run_report "puma-NOpreload"     bundle exec puma -q -w "$WORKERS" -t "$THREADS:$THREADS" -C "$BENCH/puma_no_preload.rb" -p "$PORT" config.ru
run_report "puma-PRELOAD"       bundle exec puma -q -w "$WORKERS" -t "$THREADS:$THREADS" --preload  -p "$PORT" config.ru
