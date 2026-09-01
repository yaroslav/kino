#!/usr/bin/env bash
# HTTP/2 head-to-head: kino's native h2 against falcon (the other
# h2-native Ruby server) and against the classic pattern for servers
# without h2 — nginx terminating h2 and proxying HTTP/1.1 upstream
# (puma; plus kino-h1 behind the same nginx as the proxy-cost control).
# The h2-enabled kino boot also serves the --h1 lanes, so the h2-vs-h1
# codec delta comes from one process, not two boots.
#
# Every lane is measured with h2load (h2 and --h1 alike) so the load
# generator is identical everywhere. h2 lanes run CLIENTS connections
# with STREAMS concurrent streams each; --h1 lanes run CLIENTS*STREAMS
# connections, so total in-flight requests match. VM drift invalidates
# sequential ladders: for close calls, run the whole script again and
# compare runs, never adjacent lines of one run (doc/benchmarks.md).
#
# Linux-only, run under Docker (the load generator runs INSIDE the
# container; Docker-for-Mac port forwarding would distort the numbers):
#   docker build -f bench/Dockerfile -t kino-bench .
#   docker build -f bench/h2.Dockerfile -t kino-h2-bench .
#   docker run --rm kino-h2-bench
#
# Usage: bench/h2.sh [duration_seconds] [clients] [streams_per_client]
# Env:   KINO_VARIANT=ractor|threaded|ractor-lanes|ractor-shards
set -u
cd "$(dirname "$0")/.."

if [ "$(uname -s)" != "Linux" ]; then
  echo "h2 benchmarks run under Docker/Linux only; see the header of $0" >&2
  exit 1
fi

DURATION="${1:-5}"
CLIENTS="${2:-8}"
STREAMS="${3:-8}"
H1_CONNS=$((CLIENTS * STREAMS))
PORT=9293      # every measured target serves here
UPSTREAM=9295  # the h1 backend behind nginx
CORES=$(getconf _NPROCESSORS_ONLN)
VARIANT="${KINO_VARIANT:-ractor}"
ENDPOINTS=(/plaintext /10k /big-cookie /upload)

export RUBY_YJIT_ENABLE=1

command -v h2load >/dev/null || { echo "h2load required: brew install nghttp2" >&2; exit 1; }

WORKDIR=$(mktemp -d /tmp/kino-h2-bench.XXXXXX)
trap 'rm -rf "$WORKDIR"' EXIT

# A ~2 KB cookie for the HPACK lane and a 64 KB body for the upload lane.
BIG_COOKIE="s=$(head -c 996 /dev/zero | tr '\0' 'a'); t=$(head -c 996 /dev/zero | tr '\0' 'b')"
head -c 65536 /dev/urandom >"$WORKDIR/upload.bin"

# Throwaway self-signed cert shared by kino and nginx (falcon brings its
# own via the localhost gem); h2load does not verify certificates.
openssl req -x509 -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout "$WORKDIR/key.pem" -out "$WORKDIR/cert.pem" >/dev/null 2>&1

# h2load prints "finished in 5.00s, 12345.67 req/s, ..."; field 4 is req/s.
measure() { # measure <base-url> <h2|h1> <endpoint>
  local base=$1 proto=$2 endpoint=$3
  local args=()
  case $proto in
    h2) args+=(-c "$CLIENTS" -m "$STREAMS") ;;
    h1) args+=(--h1 -c "$H1_CONNS") ;;
  esac
  case $endpoint in
    /big-cookie) args+=(-H "cookie: $BIG_COOKIE") ;;
    # Wide client windows (-w/-W), or h2load's 64 KB defaults serialize
    # the 64 KB bodies and the lane measures the client, not the server.
    /upload) args+=(-d "$WORKDIR/upload.bin" -w 20 -W 25) ;;
  esac
  h2load -D "$DURATION" "${args[@]}" "$base$endpoint" 2>/dev/null \
    | awk '/finished in/ {print $4}'
}

run_lanes() { # run_lanes <name> <base-url> <h2|h1>
  local name=$1 base=$2 proto=$3
  for endpoint in "${ENDPOINTS[@]}"; do
    local rps
    rps=$(measure "$base" "$proto" "$endpoint")
    printf "%-16s %-4s %-12s %12s req/s\n" "$name" "$proto" "$endpoint" "${rps:-FAILED}"
    sleep 2 # let the loopback stack settle between runs
  done
}

wait_ready() { # wait_ready <base-url>
  for _ in $(seq 1 100); do
    curl -skf -m 1 "$1/" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server at $1 did not come up" >&2
  return 1
}

wait_port_free() { # wait_port_free <port>
  for _ in $(seq 1 100); do
    curl -sf -m 1 "http://127.0.0.1:$1/" >/dev/null 2>&1 || return 0
    sleep 0.1
  done
  echo "FATAL: something is still serving on :$1 — aborting" >&2
  exit 1
}

stop_pid() { # stop_pid <pid> <port>
  local pid=$1 port=$2
  pkill -TERM -P "$pid" 2>/dev/null
  kill -TERM "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  sleep 0.5
  # Backstop: kill whoever still LISTENS on the port (forked workers
  # dodge name-based pkill; the bench image ships fuser, not lsof).
  if command -v lsof >/dev/null; then
    lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u | xargs -r kill -9 2>/dev/null
  elif command -v fuser >/dev/null; then
    fuser -k -9 "$port/tcp" 2>/dev/null
  fi
  wait_port_free "$port"
}

echo "kino=$VARIANT workers=$CORES duration=${DURATION}s h2: ${CLIENTS}c x ${STREAMS}m / h1: ${H1_CONNS}c"
echo

# --- kino, native h2, plaintext (one boot serves h2c AND the h1 lanes) ---
bundle exec ruby bench/kino_server.rb "$VARIANT" "$PORT" "$CORES" 3 \
  >"$WORKDIR/kino-h2c.log" 2>&1 &
KINO_PID=$!
wait_ready "http://127.0.0.1:$PORT" && {
  run_lanes "kino-h2c" "http://127.0.0.1:$PORT" h2
  run_lanes "kino-h1c" "http://127.0.0.1:$PORT" h1
}
stop_pid "$KINO_PID" "$PORT"

# --- kino, native h2, TLS ---
env KINO_TLS_CERT="$WORKDIR/cert.pem" KINO_TLS_KEY="$WORKDIR/key.pem" \
  bundle exec ruby bench/kino_server.rb "$VARIANT" "$PORT" "$CORES" 3 \
  >"$WORKDIR/kino-tls.log" 2>&1 &
KINO_PID=$!
wait_ready "https://127.0.0.1:$PORT" && {
  run_lanes "kino-h2-tls" "https://127.0.0.1:$PORT" h2
  run_lanes "kino-h1-tls" "https://127.0.0.1:$PORT" h1
}
stop_pid "$KINO_PID" "$PORT"

# --- falcon, native h2, TLS (its usual per-core forking; bound to
# "localhost" so its localhost-gem development cert can be minted) ---
bundle exec falcon serve --bind "https://localhost:$PORT" --count "$CORES" \
  --config bench/config.ru >"$WORKDIR/falcon.log" 2>&1 &
FALCON_PID=$!
if wait_ready "https://localhost:$PORT"; then
  run_lanes "falcon-h2-tls" "https://localhost:$PORT" h2
fi
stop_pid "$FALCON_PID" "$PORT"

# --- nginx h2 termination in front of h1 backends ---
if command -v nginx >/dev/null; then
  cat >"$WORKDIR/nginx.conf" <<CONF
worker_processes auto;
pid $WORKDIR/nginx.pid;
error_log $WORKDIR/nginx-error.log;
events { worker_connections 1024; }
http {
  access_log off;
  upstream backend {
    server 127.0.0.1:$UPSTREAM;
    keepalive 64;
  }
  server {
    listen $PORT ssl;
    http2 on;
    # Default keepalive_requests (1000) closes each h2 connection after
    # 1000 requests and h2load never reopens it, capping every lane at
    # clients*1000/duration. Effectively unlimited for the bench.
    keepalive_requests 1000000;
    ssl_certificate $WORKDIR/cert.pem;
    ssl_certificate_key $WORKDIR/key.pem;
    location / {
      proxy_pass http://backend;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_set_header Host \$host;
    }
  }
}
CONF

  proxy_lane() { # proxy_lane <name> <backend-cmd...>
    local name=$1; shift
    wait_port_free "$UPSTREAM"
    "$@" >"$WORKDIR/$name-backend.log" 2>&1 &
    local backend_pid=$!
    wait_ready "http://127.0.0.1:$UPSTREAM" || { stop_pid "$backend_pid" "$UPSTREAM"; return 1; }
    nginx -c "$WORKDIR/nginx.conf" -g "daemon off;" >"$WORKDIR/$name-nginx.log" 2>&1 &
    local nginx_pid=$!
    if wait_ready "https://127.0.0.1:$PORT"; then
      run_lanes "$name" "https://127.0.0.1:$PORT" h2
    fi
    stop_pid "$nginx_pid" "$PORT"
    stop_pid "$backend_pid" "$UPSTREAM"
  }

  proxy_lane "nginx-puma" bundle exec puma -q -w "$CORES" -t 3:3 -p "$UPSTREAM" bench/config.ru
  proxy_lane "nginx-kino-h1" bundle exec ruby bench/kino_server.rb "$VARIANT" "$UPSTREAM" "$CORES" 3
else
  echo "nginx not found: skipping the h2-termination proxy lanes" >&2
fi
