# HTTP/2 bench image: h2load + nginx layered on the compiled kino image.
# Build from the repo root:
#   docker build -f bench/Dockerfile -t kino-bench .
#   docker build -f bench/h2.Dockerfile -t kino-h2-bench .
# Run (load generator runs INSIDE the container; Docker-for-Mac port
# forwarding would distort the numbers):
#   docker run --rm kino-h2-bench
FROM kino-bench

RUN apt-get update && apt-get install -y --no-install-recommends \
      nghttp2-client nginx openssl \
    && rm -rf /var/lib/apt/lists/*

CMD ["bash", "bench/h2.sh", "5", "8", "8"]
