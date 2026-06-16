# Linux benchmark image: Ruby 4.0 + Rust toolchain + ab.
# Build from the repo root:  docker build -f bench/Dockerfile -t kino-bench .
# Run (load generator runs INSIDE the container; Docker-for-Mac port
# forwarding would distort the numbers):  docker run --rm kino-bench
FROM ruby:4.0-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl ca-certificates clang libclang-dev apache2-utils wrk \
      pkg-config libssl-dev libyaml-dev psmisc procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY . .

RUN bundle install && bundle exec rake compile

CMD ["bench/run.sh", "5", "64"]
