# frozen_string_literal: true

# Kino configuration for the Rails hello-world example.
# Run with:  bundle exec kino
# (the kino CLI picks up config.ru and this file automatically)

# Threaded mode still works: bundle exec kino --mode threaded
mode :ractor

port 9292

environment "production"

# /live is a native WebSocket, and those need lanes: each socket's
# messages go to the worker that accepted it.
lanes true

# Kino's own access log (status-colored on color terminals). This is the
# server's view; Rails' request log is the app's view; both interleave
# on stdout.
log_requests true
