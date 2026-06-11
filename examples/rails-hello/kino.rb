# frozen_string_literal: true

# Kino configuration for the Rails hello-world example.
# Run with:  bundle exec kino
# (the kino CLI picks up config.ru and this file automatically)

# Rails needs :threaded mode today; see the ractor_probe.rb script in this
# directory for the current state of the :ractor experiment.
mode :threaded

port 9292
threads 5

environment "production"

# Kino's own access log (status-colored on color terminals). This is the
# server's view; Rails' request log is the app's view; both interleave
# on stdout.
log_requests true
