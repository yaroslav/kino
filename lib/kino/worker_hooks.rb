# frozen_string_literal: true

module Kino
  # The hooks that run in worker context and so must be shareable (after_boot and
  # on_worker_exit run on the main thread).
  WORKER_HOOKS = %i[on_error after_worker_boot after_request_complete worker_loop].freeze
  private_constant :WORKER_HOOKS

  # @private
  # The worker-context lifecycle hooks, bundled so one frozen, shareable
  # value crosses into each worker (a ractor in :ractor mode) instead of
  # several bare procs. Any member may be nil. A Data instance is frozen,
  # so it is Ractor.shareable? exactly when its members are (nil, or a
  # Ractor.shareable_proc), letting it ride the ractor boundary like the app.
  # `worker_loop`, when set, runs the worker's loop in place of Kino's.
  # `access_timing` rides along: whether the worker measures the GC pause
  # and allocations around each app call for the access log's breakdown.
  WorkerHooks = Data.define(*WORKER_HOOKS, :access_timing)
end
