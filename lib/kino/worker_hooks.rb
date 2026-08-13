# frozen_string_literal: true

module Kino
  # @private
  # The worker-context lifecycle hooks, bundled so one frozen, shareable
  # value crosses into each worker (a ractor in :ractor mode) instead of
  # several bare procs. Any member may be nil. A Data instance is frozen,
  # so it is Ractor.shareable? exactly when its members are (nil, or a
  # Ractor.shareable_proc), letting it ride the ractor boundary like the app.
  WorkerHooks = Data.define(:on_error, :after_worker_boot, :after_request_complete)
end
