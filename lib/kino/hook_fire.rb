# frozen_string_literal: true

module Kino
  # @private
  # Fires a lifecycle hook and turns a raise into a logged line instead of
  # letting it escape. Stateless and touches only its arguments plus
  # Native.log_error (already called from inside worker ractors today), so
  # it is safe to call from worker context: no main-ractor state is
  # captured.
  module HookFire
    module_function

    def fire(hook, name, *args)
      return unless hook

      begin
        hook.call(*args)
      rescue => e
        Native.log_error("#{name} hook raised #{e.class}: #{e.message}")
      end
    end
  end
end
