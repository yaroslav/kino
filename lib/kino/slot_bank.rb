# frozen_string_literal: true

module Kino
  # @private
  # Dispatch slots for a worker pool: fresh ones from the native registry,
  # or ones that retired workers handed back, reset for their next
  # occupant. The native side never removes a slot, so recycling is what
  # keeps the slot table from growing as an elastic pool breathes. Only
  # cleanly exited workers return slots; a crashed worker's slots are
  # abandoned (stale interrupt kicks and dead weak refs go down with
  # them).
  class SlotBank
    def initialize(server_id)
      @server_id = server_id
      @free = []
      @lock = Mutex.new
    end

    def claim
      id = @lock.synchronize { @free.pop }
      return Native.register_worker(@server_id) unless id

      Native.reset_slot(@server_id, id)
      id
    end

    def release(ids)
      @lock.synchronize { @free.concat(ids) }
    end
  end
end
