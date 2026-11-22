# frozen_string_literal: true

module Kino
  # @private
  # Dispatch slots for a worker pool: fresh ones from the native registry,
  # or ones retired or crashed workers handed back, reset once quiet. The
  # native side never removes a slot, so recycling keeps the table from
  # growing. The bank keeps each slot's generation as it handed it out, so
  # an id read for one worker never acts on a later one.
  class SlotBank
    def initialize(server_id)
      @server_id = server_id
      @free = []
      @generations = {}
      @lock = Mutex.new
    end

    def claim
      @lock.synchronize do
        @free.each_with_index do |id, index|
          next unless (generation = Native.reset_slot(@server_id, id))

          @free.delete_at(index)
          @generations[id] = generation
          return id
        end
      end
      id = Native.register_worker(@server_id)
      @lock.synchronize { @generations[id] = 0 }
      id
    end

    # The generation slot `id` had when this bank handed it out.
    def generation(id) = @lock.synchronize { @generations.fetch(id) }

    def release(ids)
      @lock.synchronize { @free.concat(ids) }
    end

    # Whether the workers on slots `ids` have taken a request since they got them.
    def served_any?(ids) = WorkerRow.all(@server_id).any? { |row| ids.include?(row.slot) && row.served.positive? }
  end
end
