# frozen_string_literal: true

module Kino
  # @private
  # One slot's row of Native.worker_stats, by name; `slot` is the :index of a Server#stats
  # worker_status row.
  WorkerRow = Struct.new(:slot, :served, :in_flight, :busy_ms, :quarantined, :retired,
    :websockets_open, :hijacked_connections, :generation) do
    # Every slot's row; none once the server is gone.
    def self.all(server_id) = Native.worker_stats(server_id).map { |row| new(*row) }

    # Whether the slot holds a request, an open WebSocket or a hijacked connection.
    def occupied? = in_flight.positive? || websockets_open.positive? || hijacked_connections.positive?
  end
end
