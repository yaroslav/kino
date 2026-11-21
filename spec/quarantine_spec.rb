# frozen_string_literal: true

require "json"

RSpec.describe "quarantine" do
  # A handler that blocks on a shared gate until released, to hold a slot.
  def gated_app(gate)
    lambda do |_env|
      gate.pop
      [200, {"content-type" => "text/plain"}, ["done"]]
    end
  end

  it "does not start a monitor when quarantine_timeout is unset" do
    app = ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] }
    with_server(app, mode: :threaded, workers: 1, threads: 2) do |_h, _p, server|
      expect(server.instance_variable_get(:@quarantine_monitor)).to be_nil
    end
  end

  it "quarantines a wedged threaded slot and restores capacity" do
    gate = Queue.new
    server = Kino::Server.new(gated_app(gate), mode: :threaded, workers: 1, threads: 2,
      quarantine_timeout: 0.2).start
    host, port = "127.0.0.1", server.port

    wedge = Thread.new { Net::HTTP.get_response(host, "/", port) }
    # Wait for the monitor (0.5s tick, 0.2s timeout) to quarantine + replace.
    wait_for("a quarantine", every: 0.05) { server.stats[:quarantined] >= 1 }
    expect(server.stats[:quarantined]).to eq(1)
    expect(server.stats[:worker_status].count { |w| w[:quarantined] }).to eq(1)
    # A replacement slot was registered, so total slots grew past the
    # original two.
    expect(server.stats[:worker_status].length).to be > 2

    # Release the gate; the wedged request still completes (never killed).
    gate << :go
    expect(wedge.value.code).to eq("200")
  ensure
    loop do
      done = begin
        gate.num_waiting.zero?
      rescue
        true
      end
      break if done

      gate << :go
    end
    server&.shutdown(timeout: 0.2)
  end

  it "quarantines a wedged ractor worker and spawns a replacement" do
    # A Ractor cannot close over a Queue, so hold the slot with a bounded
    # Kino.sleep long enough to exceed the 0.2s quarantine window and the
    # detection deadline, then return on its own so the request thread and
    # shutdown stay bounded.
    wedging = Ractor.shareable_proc do |env|
      Kino.sleep(3) if env["PATH_INFO"] == "/wedge"
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end

    server = Kino::Server.new(wedging, mode: :ractor, workers: 2, threads: 1,
      quarantine_timeout: 0.2).start
    host, port = "127.0.0.1", server.port

    wedge = Thread.new { Net::HTTP.get_response(host, "/wedge", port) }
    wait_for("a quarantine and its replacement", timeout: 6, every: 0.05) do
      server.stats[:quarantined] >= 1 && server.stats[:worker_status].length > 2
    end

    expect(server.stats[:quarantined]).to eq(1)
    expect(server.stats[:worker_status].any? { |w| w[:quarantined] }).to be(true)
    # A replacement slot was registered, so slots grew past the original two.
    expect(server.stats[:worker_status].length).to be > 2

    wedge.join
  ensure
    server&.shutdown(timeout: 0.2)
  end

  it "stops replacing at quarantine_max and runs degraded" do
    gate = Queue.new
    server = Kino::Server.new(gated_app(gate), mode: :threaded, workers: 1, threads: 3,
      quarantine_timeout: 0.2, quarantine_max: 1).start
    host, port = "127.0.0.1", server.port

    # Wedge two of the three slots; only one quarantine is allowed.
    wedges = Array.new(2) { Thread.new { Net::HTTP.get_response(host, "/", port) } }
    wait_for("a quarantine", every: 0.05) { server.stats[:quarantined] >= 1 }
    sleep 1 # give the monitor several ticks to (not) exceed the cap

    expect(server.stats[:quarantined]).to eq(1)
  ensure
    gate << :go until begin
      gate.num_waiting
    rescue
      0
    end.zero?
    wedges&.each { |t| t.join(1) }
    server&.shutdown(timeout: 0.2)
  end

  it "replaces a wedged ractor only once even when both its slots wedge in the same tick" do
    # A single ractor (workers:1, threads:2) has two dispatch slots. Wedging
    # both means one monitor tick sees two slots past the timeout that map
    # to the same worker_index; replace must fire once for the ractor, not
    # once per slot.
    wedging = Ractor.shareable_proc do |env|
      Kino.sleep(3) if env["PATH_INFO"] == "/wedge"
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end

    server = Kino::Server.new(wedging, mode: :ractor, workers: 1, threads: 2,
      quarantine_timeout: 0.2).start
    host, port = "127.0.0.1", server.port

    wedges = Array.new(2) { Thread.new { Net::HTTP.get_response(host, "/wedge", port) } }
    wait_for("both slots quarantined and one replacement", timeout: 6, every: 0.05) do
      server.stats[:quarantined] >= 2 && server.stats[:worker_status].length == 4
    end

    expect(server.stats[:quarantined]).to eq(2)
    # One replacement ractor of 2 slots (2 original + 2 replacement = 4),
    # not one replacement per wedged slot (2 + 2 + 2 = 6).
    expect(server.stats[:worker_status].length).to eq(4)

    sleep 1 # a few more monitor ticks; must not grow past 4
    expect(server.stats[:worker_status].length).to eq(4)

    wedges.each(&:join)
  ensure
    server&.shutdown(timeout: 0.2)
  end
end
