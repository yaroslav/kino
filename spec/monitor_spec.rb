# frozen_string_literal: true

# A monitor that counts its scans and can be told to raise on one.
class CountingMonitor < Kino::Monitor
  attr_reader :scans

  def initialize(tick:, raise_on: nil, error: RuntimeError)
    super(name: "counting", tick: tick)
    @scans = 0
    @raise_on = raise_on
    @error = error
  end

  private

  def scan
    @scans += 1
    raise @error, "scan #{@scans} failed" if @scans == @raise_on
  end
end

RSpec.describe Kino::Monitor do
  it "scans every tick on its own thread until stopped" do
    monitor = CountingMonitor.new(tick: 0.01).start
    wait_for("three scans", timeout: 2, every: 0.01) { monitor.scans >= 3 }
    monitor.stop
    scans = monitor.scans

    expect(scans).to be >= 3
    sleep 0.05
    expect(monitor.scans).to eq(scans)
  end

  it "logs a raising scan and keeps going" do
    monitor = CountingMonitor.new(tick: 0.01, raise_on: 2)
    err = capture_native_stderr do
      monitor.start
      wait_for("four scans", timeout: 2, every: 0.01) { monitor.scans >= 4 }
      monitor.stop
    end

    expect(monitor.scans).to be >= 4
    expect(err).to include("counting tick error: RuntimeError: scan 2 failed")
  end

  # Ruby reported the crash as it happened; stopping the monitor at
  # shutdown must not raise it again. The thread sleeps a tick before it
  # dies and stop joins for two; a long tick lets the join see the crash.
  it "stops quietly after its thread died of an exception outside StandardError" do
    monitor = CountingMonitor.new(tick: 0.5, raise_on: 1, error: Exception)
    err = capture_native_stderr do
      monitor.start
      wait_for("the first scan", timeout: 2, every: 0.01) { monitor.scans >= 1 }

      expect { monitor.stop }.not_to raise_error
    end

    expect(err).to include("scan 1 failed (Exception)")
  end
end
