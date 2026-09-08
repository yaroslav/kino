# frozen_string_literal: true

# A monitor that counts its scans and can be told to raise on one.
class CountingMonitor < Kino::Monitor
  attr_reader :scans

  def initialize(tick:, raise_on: nil)
    super(name: "counting", tick: tick)
    @scans = 0
    @raise_on = raise_on
  end

  private

  def scan
    @scans += 1
    raise "scan #{@scans} failed" if @scans == @raise_on
  end
end

RSpec.describe Kino::Monitor do
  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  it "scans every tick on its own thread until stopped" do
    monitor = CountingMonitor.new(tick: 0.01).start
    deadline = monotonic + 2
    sleep 0.01 until monitor.scans >= 3 || monotonic > deadline
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
      deadline = monotonic + 2
      sleep 0.01 until monitor.scans >= 4 || monotonic > deadline
      monitor.stop
    end

    expect(monitor.scans).to be >= 4
    expect(err).to include("counting tick error: RuntimeError: scan 2 failed")
  end
end
