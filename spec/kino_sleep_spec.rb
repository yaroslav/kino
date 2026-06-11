# frozen_string_literal: true

RSpec.describe "Kino.sleep" do
  # The strict contract is "never wakes early". The upper bound only
  # guards against pathology (a lost wakeup, a stuck loop): shared CI
  # runners routinely overshoot any sleep by 100ms+, so precision is a
  # benchmark topic (doc/benchmarks.md), not a spec assertion.
  overshoot = 0.5

  def elapsed
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
  end

  it "sleeps at least the requested duration" do
    time = elapsed { Kino.sleep(0.05) }

    expect(time).to be_between(0.05, 0.05 + overshoot)
  end

  it "handles durations longer than one internal chunk" do
    time = elapsed { Kino.sleep(0.12) }

    expect(time).to be_between(0.12, 0.12 + overshoot)
  end

  it "stays interruptible by Thread#kill" do
    thread = Thread.new { Kino.sleep(30) }
    sleep 0.05
    thread.kill

    expect(thread.join(1)).to eq(thread), "Kino.sleep must be killable within ~a tick"
  end

  it "works inside a non-main Ractor" do
    time = Ractor.new do
      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Kino.sleep(0.05)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    end.value

    expect(time).to be_between(0.05, 0.05 + overshoot)
  end

  it "rejects negative durations" do
    expect { Kino.sleep(-1) }.to raise_error(ArgumentError)
  end
end
