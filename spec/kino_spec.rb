# frozen_string_literal: true

RSpec.describe Kino do
  it "has a version number" do
    expect(Kino::VERSION).not_to be nil
  end

  # The rb_ext_ractor_safe canary: without it, ANY native call from a
  # non-main ractor raises Ractor::UnsafeError, and worker ractors are
  # this gem's entire reason to exist.
  it "can call natives from a non-main Ractor" do
    result = Ractor.new { Kino::Native.queue_stats(0) }.value

    expect(result).to eq([0, 0])
  end
end
