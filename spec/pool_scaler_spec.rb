# frozen_string_literal: true

# The pool seam the scaler drives: worker groups (index => slot ids),
# grow, retire. Records what the scaler asked for.
class ScalerFakePool
  attr_reader :grown, :retired

  def initialize(groups)
    @groups = groups
    @grown = []
    @retired = []
    @next_index = groups.keys.max.to_i + 1
  end

  def active_count = @groups.size

  def groups = @groups.dup

  def grow
    index = @next_index
    @next_index += 1
    @groups[index] = [index]
    @grown << index
    index
  end

  def retire(index)
    @retired << index
    @groups.delete(index)
    true
  end
end

RSpec.describe Kino::PoolScaler do
  # A worker_stats row: [slot, served, in_flight, busy_ms, quarantined, retired].
  def row(slot, served:, in_flight: 0)
    [slot, served, in_flight, 0, false, false]
  end

  def scaler(pool, floor: 1, ceiling: 4, scale_down_after: 30)
    described_class.new(server_id: 0, pool: pool, floor: floor, ceiling: ceiling,
      scale_down_after: scale_down_after)
  end

  describe "scaling up" do
    it "grows by one worker once the queue has been non-empty on two consecutive ticks" do
      pool = ScalerFakePool.new({0 => [0]})
      s = scaler(pool)
      busy = [row(0, served: 1, in_flight: 1)]

      s.step(0.0, 0, busy)
      s.step(0.1, 3, busy)
      expect(pool.grown).to be_empty

      s.step(0.2, 3, busy)
      expect(pool.grown).to eq([1])
    end

    it "ignores a single-tick blip" do
      pool = ScalerFakePool.new({0 => [0]})
      s = scaler(pool)
      busy = [row(0, served: 1, in_flight: 1)]

      s.step(0.1, 5, busy)
      s.step(0.2, 0, busy)
      s.step(0.3, 5, busy)

      expect(pool.grown).to be_empty
    end

    it "keeps growing one per tick while pressure lasts" do
      pool = ScalerFakePool.new({0 => [0]})
      s = scaler(pool)
      busy = [row(0, served: 1, in_flight: 1)]

      4.times { |i| s.step(i / 10.0, 3, busy) }

      expect(pool.grown).to eq([1, 2, 3])
    end

    it "never grows past the ceiling" do
      pool = ScalerFakePool.new({0 => [0], 1 => [1], 2 => [2]})
      s = scaler(pool, ceiling: 3)
      busy = pool.groups.keys.map { |i| row(i, served: 1, in_flight: 1) }

      5.times { |i| s.step(i / 10.0, 9, busy) }

      expect(pool.grown).to be_empty
    end
  end

  describe "scaling down" do
    it "retires a worker above the floor once it has been idle for scale_down_after" do
      pool = ScalerFakePool.new({0 => [0], 1 => [1]})
      s = scaler(pool, floor: 1, scale_down_after: 10)

      # Worker 0 keeps serving between ticks; worker 1 never does. Ticks
      # are whole seconds so the idle arithmetic is exact.
      (0..12).each do |t|
        s.step(t, 0, [row(0, served: t + 1), row(1, served: 5)])
        expect(pool.retired).to be_empty if t < 11
      end

      expect(pool.retired).to eq([1])
    end

    it "does not count a worker as idle while its served count keeps moving" do
      pool = ScalerFakePool.new({0 => [0], 1 => [1]})
      s = scaler(pool, floor: 1, scale_down_after: 5)

      (0..20).each do |t|
        # in_flight is always 0 at the sampling instant, but the counter moves.
        s.step(t, 0, [row(0, served: 1), row(1, served: t + 1)])
      end

      expect(pool.retired).to eq([0])
    end

    it "never retires below the floor" do
      pool = ScalerFakePool.new({0 => [0], 1 => [1]})
      s = scaler(pool, floor: 2, scale_down_after: 2)
      idle = [row(0, served: 1), row(1, served: 1)]

      10.times { |t| s.step(t, 0, idle) }

      expect(pool.retired).to be_empty
    end

    it "retires at most one worker per tick" do
      pool = ScalerFakePool.new({0 => [0], 1 => [1], 2 => [2]})
      s = scaler(pool, floor: 1, scale_down_after: 2)
      idle = [row(0, served: 1), row(1, served: 1), row(2, served: 1)]

      s.step(0, 0, idle)
      s.step(1, 0, idle) # idle since 1
      s.step(3, 0, idle) # first retirement
      expect(pool.retired.size).to eq(1)

      s.step(4, 0, idle)
      expect(pool.retired.size).to eq(2)
    end

    it "treats every slot of a multi-thread worker as one unit" do
      pool = ScalerFakePool.new({0 => [0, 1], 1 => [2, 3]})
      s = scaler(pool, floor: 1, scale_down_after: 2)
      # Worker 1's second slot stays busy: the worker is not idle.
      rows = [row(0, served: 1), row(1, served: 1), row(2, served: 1), row(3, served: 1, in_flight: 1)]

      6.times { |t| s.step(t, 0, rows) }

      expect(pool.retired).to eq([0])
    end
  end
end
