# frozen_string_literal: true

# The pool seam behind PoolScaler, exercised on both pools directly: the
# count the scaler compares against its floor and ceiling must drop the
# moment a worker is told to leave, not when it has finished leaving,
# or a slow exit lets the scaler retire the next worker too.
RSpec.describe "worker pools" do
  def native_boot
    Kino::Native.server_start({bind: "127.0.0.1", port: 0, queue_depth: 8, queue_timeout_ms: 100,
                               mode: "threaded", workers: 2, threads: 1})
  end

  def native_teardown(id)
    Kino::Native.stop_accepting(id)
    Kino::Native.close_queue(id)
    Kino::Native.shutdown_runtime(id, 200)
    Kino::Native.control_stop(id)
  end

  it "counts a retiring thread group out before its threads have exited" do
    id, = native_boot
    app = ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] }
    pool = Kino::ThreadedPool.new(id, app, threads: 1).start(2)
    expect(pool.active_count).to eq(2)

    expect(pool.retire(1)).to be(true)

    expect(pool.active_count).to eq(1)
    expect(pool.groups.keys).to eq([0])
  ensure
    Kino::Native.close_queue(id) if id
    pool&.shutdown(1)
    native_teardown(id) if id
  end

  it "counts a retiring ractor out before it has exited" do
    id, = native_boot
    app = Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] }
    supervisor = Kino::RactorSupervisor.new(id, app, workers: 2, threads: 1).start
    expect(supervisor.active_count).to eq(2)
    # Only a worker whose supervisor thread has assigned its slots is
    # retirable; the scaler retires from this listing too.
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    sleep 0.01 until supervisor.groups.size == 2 || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    expect(supervisor.retire(1)).to be(true)

    expect(supervisor.active_count).to eq(1)
    expect(supervisor.groups.keys).to eq([0])
  ensure
    Kino::Native.close_queue(id) if id
    supervisor&.shutdown(1)
    native_teardown(id) if id
  end
end
