# frozen_string_literal: true

# Proofs that the native layer is Ractor-safe, blocking takes release the
# GVL, and the unblock function (UBF) makes blocked workers interruptible.
# Everything later (the request queue) is these primitives at scale.
RSpec.describe "native primitives" do
  describe "blocking take" do
    it "delivers a value pushed while a Ractor is blocked" do
      id = Kino::Native._test_channel_create(8)
      ractor = Ractor.new(id) { |chan| Kino::Native._test_take(chan) }

      sleep 0.05 # let the ractor reach the blocking take
      Kino::Native._test_push(id, "payload")

      expect(ractor.value).to eq("payload")
    end

    it "returns nil once the channel is closed" do
      id = Kino::Native._test_channel_create(8)
      thread = Thread.new { Kino::Native._test_take(id) }

      sleep 0.05
      Kino::Native._test_close(id)

      expect(thread.value).to be_nil
    end

    it "drains buffered values before reporting closed" do
      id = Kino::Native._test_channel_create(8)
      Kino::Native._test_push(id, "buffered")
      Kino::Native._test_close(id)

      expect(Kino::Native._test_take(id)).to eq("buffered")
      expect(Kino::Native._test_take(id)).to be_nil
    end
  end

  describe "GVL release" do
    it "lets other threads run while blocked in a take" do
      id = Kino::Native._test_channel_create(8)
      counter = 0
      counting = Thread.new { loop { counter += 1 } }

      blocked = Thread.new { Kino::Native._test_take(id) }
      sleep 0.05
      before = counter
      sleep 0.2 # blocked thread holds no lock; counter must advance
      after = counter

      expect(after).to be > before
    ensure
      counting&.kill
      Kino::Native._test_close(id) if id
      blocked&.join(1)
    end
  end

  describe "UBF interruptibility" do
    it "allows Thread#kill to interrupt a blocked take promptly" do
      id = Kino::Native._test_channel_create(8)
      thread = Thread.new { Kino::Native._test_take(id) }

      sleep 0.05
      thread.kill

      expect(thread.join(1)).to eq(thread), "blocked take was not interrupted within 1s"
    end
  end

  # A raise into a thread that waits without the GVL comes out of the
  # native as any exception does, and leaves nothing held behind it.
  describe "Thread#raise into a native's wait" do
    it "raises out of Kino.sleep's wait within a tick, and it sleeps again afterwards" do
      slept = Thread::Queue.new
      thread = Thread.new do
        Kino.sleep(30)
      rescue => e
        slept << e
        Kino.sleep(0.05)
        :slept_again
      end
      wait_for("the thread asleep in Kino.sleep") { thread.status == "sleep" }
      error = nil
      raised = timed do
        thread.raise(RuntimeError, "raised into a sleep")
        error = slept.pop(timeout: 1)
      end

      expect(raised).to be < 0.5, "the raise took #{raised.round(2)} s to come out of Kino.sleep"
      expect(error&.backtrace&.first).to include("Kino::Native.sleep_chunk")
      expect(thread.value).to eq(:slept_again)
    end

    it "raises out of the shutdown's wait for hijacked connections, and the server serves on and stops" do
      with_server(->(_env) { [200, {}, ["ok"]] }) do |host, port, server|
        id = server.instance_variable_get(:@id)
        # Delivered at the wait itself: deferred until a blocking point,
        # which the native's release is.
        error = Thread.new do
          Thread.handle_interrupt(RuntimeError => :on_blocking) do
            Thread.current.raise(RuntimeError, "raised into the wait")
            Kino::Native.close_bridges(id)
          end
          nil
        rescue => e
          e
        end.value

        expect(error&.message).to eq("raised into the wait")
        expect(error.backtrace.first).to include("Kino::Native.close_bridges")
        expect(Net::HTTP.get(host, "/", port)).to eq("ok")
        expect(timed { server.shutdown }).to be < 2
      end
    end
  end

  describe "panic containment" do
    it "surfaces a native panic as a RuntimeError instead of killing the process" do
      expect { Kino::Native._test_panic }.to raise_error(
        RuntimeError, /panic in native blocking call: intentional test panic/
      )
    end
  end

  describe "ractor safety" do
    it "can call every native from a non-main Ractor" do
      result = Ractor.new do
        id = Kino::Native._test_channel_create(2)
        Kino::Native._test_push(id, "in-ractor")
        value = Kino::Native._test_take(id)
        Kino::Native._test_close(id)
        [value, Kino::Native._test_take(id)]
      end.value

      expect(result).to eq(["in-ractor", nil])
    end
  end
end
