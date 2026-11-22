# frozen_string_literal: true

# Worker threads that share a GVL (every worker in :threaded mode, a
# Ractor's threads) take turns with it. A worker handed a request while
# it waited (by the shared queue, or on a connection it serves inline)
# has to win the GVL back to run it, while a worker already running keeps
# taking the requests queued for it without ever waiting. The handed
# request must not wait for those to run dry: a worker passes the GVL
# between requests once it has held it for a millisecond.
RSpec.describe "fairness among worker threads sharing a GVL" do
  def burst_size = 16

  # Each request holds the GVL for 5 ms of Ruby work, so the whole burst
  # lands in the queue while the first ones run, and the burst stays
  # inside Ruby's 100 ms timeslice. Each reports its thread's name as it
  # starts.
  def busy_app(starts)
    Ractor.shareable_proc do |_env|
      starts << Thread.current.name
      deadline = Kino.monotonic + 0.005
      nil until Kino.monotonic >= deadline
      [200, {"content-type" => "text/plain"}, ["ok"]]
    end
  end

  # Open the connections first, then write every request at once, from
  # another process so the client never competes for this GVL.
  def burst(host, port)
    script = <<~RUBY
      require "socket"
      socks = Array.new(#{burst_size}) { TCPSocket.new(#{host.inspect}, #{port}) }
      sleep 0.2
      socks.each { |s| s.write("GET / HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n") }
      socks.each { |s| s.readpartial(4096) }
    RUBY
    pid = spawn(Gem.ruby, "-e", script)
    Process.wait(pid)
    expect($?.success?).to be(true)
  end

  # The threads the burst's requests started on, in order, on a server
  # of two worker threads, both waiting as it comes.
  def start_order(**opts)
    starts = Ractor::Port.new
    with_server(busy_app(starts), **opts) do |host, port|
      sleep 0.2
      burst(host, port)
      Array.new(burst_size) { starts.receive }
    end
  end

  # The worker that wins the GVL passes it between requests once it has
  # held it a millisecond, so from the moment the other one waits for it,
  # the two take turns, a request or two each. When the other one joins
  # is the machine's to decide, so this counts turns, not positions.
  # Before, the running worker ran every request it could take before the
  # other started one: one turn at most, and a timeslice running out on a
  # slow runner adds no more than one.
  def expect_turns_taken(order)
    turns = order.each_cons(2).count { |before, after| before != after }
    expect(turns).to be >= 3, "the burst started on #{order}"
  end

  it_in_both_loops "runs a request handed to a waiting worker before the running worker drains the burst" do
    expect_turns_taken(start_order(**server_defaults))
  end

  # Inline, its connections' requests decide it; here its loop's takes do.
  it "does the same under a worker_loop whose requests come through its lane" do
    expect_turns_taken(start_order(worker_loop: async_loop(8), _inline: false))
  end

  it "does the same for two threads of one Ractor" do
    expect_turns_taken(start_order(mode: :ractor, threads: 2))
  end
end
