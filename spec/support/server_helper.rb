# frozen_string_literal: true

require "async"
require "net/http"
require "socket"
require "timeout"
require "tmpdir"
require_relative "http_response"

module ServerHelper
  # A worker_loop that runs each job as an Async task, `concurrency` at
  # a time; with `returned` (a port), it reports :returned there once
  # serve returned.
  def async_loop(concurrency, returned: nil)
    Ractor.shareable_proc do |worker|
      Sync { |task| worker.serve(concurrency: concurrency) { |job| task.async { job.call } } }
      returned << :returned if returned
    end
  end

  # A worker_loop that runs each job as a task on a reactor of
  # `reactor_class` (an Async::Reactor, such as one that tallies what its
  # fibers do), `concurrency` at a time.
  def loop_on(reactor_class, concurrency)
    Ractor.shareable_proc do |worker|
      Fiber.blocking do
        reactor = reactor_class.new
        reactor.run(finished: false) { |task| worker.serve(concurrency: concurrency) { |job| task.async { job.call } } }.wait
      ensure
        Fiber.set_scheduler(nil)
      end
    end
  end

  # What every server of an example starts with: none, unless it runs
  # under a worker_loop (BothLoops#it_in_both_loops).
  def server_defaults = {}

  # Boot a server on an ephemeral port, yield the address, always shut down.
  # Specs default to a small threaded topology; ractor-mode specs override.
  def with_server(app, **opts)
    opts = {workers: 1, threads: 2, mode: :threaded}.merge(opts)
    server = Kino::Server.new(app, **opts).start
    yield "127.0.0.1", server.port, server
  ensure
    server&.shutdown
  end

  # A GET over TLS; the fixture cert is self-signed, so it is not verified.
  def https_get(host, port, path)
    http = Net::HTTP.new(host, port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    http.start { |session| session.get(path) }
  end

  # The worker_loop that a kino.rb holding `code` sets.
  def worker_loop_from(code)
    Dir.mktmpdir("kino-config") do |dir|
      path = File.join(dir, "kino.rb")
      File.write(path, code)
      Kino::Configuration.new.load_file(path)[:worker_loop]
    end
  end

  # The job-fiber recipe in doc/worker-loop.md: its ruby block that calls scheduler.resume(,
  # loaded as a kino.rb.
  def documented_job_fibers
    doc = File.read(File.expand_path("../../doc/worker-loop.md", __dir__))
    recipe = doc[/^```ruby\n((?:(?!^```).)*?scheduler\.resume\((?:(?!^```).)*?)^```$/m, 1]
    expect(recipe).not_to be_nil, "doc/worker-loop.md has no ruby block that calls scheduler.resume("
    worker_loop_from(recipe).tap do |worker_loop|
      expect(worker_loop).not_to be_nil, "doc/worker-loop.md's ruby block that calls scheduler.resume( sets no worker_loop"
    end
  end

  # Boot a worker_loop server (Async, 8 jobs at a time, on 2 workers of one thread), which serves
  # plain HTTP/1.1 connections inline, and yield it once every worker's loop is up, a probe
  # connection placed inline on each worker thread and closed again: a connection accepted before
  # then goes through hyper instead.
  def with_inline_server(app, mode:, **opts)
    opts = {workers: 2, threads: 1, worker_loop: async_loop(8)}.merge(opts)
    with_server(app, mode: mode, **opts) do |host, port, server|
      await_inline(server, server.unix? ? server.url.delete_prefix("unix://") : port, opts[:workers] * opts[:threads])
      yield host, port, server
    end
  end

  # A new connection to `target`: a port on 127.0.0.1, or the path of a unix socket.
  def client_socket(target)
    socket = target.is_a?(String) ? UNIXSocket.new(target) : TCPSocket.new("127.0.0.1", target)
    socket.timeout = WsClient::DEADLINE
    socket
  end

  # Returns once each of the `workers` worker threads of `server` (on `target`, a port or a unix
  # socket's path, serving inline) has had a probe connection placed on it and closed again. A
  # round is decided as soon as each of its probes is placed on a worker or declined to hyper: one
  # that met a worker not serving yet is tried again a few milliseconds on, rather than waited out.
  def await_inline(server, target, workers)
    wait_for("every worker serving inline", every: 0.005) do
      declined = server.stats[:inline][:declined]
      probes = Array.new(workers) { client_socket(target) }
      wait_for("the probes placed or declined", every: 0.001) do
        inline = server.stats[:inline]
        inline[:connections] + inline[:declined] - declined >= workers
      end
      server.stats[:worker_status].count { |row| row[:inline_connections].positive? } == workers
    ensure
      probes&.each(&:close)
      wait_for("the probes to close", every: 0.001) { server.stats[:inline][:connections].zero? }
    end
  end

  # Write `bytes` on a new connection to `target` (a port, or a unix socket's path), then read
  # until the server closes it, or until `wait` seconds pass with nothing new.
  def raw_exchange(target, bytes, wait: 1.0)
    read = +"".b
    socket = client_socket(target)
    socket.write(bytes)
    while socket.wait_readable(wait)
      chunk = socket.read_nonblock(65_536, exception: false)
      break if chunk.nil?

      read << chunk unless chunk == :wait_readable
    end
    read
  rescue Errno::ECONNRESET
    read
  ensure
    socket&.close
  end

  # A connection whose receive window is small (`receive_buffer` bytes), so a response it does
  # not read soon fills it.
  def narrow_connection(port, receive_buffer: 16_384)
    socket = Socket.new(:INET, :STREAM)
    socket.timeout = WsClient::DEADLINE
    socket.setsockopt(:SOCKET, :RCVBUF, receive_buffer)
    socket.connect(Socket.sockaddr_in(port, "127.0.0.1"))
    socket
  end

  # One response off `io` (a connection, or a StringIO of what one read), framed by its chunks or
  # its content-length: [status line, headers (names downcased), body]. Fails unless it is all in
  # within `within` seconds.
  def read_response(io, within: WsClient::DEADLINE)
    Timeout.timeout(within) do
      status, headers = HttpResponse.head(io)
      [status, headers, HttpResponse.body(io, headers)]
    end
  rescue Timeout::Error
    raise "no complete response within #{within} s"
  end

  # How many descriptors this process has open, a server in it included.
  def open_fds = Dir.children("/dev/fd").size

  # Run the block with RUBY_MAX_CPU set to `value` (unset when nil), then
  # restore what it was.
  def with_max_cpu(value)
    previous = ENV["RUBY_MAX_CPU"]
    value.nil? ? ENV.delete("RUBY_MAX_CPU") : ENV["RUBY_MAX_CPU"] = value
    yield
  ensure
    previous.nil? ? ENV.delete("RUBY_MAX_CPU") : ENV["RUBY_MAX_CPU"] = previous
  end

  # The native id of `server`, which the natives and Kino::WorkerRow take.
  def server_id(server) = server.instance_variable_get(:@id)

  # Retire the worker on dispatch `slot` natively, as the pool scaler
  # would, whatever the pool thinks: what a worker meets when a retirement
  # races something that just happened to it.
  def retire_slot(server, slot)
    id = server_id(server)
    generation = Kino::WorkerRow.all(id).find { |row| row.slot == slot }.generation
    Kino::Native.retire_slot(id, slot, generation)
  end

  # GET `path` from the server on `port`, on a connection of its own: the response, or a failure
  # naming the path unless it comes within `within` seconds. Never retried, so a request the
  # server loses fails here.
  def get_response(port, path, within: 5)
    Net::HTTP.start("127.0.0.1", port, read_timeout: within, max_retries: 0) { |http| http.get(path) }
  rescue Net::ReadTimeout
    raise "GET #{path} got no answer within #{within}s"
  end

  # The body of #get_response.
  def get(port, path, **opts) = get_response(port, path, **opts).body

  # POST `kib` KiB to `path` as a chunked body, 1 KiB a chunk: the
  # response's status line, or nil once the server dropped the connection.
  def post_chunked(port, path, kib)
    socket = client_socket(port)
    socket.write("POST #{path} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
    kib.times { socket.write("400\r\n#{"x" * 0x400}\r\n") }
    socket.write("0\r\n\r\n")
    socket.gets
  rescue Errno::EPIPE, Errno::ECONNRESET
    # The server tore the connection down while the body was still going.
    nil
  ensure
    socket&.close
  end

  # Spawn the real `kino` CLI as a subprocess on an ephemeral port (so
  # concurrent spec runs never collide), wait for the startup banner, and
  # yield the bound port plus the stdout capture path. Always reaps.
  def with_cli_server(dir, config_body, rackup_body)
    rackup = File.join(dir, "config.ru")
    File.write(rackup, rackup_body)
    config = File.join(dir, "kino.rb")
    File.write(config, "port 0\n#{config_body}")
    out = File.join(dir, "stdout.log")
    exe = File.expand_path("../../exe/kino", __dir__)

    pid = spawn(Gem.ruby, exe, "-C", config, rackup,
      chdir: dir, out: out, err: File::NULL)
    port = nil
    100.times do
      port = File.read(out)[%r{listening: https?://[^:]+:(\d+)}, 1]&.to_i
      break if port
      sleep 0.05
    end
    raise "kino CLI never reported a listening port (see #{out})" unless port

    yield port, out
  ensure
    if pid
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
  end

  def native_teardown(id)
    Kino::Native.stop_accepting(id)
    Kino::Native.close_queue(id)
    Kino::Native.shutdown_runtime(id, 200)
    Kino::Native.control_stop(id)
  end
end

# Defines an example twice: in Kino's own loop, then under a worker_loop
# on Async, for servers started with server_defaults.
module BothLoops
  def it_in_both_loops(description, &example)
    it(description, &example)
    it("#{description}, under a worker_loop") do
      define_singleton_method(:server_defaults) { {worker_loop: async_loop(8)} }
      instance_exec(&example)
    end
  end
end

RSpec.configure do |config|
  config.include ServerHelper
  config.extend BothLoops
end
