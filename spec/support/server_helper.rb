# frozen_string_literal: true

require "net/http"

module ServerHelper
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

RSpec.configure do |config|
  config.include ServerHelper
end
