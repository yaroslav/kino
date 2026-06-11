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
end

RSpec.configure do |config|
  config.include ServerHelper
end
