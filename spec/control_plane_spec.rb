# frozen_string_literal: true

require "net/http"
require "json"
require "tmpdir"
require "socket"

RSpec.describe "control plane" do
  # Native-level boot: exercises the listener without the Ruby Server
  # object, so the thread lifecycle is visible on its own.
  def native_boot(**extra)
    Kino::Native.server_start({bind: "127.0.0.1", port: 0,
                               queue_depth: 8, queue_timeout_ms: 100}.merge(extra))
  end

  def native_teardown(id)
    Kino::Native.stop_accepting(id)
    Kino::Native.close_queue(id)
    Kino::Native.shutdown_runtime(id, 200)
    Kino::Native.control_stop(id)
  end

  def control_get(port, path)
    Net::HTTP.get_response("127.0.0.1", path, port)
  end

  it "does not listen unless asked" do
    id, _port, control_port = native_boot
    expect(control_port).to be_nil
  ensure
    native_teardown(id)
  end

  it "serves probes through the whole lifecycle" do
    id, _port, control_port = native_boot(control_bind: "127.0.0.1:0")
    expect(control_port).to be_a(Integer)

    expect(control_get(control_port, "/live").code).to eq("200")
    booting = control_get(control_port, "/ready")
    expect([booting.code, booting.body]).to eq(["503", "booting\n"])

    Kino::Native.control_ready(id)
    expect(control_get(control_port, "/ready").code).to eq("200")

    Kino::Native.stop_accepting(id)
    draining = control_get(control_port, "/ready")
    expect([draining.code, draining.body]).to eq(["503", "draining\n"])

    # The control thread outlives the main runtime and stops on request.
    Kino::Native.close_queue(id)
    Kino::Native.shutdown_runtime(id, 200)
    expect(control_get(control_port, "/live").code).to eq("200")
    Kino::Native.control_stop(id)
    expect { control_get(control_port, "/live") }.to raise_error(SystemCallError)
  end

  it "refuses a control bind it cannot claim" do
    blocker = TCPServer.new("127.0.0.1", 0)
    taken = blocker.addr[1]
    expect {
      native_boot(control_bind: "127.0.0.1:#{taken}")
    }.to raise_error(RuntimeError, /control bind failed/)
  ensure
    blocker&.close
  end

  describe "through Kino::Server" do
    let(:ok_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

    it "exposes control_port and stops the listener after shutdown" do
      server = Kino::Server.new(ok_app, mode: :threaded, workers: 1, threads: 2,
        control_bind: "127.0.0.1:0").start
      port = server.control_port
      expect(port).to be_a(Integer)
      expect(control_get(port, "/ready").code).to eq("200")

      server.shutdown
      expect { control_get(port, "/live") }.to raise_error(SystemCallError)
    ensure
      server&.shutdown
    end

    it "leaves control_port nil when the control plane is off" do
      with_server(ok_app) do |_host, _port, server|
        expect(server.control_port).to be_nil
      end
    end

    it "serves stats JSON with the same vocabulary as Server#stats" do
      with_server(ok_app, control_bind: "127.0.0.1:0") do |host, port, server|
        Net::HTTP.get_response(host, "/", port)

        body = control_get(server.control_port, "/stats").body
        json = JSON.parse(body)
        server.stats.each_key { |key| expect(json).to have_key(key.to_s) }
        expect(json["served"]).to eq(1)
        expect(json["state"]).to eq("ready")
        expect(json["version"]).to eq(Kino::VERSION)
      end
    end

    it "serves Prometheus metrics" do
      with_server(ok_app, control_bind: "127.0.0.1:0") do |host, port, server|
        3.times { Net::HTTP.get_response(host, "/", port) }

        body = control_get(server.control_port, "/metrics").body
        expect(body).to include("# TYPE kino_requests_served_total counter")
        expect(body).to include("kino_requests_served_total 3")
        expect(body).to include("kino_ready 1")
      end
    end

    it "guards the data endpoints with the token but never the probes" do
      with_server(ok_app, control_bind: "127.0.0.1:0", control_token: "s3cret") do |_h, _p, server|
        port = server.control_port
        expect(control_get(port, "/stats").code).to eq("401")
        expect(control_get(port, "/ready").code).to eq("200")
        expect(control_get(port, "/live").code).to eq("200")

        authed = Net::HTTP.start("127.0.0.1", port) do |http|
          request = Net::HTTP::Get.new("/stats")
          request["Authorization"] = "Bearer s3cret"
          http.request(request)
        end
        expect(authed.code).to eq("200")
      end
    end

    it "treats an empty control_token as auth off, uniformly" do
      with_server(ok_app, control_bind: "127.0.0.1:0", control_token: "") do |_h, _p, server|
        expect(control_get(server.control_port, "/stats").code).to eq("200")
      end
    end

    it "answers 404 for unknown paths and 405 for writes" do
      with_server(ok_app, control_bind: "127.0.0.1:0") do |_h, _p, server|
        port = server.control_port
        expect(control_get(port, "/nope").code).to eq("404")
        post = Net::HTTP.start("127.0.0.1", port) { |http| http.request(Net::HTTP::Post.new("/stats")) }
        expect(post.code).to eq("405")
      end
    end

    it "serves over a unix socket" do
      Dir.mktmpdir do |dir|
        path = File.join(dir, "kino-control.sock")
        with_server(ok_app, control_bind: "unix://#{path}") do |_h, _p, server|
          expect(server.control_port).to be_nil
          response = UNIXSocket.open(path) do |sock|
            sock.write("GET /live HTTP/1.1\r\nHost: kino\r\nConnection: close\r\n\r\n")
            sock.read
          end
          expect(response).to include("200")
          expect(response).to include("ok")
        end
        expect(File.exist?(path)).to be(false)
      end
    end

    it "refuses a unix control bind that is still live" do
      Dir.mktmpdir do |dir|
        path = File.join(dir, "kino-control.sock")
        blocker = UNIXServer.new(path)
        begin
          expect {
            Kino::Server.new(ok_app, mode: :threaded, workers: 1, threads: 2,
              control_bind: "unix://#{path}").start
          }.to raise_error(RuntimeError, /control bind failed/)
        ensure
          blocker.close
        end
      end
    end

    it "takes over a stale unix control socket" do
      Dir.mktmpdir do |dir|
        path = File.join(dir, "kino-control.sock")
        UNIXServer.new(path).close

        with_server(ok_app, control_bind: "unix://#{path}") do |_h, _p, server|
          expect(server.control_port).to be_nil
          response = UNIXSocket.open(path) do |sock|
            sock.write("GET /live HTTP/1.1\r\nHost: kino\r\nConnection: close\r\n\r\n")
            sock.read
          end
          expect(response).to include("200")
          expect(response).to include("ok")
        end
        expect(File.exist?(path)).to be(false)
      end
    end

    it "reports draining on /ready for the whole drain window" do
      slow = ->(_env) {
        sleep 0.5
        [200, {"content-type" => "text/plain"}, ["done"]]
      }
      server = Kino::Server.new(slow, mode: :threaded, workers: 1, threads: 1,
        control_bind: "127.0.0.1:0").start
      port = server.control_port

      request = Thread.new { Net::HTTP.get_response("127.0.0.1", "/", server.port) }
      sleep 0.1 # the slow request is now in flight
      drainer = Thread.new { server.shutdown }
      sleep 0.1 # shutdown has flipped the state and is draining

      draining = control_get(port, "/ready")
      expect([draining.code, draining.body]).to eq(["503", "draining\n"])

      expect(request.value.code).to eq("200")
      drainer.join
    ensure
      drainer&.join
      server&.shutdown
    end

    it "keeps answering stats while every worker is wedged" do
      wedged = ->(_env) {
        sleep 30
        [200, {"content-type" => "text/plain"}, ["late"]]
      }
      server = Kino::Server.new(wedged, mode: :threaded, workers: 1, threads: 2,
        control_bind: "127.0.0.1:0").start

      stuck = Array.new(2) {
        Thread.new {
          begin
            Net::HTTP.get_response("127.0.0.1", "/", server.port)
          rescue
            nil
          end
        }
      }
      sleep 0.2 # both worker threads are now inside the app

      json = JSON.parse(control_get(server.control_port, "/stats").body)
      expect(json["in_flight"]).to eq(2)
      expect(json["state"]).to eq("ready")
    ensure
      server&.shutdown(timeout: 0.1) # threaded stragglers are killed; bounded
      stuck&.each { |t| t.join(1) }
    end
  end
end
