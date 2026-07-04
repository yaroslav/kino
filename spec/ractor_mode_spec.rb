# frozen_string_literal: true

require "tmpdir"

# Phase 3: the headline feature. Requests served by worker Ractors, true
# parallelism, supervised crash recovery.
RSpec.describe "ractor mode" do
  # A Ractor-shareable Rack app: captures nothing, builds everything
  # per-request inside the worker ractor.
  def shareable_app
    Ractor.shareable_proc do |env|
      case env["PATH_INFO"]
      when "/boom"
        # Exception (not StandardError) bypasses the worker rescue and kills
        # the whole ractor: the supervisor's problem.
        raise Exception, "deliberate hard crash" # rubocop:disable Lint/RaiseException
      when "/whoami"
        [200, {"content-type" => "text/plain"}, [Ractor.main? ? "main" : "worker-ractor"]]
      when "/meet"
        # Rendezvous: append our arrival to a shared file, then wait until
        # a second request has arrived too. Two requests can only "meet"
        # if they are in flight at the same time in different ractors;
        # serial dispatch would let the first finish before the second
        # enters. The deadline is a hang guard, not a timing assertion.
        meet_file = env["HTTP_X_MEET_FILE"]
        tag = "r#{Ractor.current.object_id}"
        File.open(meet_file, "a") { |f| f.flock(File::LOCK_EX) && f.write("#{tag}\n") }
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
        met = loop do
          break true if File.read(meet_file).lines.size >= 2
          break false if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          Kino.sleep(0.005)
        end
        [200, {"content-type" => "text/plain"}, ["#{met ? "met" : "alone"} #{tag}"]]
      else
        [200, {"content-type" => "text/plain"}, ["ok"]]
      end
    end
  end

  describe "mode resolution" do
    it "auto-selects :ractor for a shareable app" do
      server = Kino::Server.new(shareable_app, mode: :auto)
      expect(server.mode).to eq(:ractor)
    end

    it "falls back to :threaded for an unshareable app, with a warning" do
      unshareable = ->(_env) { [200, {}, ["hi"]] }
      server = nil
      expect { server = Kino::Server.new(unshareable, mode: :auto) }
        .to output(/not Ractor-shareable.*falling back/).to_stderr
      expect(server.mode).to eq(:threaded)
    end

    it "raises when :ractor is forced with an unshareable app" do
      unshareable = ->(_env) { [200, {}, ["hi"]] }
      expect { Kino::Server.new(unshareable, mode: :ractor) }
        .to raise_error(Kino::UnshareableAppError, /Ractor-shareable/)
    end

    it "raises when :ractor is forced with an unshareable on_error hook" do
      sink = [] # captured mutable state makes the proc unshareable
      hook = ->(error, _env) { sink << error }
      expect { Kino::Server.new(shareable_app, mode: :ractor, on_error: hook) }
        .to raise_error(Kino::Error, /Ractor-shareable on_error/)
    end

    it "delivers worker errors to a shareable on_error hook" do
      Dir.mktmpdir("kino-on-error") do |dir|
        log = File.join(dir, "errors.log").freeze
        hook = Ractor.shareable_proc do |error, env|
          File.write(log, "#{error.class}: #{error.message} at #{env["PATH_INFO"]}\n", mode: "a")
        end
        app = Ractor.shareable_proc do |env|
          raise "ractor kaput" if env["PATH_INFO"] == "/boom-soft"

          [200, {"content-type" => "text/plain"}, ["ok"]]
        end

        with_server(app, mode: :ractor, workers: 1, threads: 1, on_error: hook) do |host, port|
          expect(Net::HTTP.get_response(host, "/boom-soft", port).code).to eq("500")
          expect(Net::HTTP.get_response(host, "/", port).body).to eq("ok")
        end

        expect(File.read(log)).to include("RuntimeError: ractor kaput at /boom-soft")
      end
    end
  end

  it "serves requests from non-main ractors" do
    with_server(shareable_app, mode: :ractor, workers: 2, threads: 1) do |host, port|
      expect(Net::HTTP.get_response(host, "/whoami", port).body).to eq("worker-ractor")
    end
  end

  it "overlaps requests across threads within one ractor" do
    # Same rendezvous as the parallelism spec below, but on ONE ractor
    # with two threads: the requests can only meet if the threads overlap.
    with_server(shareable_app, mode: :ractor, workers: 1, threads: 2) do |host, port|
      Dir.mktmpdir("kino-meet") do |dir|
        meet_file = File.join(dir, "meet.txt")
        File.write(meet_file, "")

        bodies = 2.times.map {
          Thread.new do
            http = Net::HTTP.new(host, port)
            request = Net::HTTP::Get.new("/meet")
            request["x-meet-file"] = meet_file
            http.request(request).body
          end
        }.map(&:value)

        expect(bodies).to all(start_with("met")),
          "requests never overlapped: #{bodies.inspect}"
        # Same ractor this time: the tags must match.
        expect(bodies.map { |b| b.split.last }.uniq.size).to eq(1)
      end
    end
  end

  describe "crash recovery" do
    it "500s the crashed request, respawns, and keeps serving" do
      with_server(shareable_app, mode: :ractor, workers: 1, threads: 1) do |host, port, server|
        crashed = Net::HTTP.get_response(host, "/boom", port)
        expect(crashed.code).to eq("500")

        # Respawn is near-instant but asynchronous; poll briefly.
        recovered = nil
        20.times do
          recovered = begin
            Net::HTTP.get_response(host, "/ok", port)
          rescue
            nil
          end
          break if recovered&.code == "200"

          sleep 0.1
        end

        expect(recovered&.code).to eq("200")
        expect(server.stats[:respawns]).to eq(1)
      end
    end
  end

  describe "parallelism" do
    it "serves two requests concurrently, in two different ractors" do
      # Deterministic, no wall-clock thresholds (those proved unfixably
      # flaky on shared CI runners): each request records its arrival and
      # waits for the other; they can only meet if both are in flight at
      # once, each in its own ractor.
      with_server(shareable_app, mode: :ractor, workers: 2, threads: 1) do |host, port|
        Dir.mktmpdir("kino-meet") do |dir|
          meet_file = File.join(dir, "meet.txt")
          File.write(meet_file, "")

          responses = 2.times.map {
            Thread.new do
              http = Net::HTTP.new(host, port)
              request = Net::HTTP::Get.new("/meet")
              request["x-meet-file"] = meet_file
              http.request(request)
            end
          }.map(&:value)

          expect(responses.map(&:code)).to all(eq("200"))
          bodies = responses.map(&:body)
          expect(bodies).to all(start_with("met")),
            "requests never overlapped: #{bodies.inspect}"
          # ...and in two distinct worker ractors.
          expect(bodies.map { |b| b.split.last }.uniq.size).to eq(2)
        end
      end
    end
  end
end
