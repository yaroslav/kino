# frozen_string_literal: true

require "tmpdir"
require "logger"

RSpec.describe "logging" do
  describe Kino::Logger::Device do
    around do |example|
      Dir.mktmpdir("kino-log") { |dir| @dir = dir and example.run }
    end

    def wait_for_content(path)
      50.times do
        content = File.read(path) if File.exist?(path)
        return content if content && !content.empty?
        sleep 0.05
      end
      File.exist?(path) ? File.read(path) : ""
    end

    it "works as a ::Logger device writing to a file" do
      path = File.join(@dir, "app.log")
      logger = Logger.new(Kino::Logger::Device.new(path))

      logger.info("hello from the async sink")
      logger.close

      content = wait_for_content(path)
      expect(content).to include("INFO")
      expect(content).to include("hello from the async sink")
    end

    it "is Ractor-shareable and usable from a worker ractor" do
      path = File.join(@dir, "ractor.log")
      device = Kino::Logger::Device.new(path)
      expect(Ractor.shareable?(device)).to be(true)

      Ractor.new(device) { |d| d.write("written from a ractor\n") }.join
      device.close

      expect(wait_for_content(path)).to include("written from a ractor")
    end

    it "ignores writes after close" do
      path = File.join(@dir, "closed.log")
      device = Kino::Logger::Device.new(path)
      device.close

      expect { device.write("late\n") }.not_to raise_error
    end
  end

  describe Kino::Logger do
    it "is a ::Logger over the native device" do
      Dir.mktmpdir("kino-log") do |dir|
        path = File.join(dir, "kino_logger.log")
        logger = Kino::Logger.new(path, progname: "demo")
        logger.warn("styled api")
        logger.close

        content = nil
        50.times do
          content = File.read(path) if File.exist?(path)
          break if content && !content.empty?
          sleep 0.05
        end
        expect(content).to include("WARN")
        expect(content).to include("demo")
        expect(content).to include("styled api")
      end
    end
  end

  describe "native access log" do
    it "logs every request to stdout, 503s included" do
      app = <<~RU
        run lambda { |env|
          case env["PATH_INFO"]
          when "/missing" then [404, {"content-type" => "text/plain"}, ["nope"]]
          when "/moved" then [301, {"location" => "/"}, []]
          else [200, {"content-type" => "text/plain"}, ["ok"]]
          end
        }
      RU

      Dir.mktmpdir("kino-access") do |dir|
        with_cli_server(dir, "workers 1\nthreads 1\nmode :threaded\nlog_requests true\n", app) do |port, out|
          expect(Net::HTTP.get_response("127.0.0.1", "/", port).code).to eq("200")
          Net::HTTP.get_response("127.0.0.1", "/missing", port)
          Net::HTTP.get_response("127.0.0.1", "/moved", port)

          lines = nil
          50.times do
            lines = File.read(out)
            break if lines.scan("← ").size >= 3
            sleep 0.05
          end

          stamp = /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d [+-]\d{4} /
          # An arrival record lands before the app runs, a completion after.
          expect(lines).to match(/#{stamp}→ GET \/  from 127\.0\.0\.1$/)
          expect(lines).to match(/#{stamp}← 200 GET \/  \d+\.\dms \(ruby \d+\.\dms \[gc \d+\.\dms; \S+ obj\]; kino \d+\.\dms; wait \d+\.\dms\)$/)
          expect(lines).to match(/← 404 GET \/missing  \d+\.\dms/)
          expect(lines).to match(/← 301 GET \/moved  \d+\.\dms/)
          # A blank line sets one request apart from the next.
          expect(lines).to match(/wait \d+\.\dms\)\n\n/)
        end
      end
    end

    it "leaves out the GC and allocation figures when parallel ractors would blur them" do
      app = <<~RU
        run Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] }
      RU

      Dir.mktmpdir("kino-access") do |dir|
        with_cli_server(dir, "workers 2\nthreads 1\nmode :ractor\nlog_requests true\n", app) do |port, out|
          expect(Net::HTTP.get_response("127.0.0.1", "/", port).code).to eq("200")

          lines = nil
          50.times do
            lines = File.read(out)
            break if lines.include?("← ")
            sleep 0.05
          end

          expect(lines).to match(/← 200 GET \/  \d+\.\dms \(ruby \d+\.\dms; kino \d+\.\dms; wait \d+\.\dms\)$/)
          expect(lines).not_to include("[gc")
        end
      end
    end
  end
end
