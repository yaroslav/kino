# frozen_string_literal: true

require "tmpdir"
require "net/http"
require "rackup"

RSpec.describe "Rackup::Handler::Kino" do
  let(:handler) { Rackup::Handler.get(:kino) }

  it "is found by name through the rackup registry" do
    expect(handler).to be(Rackup::Handler::Kino)
  end

  it "advertises its server-specific -O options" do
    expect(handler.valid_options.keys).to include(
      "Workers=COUNT", "Threads=COUNT", "Mode=MODE", "Config=PATH"
    )
  end

  describe ".server_options" do
    around do |example|
      Dir.mktmpdir("kino-rackup") do |dir|
        Dir.chdir(dir) { example.run }
      end
    end

    it "maps Host and Port to bind and port" do
      options = handler.server_options(Host: "0.0.0.0", Port: 4000)

      expect(options).to include(bind: "0.0.0.0", port: 4000)
    end

    it "coerces the string values rackup's -O passes" do
      options = handler.server_options(Port: "3000", Workers: "4", Threads: "2", Mode: "threaded")

      expect(options).to include(port: 3000, workers: 4, threads: 2, mode: :threaded)
    end

    it "ignores the host's own bookkeeping options" do
      options = handler.server_options(
        Port: 3000, environment: "development", pid: nil, config: "config.ru",
        AccessLog: [], server: "kino", daemonize: false, log_stdout: true
      )

      expect(options).to include(port: 3000)
    end

    it "defaults the port to 9292 when nothing chose one" do
      expect(handler.server_options({})).to include(port: 9292)
    end

    it "lets the config file beat a host-provided default" do
      File.write("kino.rb", "port 4000\n")

      options = handler.server_options(Port: 3000, user_supplied_options: [])

      expect(options).to include(port: 4000)
    end

    it "lets a user-supplied option beat the config file" do
      File.write("kino.rb", "port 4000\n")

      options = handler.server_options(Port: 3000, user_supplied_options: [:Port])

      expect(options).to include(port: 3000)
    end

    it "treats every option as user-supplied when the host sends no list" do
      File.write("kino.rb", "port 4000\n")

      options = handler.server_options(Port: "3000")

      expect(options).to include(port: 3000)
    end

    it "keeps a host default the config file left unset" do
      File.write("kino.rb", "threads 1\n")

      options = handler.server_options(Host: "0.0.0.0", user_supplied_options: [])

      expect(options).to include(bind: "0.0.0.0", threads: 1)
    end

    it "loads the file named by Config" do
      File.write("custom.rb", "workers 2\n")

      options = handler.server_options(Config: "custom.rb")

      expect(options).to include(workers: 2)
    end

    it "falls back to config/kino.rb when there is no kino.rb" do
      Dir.mkdir("config")
      File.write("config/kino.rb", "workers 3\n")

      expect(handler.server_options({})).to include(workers: 3)
    end
  end

  describe ".run" do
    let(:app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["hi"]] } }

    it "yields the built server to the host before serving" do
      yielded = nil

      expect {
        handler.run(app, Host: "127.0.0.1", Port: 0, Mode: "threaded") do |server|
          yielded = server
          throw :stop
        end
      }.to throw_symbol(:stop)

      expect(yielded).to be_a(Kino::Server)
    end

    it "serves an app through the real rackup executable" do
      Dir.mktmpdir("kino-rackup") do |dir|
        File.write(File.join(dir, "config.ru"), <<~'RU')
          run ->(env) { [200, {"content-type" => "text/plain"}, ["from rackup #{env["PATH_INFO"]}"]] }
        RU
        out = File.join(dir, "stdout.log")
        rackup = Gem.bin_path("rackup", "rackup")
        pid = spawn(Gem.ruby, rackup, "-s", "kino", "-o", "127.0.0.1", "-p", "0", "-O", "Threads=1",
          chdir: dir, out: out, err: out)
        port = nil
        100.times do
          port = File.read(out)[%r{listening: https?://[^:]+:(\d+)}, 1]&.to_i
          break if port
          sleep 0.05
        end
        raise "rackup never reported a listening port:\n#{File.read(out)}" unless port

        response = Net::HTTP.get_response("127.0.0.1", "/hello", port)
        expect(response.code).to eq("200")
        expect(response.body).to eq("from rackup /hello")

        Process.kill("TERM", pid)
        _, status = Process.wait2(pid)
        expect(status.exitstatus).to eq(0)
      ensure
        Process.kill("KILL", pid) if pid && !status
      end
    end
  end
end
