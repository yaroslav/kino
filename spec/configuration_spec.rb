# frozen_string_literal: true

require "tmpdir"

RSpec.describe Kino::Configuration do
  def write_config(content)
    path = File.join(@dir, "kino.rb")
    File.write(path, content)
    path
  end

  around do |example|
    Dir.mktmpdir("kino-config") do |dir|
      @dir = dir
      example.run
    end
  end

  it "starts from defaults" do
    config = described_class.new

    expect(config[:bind]).to eq("127.0.0.1")
    expect(config[:port]).to eq(0)
    expect(config[:threads]).to be_nil # resolved per mode in Server
    expect(config[:mode]).to eq(:auto)
  end

  describe "mode-dependent threads default" do
    it "defaults to 1 thread per worker in :ractor mode" do
      app = Ractor.shareable_proc { |_env| [200, {}, []] }
      server = Kino::Server.new(app, mode: :ractor)

      expect(server.stats[:threads]).to eq(1)
    end

    it "defaults to 3 threads per worker in :threaded mode" do
      server = Kino::Server.new(->(_env) { [200, {}, []] }, mode: :threaded)

      expect(server.stats[:threads]).to eq(3)
    end

    it "lets an explicit threads setting win in any mode" do
      app = Ractor.shareable_proc { |_env| [200, {}, []] }
      server = Kino::Server.new(app, mode: :ractor, threads: 4)

      expect(server.stats[:threads]).to eq(4)
    end
  end

  it "loads a Puma-style DSL file" do
    path = write_config(<<~CONFIG)
      bind "0.0.0.0"
      port 9292
      workers 4
      threads 2
      mode :ractor
      queue_depth 2048
      queue_timeout 0.5
      shutdown_timeout 15
      tokio_threads 4
      tls cert: "cert.pem", key: "key.pem"
    CONFIG

    config = described_class.new.load_file(path)

    expect(config[:bind]).to eq("0.0.0.0")
    expect(config[:port]).to eq(9292)
    expect(config[:workers]).to eq(4)
    expect(config[:threads]).to eq(2)
    expect(config[:mode]).to eq(:ractor)
    expect(config[:queue_depth]).to eq(2048)
    expect(config[:queue_timeout]).to eq(0.5)
    expect(config[:shutdown_timeout]).to eq(15)
    expect(config[:tokio_threads]).to eq(4)
    expect(config[:tls]).to eq(cert: "cert.pem", key: "key.pem")
  end

  it "lets explicit options win over the config file" do
    path = write_config("port 9292\nthreads 2\n")

    config = described_class.new.load_file(path).merge!(port: 3000)

    expect(config[:port]).to eq(3000) # kwarg beat the file
    expect(config[:threads]).to eq(2) # file value kept
  end

  it "parses environment, pidfile, and rackup directives" do
    path = write_config(<<~CONFIG)
      environment :production
      pidfile "tmp/kino.pid"
      rackup "app/config.ru"
    CONFIG

    config = described_class.new.load_file(path)

    expect(config[:environment]).to eq("production")
    expect(config[:pidfile]).to eq("tmp/kino.pid")
    expect(config[:rackup]).to eq("app/config.ru")
  end

  it "writes the pidfile on start and removes it on shutdown" do
    pidfile = File.join(@dir, "kino.pid")
    app = ->(_env) { [200, {}, []] }

    server = Kino::Server.new(app, workers: 1, threads: 1, mode: :threaded, pidfile: pidfile)
    server.start
    expect(File.read(pidfile).strip).to eq(Process.pid.to_s)

    server.shutdown
    expect(File.exist?(pidfile)).to be(false)
  end

  describe "pidfile safety" do
    let(:app) { ->(_env) { [200, {}, []] } }
    let(:pidfile) { File.join(@dir, "kino.pid") }

    def new_server
      Kino::Server.new(app, workers: 1, threads: 1, mode: :threaded, pidfile: pidfile)
    end

    def dead_pid
      pid = Process.spawn("true")
      Process.wait(pid)
      pid
    end

    it "refuses to start while the pidfile's owner is alive" do
      File.write(pidfile, "#{Process.pid}\n") # our own pid: guaranteed alive

      expect { new_server.start }
        .to raise_error(Kino::Error, /already running \(pid #{Process.pid}/)
      expect(File.read(pidfile)).to eq("#{Process.pid}\n")
    end

    it "replaces a pidfile left behind by a dead process" do
      File.write(pidfile, "#{dead_pid}\n")

      server = new_server.start
      expect(File.read(pidfile).strip).to eq(Process.pid.to_s)
      server.shutdown
    end

    it "refuses to overwrite a file that does not hold a pid" do
      File.write(pidfile, "precious data\n")

      expect { new_server.start }
        .to raise_error(Kino::Error, /does not hold a pid/)
      expect(File.read(pidfile)).to eq("precious data\n")
    end

    it "replaces a stale pidfile symlink without touching its target" do
      target = File.join(@dir, "target.pid")
      stale = "#{dead_pid}\n"
      File.write(target, stale)
      File.symlink(target, pidfile)

      server = new_server.start
      expect(File.symlink?(pidfile)).to be(false) # the link itself was replaced
      expect(File.read(target)).to eq(stale) # its target was not written through
      server.shutdown
    end

    it "leaves the pidfile alone on shutdown once it is no longer ours" do
      server = new_server.start
      File.write(pidfile, "424242\n") # a successor took the path over

      server.shutdown
      expect(File.read(pidfile)).to eq("424242\n")
    end
  end

  it "raises on an unknown setting" do
    expect { described_class.new.set(:nope, 1) }.to raise_error(ArgumentError, /unknown setting/)
  end

  it "raises on a missing config file" do
    expect { described_class.new.load_file("/nonexistent/kino.rb") }
      .to raise_error(Kino::Error, /not found/)
  end

  it "configures a server through Server.new" do
    path = write_config("threads 1\nmode :threaded\nqueue_depth 7\n")
    app = ->(_env) { [200, {}, []] }

    server = Kino::Server.new(app, config_file: path, workers: 1)

    expect(server.mode).to eq(:threaded)
    server.start
    expect(server.port).to be > 0
  ensure
    server&.shutdown
  end

  describe "sample config generator" do
    it "writes a sample config that loads cleanly with default values" do
      path = File.join(@dir, "generated.rb")
      Kino::Configuration.write_sample(path)

      config = described_class.new.load_file(path)

      # Anything active in the sample must state the built-in default, so
      # loading it changes no effective setting.
      described_class::SETTINGS.each do |key|
        expect(config[key]).to eq(described_class::DEFAULTS[key]),
          "sample changed #{key} away from its default"
      end
    end

    it "documents every DSL directive" do
      sample = Kino::Configuration.sample
      dsl_methods = Kino::Configuration::DSL.public_instance_methods(false)

      dsl_methods.each do |directive|
        expect(sample).to match(/^(?:# )?#{directive}\b/),
          "sample config must show a `#{directive}` example (active or commented)"
      end
      expect(sample).to include("Rails")
    end

    it "refuses to overwrite an existing file unless forced" do
      path = File.join(@dir, "kino.rb")
      File.write(path, "port 1\n")

      expect { Kino::Configuration.write_sample(path) }
        .to raise_error(Kino::Error, /already exists/)
      expect(File.read(path)).to eq("port 1\n")

      Kino::Configuration.write_sample(path, force: true)
      expect(File.read(path)).to include("Kino configuration")
    end

    it "generates via the CLI --init flag" do
      exe = File.expand_path("../exe/kino", __dir__)
      out = IO.popen([Gem.ruby, exe, "--init", "cli-kino.rb"], chdir: @dir, &:read)

      expect($?).to be_success, "kino --init failed: #{out}"
      expect(File.read(File.join(@dir, "cli-kino.rb"))).to include("## Rails")

      # Second run must refuse to clobber.
      IO.popen([Gem.ruby, exe, "--init", "cli-kino.rb"], chdir: @dir, err: [:child, :out], &:read)
      expect($?).not_to be_success
    end
  end

  it "boots via the kino CLI with a config file" do
    app = 'run ->(_env) { [200, {"content-type" => "text/plain"}, ["from cli"]] }'

    with_cli_server(@dir, "workers 1\nthreads 1\nmode :threaded\n", app) do |port, _out|
      response = Net::HTTP.get_response("127.0.0.1", "/", port)

      expect(response.body).to eq("from cli")
      expect(response["server"]).to eq("Kino")
    end
  end
end
