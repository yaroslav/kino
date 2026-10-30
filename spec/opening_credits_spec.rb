# frozen_string_literal: true

require "tmpdir"

RSpec.describe "opening credits" do
  let(:rackup) do
    <<~RU
      $stdout.puts "OUT-MARKER"
      $stderr.puts "ERR-MARKER"
      run ->(_env) { [200, {}, []] }
    RU
  end

  # Run the CLI with stdout and stderr in separate files; returns both
  # once "Action!" and the replayed markers have been written.
  def serve_split(dir, rackup_body)
    File.write(File.join(dir, "config.ru"), rackup_body)
    File.write(File.join(dir, "kino.rb"), "port 0\nworkers 1\nthreads 1\nmode :threaded\n")
    out = File.join(dir, "out.log")
    err = File.join(dir, "err.log")
    exe = File.expand_path("../exe/kino", __dir__)
    pid = spawn(Gem.ruby, exe, "-C", File.join(dir, "kino.rb"), File.join(dir, "config.ru"),
      chdir: dir, out: out, err: err)
    300.times do
      break if File.read(out).include?("OUT-MARKER") && File.read(err).include?("ERR-MARKER")
      sleep 0.05
    end
    yield File.read(out), File.read(err)
  ensure
    if pid
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
  end

  it "prints the credits, then the ready block, then the app's boot output" do
    Dir.mktmpdir("kino-credits") do |dir|
      serve_split(dir, rackup) do |out, err|
        expect(out.scan("presents:").size).to eq(1)
        expect(out.index("presents:")).to be < out.index("- ruby:")
        expect(out.index("Action!")).to be < out.index("OUT-MARKER")
        expect(out.scan("OUT-MARKER").size).to eq(1)
        expect(out).not_to include("ERR-MARKER")
        expect(err).to include("ERR-MARKER")
        expect(err).not_to include("OUT-MARKER")
      end
    end
  end

  it "shows captured output and the error when the app fails to load" do
    Dir.mktmpdir("kino-credits") do |dir|
      File.write(File.join(dir, "config.ru"), "$stdout.puts 'OUT-MARKER'\nraise 'boom'\n")
      exe = File.expand_path("../exe/kino", __dir__)
      out = IO.popen([Gem.ruby, exe, "config.ru"], chdir: dir, err: [:child, :out], &:read)

      expect($?).not_to be_success
      expect(out.index("presents:")).to be < out.index("OUT-MARKER")
      expect(out.index("OUT-MARKER")).to be < out.index("boom")
      expect(out).not_to include("Action!")
    end
  end

  it "survives a Rails.logger whose flush raises" do
    fake = <<~RU
      module Rails
        def self.logger = Object.new.tap { |l| l.define_singleton_method(:flush) { raise "flushboom" } }
      end
    RU
    Dir.mktmpdir("kino-credits") do |dir|
      serve_split(dir, fake + rackup) do |out, err|
        expect(out.index("Action!")).to be < out.index("OUT-MARKER")
        expect(err).to include("ERR-MARKER")
        expect(err).to include("flushboom")
      end
    end
  end

  it "still shows output and the error when the load fails and flush raises" do
    Dir.mktmpdir("kino-credits") do |dir|
      body = <<~RU
        module Rails
          def self.logger = Object.new.tap { |l| l.define_singleton_method(:flush) { raise "flushboom" } }
        end
        $stdout.puts "OUT-MARKER"
        raise "boom"
      RU
      File.write(File.join(dir, "config.ru"), body)
      exe = File.expand_path("../exe/kino", __dir__)
      out = IO.popen([Gem.ruby, exe, "config.ru"], chdir: dir, err: [:child, :out], &:read)

      expect($?).not_to be_success
      expect(out.index("OUT-MARKER")).to be < out.index("boom (RuntimeError)")
    end
  end

  it "restores the original streams even when the app reassigns $stderr" do
    Dir.mktmpdir("kino-credits") do |dir|
      body = "$stderr = $stdout\n$stdout.puts 'OUT-MARKER'\nrun ->(_env) { [200, {}, []] }\n"
      serve_split(dir, body) do |out, err|
        expect(out.index("Action!")).to be < out.index("OUT-MARKER")
        expect(err).not_to include("OUT-MARKER")
      end
    end
  end

  it "shows captured output when the server fails to start" do
    Dir.mktmpdir("kino-credits") do |dir|
      holder = TCPServer.new("127.0.0.1", 0)
      port = holder.addr[1]
      File.write(File.join(dir, "config.ru"), rackup)
      exe = File.expand_path("../exe/kino", __dir__)
      out = IO.popen([Gem.ruby, exe, "-b", "127.0.0.1", "-p", port.to_s, "-m", "threaded", "config.ru"],
        chdir: dir, err: [:child, :out], &:read)

      expect($?).not_to be_success
      expect(out).to include("OUT-MARKER")
      expect(out).not_to include("Action!")
    ensure
      holder&.close
    end
  end

  it "leaves --check output free of credits and capture" do
    Dir.mktmpdir("kino-credits") do |dir|
      File.write(File.join(dir, "config.ru"), rackup)
      exe = File.expand_path("../exe/kino", __dir__)
      out = IO.popen([Gem.ruby, exe, "--check", "config.ru"], chdir: dir, err: [:child, :out], &:read)

      expect(out).to include("OUT-MARKER")
      expect(out).not_to include("presents:")
    end
  end

  it "still prints both halves when Server.run is called directly" do
    Dir.mktmpdir("kino-credits") do |dir|
      lib = File.expand_path("../lib", __dir__)
      script = <<~RUBY
        require "kino"
        Kino::Server.run(->(_env) { [200, {}, []] }, port: 0, workers: 1, threads: 1, mode: :threaded)
      RUBY
      out = +""
      IO.popen([Gem.ruby, "-I", lib, "-e", script], chdir: dir, err: [:child, :out]) do |io|
        until out.include?("Action!")
          line = io.gets or break
          out << line
        end
        Process.kill("TERM", io.pid)
      end

      expect(out.scan("presents:").size).to eq(1)
      expect(out.index("presents:")).to be < out.index("- ruby:")
    end
  end
end
