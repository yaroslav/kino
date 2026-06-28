# frozen_string_literal: true

require "tmpdir"

RSpec.describe Kino::CLI do
  # color? gates on ENV; isolate any changes per example.
  around do |example|
    saved = ENV.to_h.slice("NO_COLOR", "TERM")
    example.run
  ensure
    ENV.delete("NO_COLOR")
    ENV.delete("TERM")
    saved.each { |key, value| ENV[key] = value }
  end

  let(:tty) { double("tty", tty?: true) }
  let(:not_tty) { double("not_tty", tty?: false) }

  describe ".color?" do
    it "is true for a tty with a normal TERM and no NO_COLOR" do
      ENV.delete("NO_COLOR")
      ENV["TERM"] = "xterm-256color"

      expect(described_class.color?(tty)).to be(true)
    end

    it "is false when the stream is not a tty" do
      ENV.delete("NO_COLOR")
      ENV["TERM"] = "xterm-256color"

      expect(described_class.color?(not_tty)).to be(false)
    end

    it "is false when NO_COLOR is set" do
      ENV["NO_COLOR"] = "1"
      ENV["TERM"] = "xterm-256color"

      expect(described_class.color?(tty)).to be(false)
    end

    it "is false for a dumb terminal" do
      ENV.delete("NO_COLOR")
      ENV["TERM"] = "dumb"

      expect(described_class.color?(tty)).to be(false)
    end
  end

  describe "config resolution" do
    around do |example|
      Dir.mktmpdir("kino-cli") do |dir|
        @dir = dir
        example.run
      end
    end

    def config_path(content)
      path = File.join(@dir, "kino.rb")
      File.write(path, content)
      path
    end

    it "defaults the port to 9292 when neither file nor flag chose one" do
      path = config_path("threads 1\n")

      config = described_class.send(:resolve_config, config_file: path, overrides: {})

      expect(config[:port]).to eq(9292)
    end

    it "keeps a port from the config file" do
      path = config_path("port 4000\n")

      config = described_class.send(:resolve_config, config_file: path, overrides: {})

      expect(config[:port]).to eq(4000)
    end

    it "lets a CLI override beat the config file" do
      path = config_path("port 4000\n")

      config = described_class.send(:resolve_config, config_file: path, overrides: {port: 3000})

      expect(config[:port]).to eq(3000)
    end
  end

  describe "--check via the kino executable" do
    def exe_path
      File.expand_path("../exe/kino", __dir__)
    end

    # Run `kino ARGV` in +dir+, returning [combined_output, Process::Status].
    def run_kino(*argv, dir:)
      out = IO.popen([Gem.ruby, exe_path, *argv], chdir: dir, err: [:child, :out], &:read)
      [out, $?]
    end

    around do |example|
      Dir.mktmpdir("kino-exe") do |dir|
        @dir = dir
        example.run
      end
    end

    it "reports a Ractor-shareable app" do
      File.write(File.join(@dir, "config.ru"),
        "run Ractor.shareable_proc { |_env| [200, {}, []] }\n")

      out, status = run_kino("--check", "config.ru", dir: @dir)

      expect(status).to be_success
      expect(out).to include("Ractor-shareable")
    end

    it "reports a non-shareable app" do
      File.write(File.join(@dir, "config.ru"),
        "run ->(_env) { [200, {}, []] }\n")

      out, status = run_kino("--check", "config.ru", dir: @dir)

      expect(status).not_to be_success
      expect(out).to include("NOT Ractor-shareable")
    end
  end
end
