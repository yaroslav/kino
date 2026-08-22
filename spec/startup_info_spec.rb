# frozen_string_literal: true

require "tmpdir"

RSpec.describe "startup info" do
  it "prints the runtime, environment, topology, pid, and address under the banner" do
    app = "run ->(_env) { [200, {}, []] }\n"

    Dir.mktmpdir("kino-startup") do |dir|
      with_cli_server(dir, "workers 1\nthreads 1\nmode :threaded\n", app) do |port, out|
        banner = File.read(out)

        expect(banner).to include("- ruby:      #{RUBY_DESCRIPTION}")
        expect(banner).to match(/- env:       \w+/)
        expect(banner).to include("- mode:      threaded, 1 worker × 1 thread")
        expect(banner).to match(/- pid:       \d+/)
        expect(banner).to include("- listening: http://127.0.0.1:#{port}")
      end
    end
  end

  it "names the control plane when one is bound" do
    app = "run ->(_env) { [200, {}, []] }\n"

    Dir.mktmpdir("kino-startup") do |dir|
      config = "workers 1\nthreads 1\nmode :threaded\ncontrol_bind \"127.0.0.1:0\"\n"
      with_cli_server(dir, config, app) do |_port, out|
        expect(File.read(out)).to match(%r{- control:   http://127\.0\.0\.1:\d+})
      end
    end
  end
end
