# frozen_string_literal: true

RSpec.describe "default worker count" do
  let(:shareable_app) { Ractor.shareable_proc { |_env| [200, {"content-type" => "text/plain"}, ["ok"]] } }
  let(:plain_app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

  before { allow(Kino).to receive(:available_parallelism).and_return(10) }

  def with_max_cpu(value)
    previous = ENV["RUBY_MAX_CPU"]
    value.nil? ? ENV.delete("RUBY_MAX_CPU") : ENV["RUBY_MAX_CPU"] = value
    yield
  ensure
    previous.nil? ? ENV.delete("RUBY_MAX_CPU") : ENV["RUBY_MAX_CPU"] = previous
  end

  def workers_for(app, **opts)
    Kino::Server.new(app, threads: 1, **opts).stats[:workers]
  end

  it "caps the ractor default at 8 when RUBY_MAX_CPU is unset" do
    with_max_cpu(nil) { expect(workers_for(shareable_app, mode: :ractor)).to eq(8) }
  end

  it "leaves the threaded default at the CPU count" do
    with_max_cpu(nil) { expect(workers_for(shareable_app, mode: :threaded)).to eq(10) }
  end

  it "follows the resolved mode under :auto" do
    with_max_cpu(nil) do
      expect(workers_for(shareable_app, mode: :auto)).to eq(8)
      capture_native_stderr { expect(workers_for(plain_app, mode: :auto)).to eq(10) }
    end
  end

  it "honors a higher RUBY_MAX_CPU up to the CPU count" do
    with_max_cpu("12") { expect(workers_for(shareable_app, mode: :ractor)).to eq(10) }
  end

  it "honors a lower RUBY_MAX_CPU" do
    with_max_cpu("4") { expect(workers_for(shareable_app, mode: :ractor)).to eq(4) }
  end

  it "treats an unparseable RUBY_MAX_CPU as 8" do
    with_max_cpu("garbage") { expect(workers_for(shareable_app, mode: :ractor)).to eq(8) }
  end

  it "keeps an explicit workers value and still warns past the cap" do
    with_max_cpu(nil) do
      server = nil
      err = capture_native_stderr do
        server = Kino::Server.new(shareable_app, mode: :ractor, threads: 1, workers: 10).start
        server.shutdown(timeout: 0.2)
      end
      expect(server.stats[:workers]).to eq(10)
      expect(err).to include("RUBY_MAX_CPU")
    end
  end

  it "does not warn on a default boot" do
    with_max_cpu(nil) do
      err = capture_native_stderr do
        Kino::Server.new(shareable_app, mode: :ractor, threads: 1).start.shutdown(timeout: 0.2)
      end
      expect(err).not_to include("RUBY_MAX_CPU")
    end
  end
end
