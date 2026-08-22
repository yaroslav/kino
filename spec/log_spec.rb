# frozen_string_literal: true

RSpec.describe Kino::Log do
  let(:pid) { Process.pid }

  it "labels lines with the pid and the source, syslog-style" do
    expect(described_class.label).to eq("kino[#{pid}] main:")
  end

  describe ".source" do
    it "is main on the main ractor's main thread" do
      expect(described_class.source).to eq("main")
    end

    it "is the thread's name on a named thread" do
      source = Thread.new {
        Thread.current.name = "worker-7"
        described_class.source
      }.value

      expect(source).to eq("worker-7")
    end

    it "is the ractor's name inside a named ractor" do
      expect(Ractor.new(name: "worker-2") { Kino::Log.source }.value).to eq("worker-2")
    end

    it "joins the ractor and thread names when both are set" do
      source = Ractor.new(name: "worker-2") {
        Thread.new {
          Thread.current.name = "thread-1"
          Kino::Log.source
        }.value
      }.value

      expect(source).to eq("worker-2/thread-1")
    end
  end

  describe "levels" do
    it "writes notes to stdout, label first" do
      out = capture_native_stdout { described_class.info("hello there") }

      expect(out).to eq("kino[#{pid}] main: hello there\n")
    end

    it "writes warnings to stderr" do
      err = capture_native_stderr { described_class.warn("careful") }

      expect(err).to eq("kino[#{pid}] main: careful\n")
    end

    it "writes errors to stderr" do
      err = capture_native_stderr { described_class.error("it broke") }

      expect(err).to eq("kino[#{pid}] main: it broke\n")
    end
  end

  describe ".exception" do
    it "reports the request, the error, and an app-first trace relative to the working directory" do
      error = begin
        raise "kaput"
      rescue => e
        e
      end
      env = {"REQUEST_METHOD" => "GET", "PATH_INFO" => "/boom"}

      err = capture_native_stderr { described_class.exception(error, env) }

      expect(err).to start_with("kino[#{pid}] main: 500 GET /boom · RuntimeError: kaput (spec/log_spec.rb:")
      expect(err).to match(%r{^    spec/log_spec\.rb:\d+:in }) # the app's own frame, first and relativized
      expect(err).to match(/^    … \d+ more$/) # the rspec frames below it, folded
    end
  end

  describe "workers" do
    let(:app) { ->(_env) { [200, {"content-type" => "text/plain"}, ["ok"]] } }

    it "run on named threads, so a line says which worker spoke" do
      seen = Queue.new
      hook = ->(_id) { seen << Kino::Log.source }

      with_server(app, after_worker_boot: hook) { |_host, _port, _server| }

      expect(seen.pop(timeout: 5)).to match(/\Aworker-\d+\z/)
    end

    it "run in named ractors in :ractor mode" do
      boom = Ractor.shareable_proc { |_env| raise "kaput in a ractor" }

      err = capture_native_stderr do
        with_server(boom, mode: :ractor, workers: 1, threads: 1) do |host, port|
          Net::HTTP.get_response(host, "/", port)
        end
      end

      expect(err).to match(/kino\[#{pid}\] worker-0: 500 GET \/ · RuntimeError: kaput in a ractor/)
    end
  end
end
