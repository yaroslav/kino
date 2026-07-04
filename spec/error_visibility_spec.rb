# frozen_string_literal: true

require "tempfile"

# Errors caught by the worker rescue must be visible to the app's
# monitoring (on_error hook, backtrace in the log), and spec-violating
# header values must be coerced like Puma coerces them, not turned into
# opaque 500s.
RSpec.describe "error visibility" do
  # An enumerable body without to_ary forces the streaming delivery path.
  let(:each_only_body) do
    Class.new do
      def initialize(*chunks) = @chunks = chunks

      def each(&) = @chunks.each(&)
    end
  end

  describe "non-String response header values" do
    it "coerces scalar values with to_s, like Puma" do
      app = lambda do |_env|
        [200, {"content-type" => "text/plain", "x-bool" => true, "x-count" => 42}, ["ok"]]
      end

      with_server(app) do |host, port|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.code).to eq("200")
        expect(response["x-bool"]).to eq("true")
        expect(response["x-count"]).to eq("42")
      end
    end

    it "coerces entries of Array header values" do
      app = ->(_env) { [200, {"x-multi" => [1, :two]}, ["ok"]] }

      with_server(app) do |host, port|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.code).to eq("200")
        expect(response.get_fields("x-multi")).to eq(%w[1 two])
      end
    end

    it "coerces non-String header names" do
      app = ->(_env) { [200, {"x-sym": "value"}, ["ok"]] }

      with_server(app) do |host, port|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.code).to eq("200")
        expect(response["x-sym"]).to eq("value")
      end
    end

    it "coerces on the streaming delivery path too" do
      app = ->(_env) { [200, {"x-bool" => false}, each_only_body.new("streamed")] }

      with_server(app) do |host, port|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.code).to eq("200")
        expect(response["x-bool"]).to eq("false")
        expect(response.body).to eq("streamed")
      end
    end
  end

  describe "on_error hook" do
    it "receives the exception and the rack env; the client still gets a 500" do
      seen = Queue.new
      boom = ->(_env) { raise "kaput" }
      hook = ->(error, env) { seen << [error, env] }

      with_server(boom, on_error: hook) do |host, port|
        response = Net::HTTP.get_response(host, "/failing/path", port)
        expect(response.code).to eq("500")

        error, env = seen.pop(timeout: 5)
        expect(error).to be_a(RuntimeError)
        expect(error.message).to eq("kaput")
        expect(env["PATH_INFO"]).to eq("/failing/path")
      end
    end

    it "fires for delivery failures after the app returned" do
      seen = Queue.new
      # to_str (not to_s) is unimplementable-by-design here: a value the
      # native layer cannot coerce, so delivery itself fails.
      uncoercible = BasicObject.new
      app = ->(_env) { [200, {"x-bad" => uncoercible}, ["ok"]] }
      hook = ->(error, _env) { seen << error }

      with_server(app, on_error: hook) do |host, port|
        expect(Net::HTTP.get_response(host, "/", port).code).to eq("500")
        expect(seen.pop(timeout: 5)).to be_a(StandardError)
      end
    end

    it "fires for mid-stream body failures, which abort the connection visibly" do
      seen = Queue.new
      exploding = Object.new
      def exploding.each
        yield "first"
        raise "boom mid-stream"
      end
      app = ->(_env) { [200, {"content-type" => "text/plain"}, exploding] }
      hook = ->(error, _env) { seen << error }

      with_server(app, on_error: hook) do |host, port|
        outcome = begin
          Net::HTTP.get_response(host, "/", port)
        rescue IOError, SystemCallError, Net::HTTPBadResponse => e
          e
        end

        expect(seen.pop(timeout: 5)&.message).to eq("boom mid-stream")
        # A body that died must not read as a complete response: the
        # truncation has to be visible on the client side.
        expect(outcome).to be_a(Exception),
          "expected a broken stream, got #{outcome.inspect}"
      end
    end

    it "keeps serving when the hook itself raises" do
      flaky = lambda do |env|
        raise "kaput" if env["PATH_INFO"] == "/boom"

        [200, {"content-type" => "text/plain"}, ["still alive"]]
      end
      bad_hook = ->(_error, _env) { raise "hook exploded" }

      with_server(flaky, on_error: bad_hook) do |host, port|
        expect(Net::HTTP.get_response(host, "/boom", port).code).to eq("500")
        expect(Net::HTTP.get_response(host, "/ok", port).body).to eq("still alive")
      end
    end

    it "rejects a non-callable on_error at construction" do
      app = ->(_env) { [200, {}, ["ok"]] }

      expect { Kino::Server.new(app, on_error: "not callable") }
        .to raise_error(ArgumentError, /on_error/)
    end
  end

  describe "worker error log" do
    it "includes the exception backtrace" do
      boom = ->(_env) { raise "kaput with backtrace" }

      log = capture_native_stderr do
        with_server(boom) do |host, port|
          Net::HTTP.get_response(host, "/", port)
        end
      end

      expect(log).to include("RuntimeError: kaput with backtrace")
      # The frame that raised must be nameable from the log line.
      expect(log).to match(/error_visibility_spec\.rb:\d+/)
    end
  end

  # Native log lines go to fd 2 via Rust eprintln!, invisible to Ruby's
  # $stderr object: capture by swapping the file descriptor itself.
  def capture_native_stderr
    original = $stderr.dup
    Tempfile.create("kino-stderr") do |captured|
      $stderr.reopen(captured)
      yield
      $stderr.flush
      captured.rewind
      captured.read
    ensure
      $stderr.reopen(original)
    end
  end
end
