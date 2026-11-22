# frozen_string_literal: true

require "rack"

# Rack 3 spec compliance, enforced by Rack::Lint on a real socket.
# Every app here is wrapped in lint: if kino mis-builds the env or mishandles
# a body shape, lint raises and the spec fails with a 500.
RSpec.describe "Rack 3 compliance" do
  def lint(app)
    Rack::Lint.new(app)
  end

  it "passes lint for a basic GET" do
    app = lint(->(_env) { [200, {"content-type" => "text/plain"}, ["lint ok"]] })

    with_server(app) do |host, port|
      response = Net::HTTP.get_response(host, "/some/path?q=1", port)

      expect(response.code).to eq("200")
      expect(response.body).to eq("lint ok")
    end
  end

  it "passes lint for a POST with body readback through rack.input" do
    app = lint(lambda do |env|
      body = env["rack.input"].read
      [200, {"content-type" => "application/octet-stream"}, [body]]
    end)

    with_server(app) do |host, port|
      response = Net::HTTP.new(host, port).post("/u", "x" * 100_000)

      expect(response.body.bytesize).to eq(100_000)
    end
  end

  it "supports partial reads with IO#read semantics" do
    app = lint(lambda do |env|
      input = env["rack.input"]
      first = input.read(5)
      rest = input.read
      tail = input.read(1)
      [200, {"content-type" => "text/plain"},
        ["first=#{first} rest=#{rest.bytesize} tail=#{tail.inspect}"]]
    end)

    with_server(app) do |host, port|
      response = Net::HTTP.new(host, port).post("/u", "hello#{"y" * 1000}")

      expect(response.body).to eq("first=hello rest=1000 tail=nil")
    end
  end

  it "round-trips a 1 MB body each way" do
    payload = Random.bytes(1_048_576)
    app = lint(lambda do |env|
      [200, {"content-type" => "application/octet-stream"}, [env["rack.input"].read]]
    end)

    with_server(app) do |host, port|
      http = Net::HTTP.new(host, port)
      response = http.post("/mirror", payload, {"content-type" => "application/octet-stream"})

      expect(response.body.b).to eq(payload)
    end
  end

  it "serves HEAD requests" do
    app = lint(lambda do |_env|
      [200, {"content-type" => "text/plain", "content-length" => "5"}, ["hello"]]
    end)

    with_server(app) do |host, port|
      response = Net::HTTP.new(host, port).head("/")

      expect(response.code).to eq("200")
      expect(response["content-length"]).to eq("5")
      expect(response.body).to be_nil
    end
  end

  it "serves 204 No Content without a body" do
    app = lint(->(_env) { [204, {}, []] })

    with_server(app) do |host, port|
      response = Net::HTTP.get_response(host, "/", port)

      expect(response.code).to eq("204")
      expect(response.body).to be_nil
    end
  end

  it "supports multi-value headers via arrays" do
    app = lint(lambda do |_env|
      [200, {"content-type" => "text/plain", "set-cookie" => ["a=1", "b=2"]}, ["ok"]]
    end)

    with_server(app) do |host, port|
      response = Net::HTTP.get_response(host, "/", port)

      expect(response.get_fields("set-cookie")).to eq(["a=1", "b=2"])
    end
  end

  describe "streaming" do
    it "delivers enumerable body chunks as they are produced" do
      app = lint(lambda do |_env|
        chunks = Enumerator.new do |yielder|
          yielder << "first"
          sleep 0.4
          yielder << "second"
        end
        [200, {"content-type" => "text/plain"}, chunks]
      end)

      with_server(app) do |host, port|
        arrivals = []
        started = nil
        Net::HTTP.start(host, port) do |http|
          http.request(Net::HTTP::Get.new("/")) do |response|
            started = monotonic
            response.read_body do |chunk|
              arrivals << [chunk, monotonic - started]
            end
          end
        end

        expect(arrivals.map(&:first).join).to eq("firstsecond")
        expect(arrivals.first[0]).to eq("first")
        expect(arrivals.first[1]).to be < 0.3, "first chunk should arrive before the app sleeps"
      end
    end

    it "supports Rack 3 callable streaming bodies" do
      app = lint(lambda do |_env|
        body = proc do |stream|
          stream.write("streamed ")
          stream.write("by proc")
          stream.close
        end
        [200, {"content-type" => "text/plain"}, body]
      end)

      with_server(app) do |host, port|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.body).to eq("streamed by proc")
      end
    end
  end

  describe "a Rack 3 streaming body that responds to close" do
    each_mode do |mode|
      it "is closed once its call returned or raised, while a partial hijack's callable is left to the app", :quiet do
        streams = Ractor.make_shareable(ReportingStreamingBody.new(reports.port, false))
        raises = Ractor.make_shareable(ReportingStreamingBody.new(reports.port, true))
        hijack = Ractor.make_shareable(ReportingHijack.new(reports.port))
        ignored = Ractor.make_shareable(ReportingIgnoredBody.new(reports.port))
        app = Ractor.shareable_proc do |env|
          case env["PATH_INFO"]
          when "/raise" then [200, {}, raises]
          when "/partial" then [200, {"content-length" => "8", "rack.hijack" => hijack}, ignored]
          else [200, {}, streams]
          end
        end
        # rack.hijack is offered on a request that asks to upgrade.
        get = lambda do |port, path, connection = "Connection: close"|
          raw_exchange(port, "GET #{path} HTTP/1.1\r\nHost: kino\r\n#{connection}\r\n\r\n")
        end

        with_server(app, mode: mode, workers: 1, threads: 1) do |_host, port|
          expect(get.call(port, "/")).to start_with("HTTP/1.1 200").and(include("streamed"))
          expect(reports.next(timeout: 2, awaiting: "close of the streaming body")).to eq(:body_closed)

          expect(get.call(port, "/raise")).to include("streamed")
          expect(reports.next(timeout: 2, awaiting: "close of the body that raised")).to eq(:body_closed)

          expect(get.call(port, "/partial", "Upgrade: chat\r\nConnection: Upgrade")).to include("hijacked")
          expect(reports.next(timeout: 2, awaiting: "close of the ignored body")).to eq(:ignored_body_closed)
        end

        expect(reports).to be_empty, "a body closed twice, or the server closed the hijack callable"
      end
    end
  end

  describe "a stream closed while another thread writes to it" do
    # /stream's body writes 12.5 MiB from a thread of its own, which
    # blocks on a client that reads nothing, then closes the stream from
    # the request's thread; each step reports to `port`.
    def closing_under_a_writer(port)
      Ractor.shareable_proc do |env|
        next [200, {}, ["ok"]] unless env["PATH_INFO"] == "/stream"

        body = lambda do |stream|
          writer = Thread.new do
            200.times { stream.write("x" * 65_536) }
            port << :written
          rescue IOError => e
            port << e.class.name
          end
          sleep 0.3
          stream.close
          port << :closed
          writer.join
        end
        [200, {}, body]
      end
    end

    each_mode do |mode|
      it "fails the blocked writer once its client leaves, and keeps the worker serving" do
        with_server(closing_under_a_writer(reports.port), mode: mode, workers: 1, threads: 1) do |_host, port|
          client = narrow_connection(port, receive_buffer: 4096)
          client.write("GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
          client.readpartial(4096)

          expect(reports.next(awaiting: "the stream's close")).to eq(:closed)
          client.close
          # The client left while the write waited: Kino::ClientGone.
          expect(reports.next(awaiting: "the writer's end")).to eq("Kino::ClientGone")
          expect(get(port, "/")).to eq("ok")
        ensure
          client&.close
        end
      end
    end
  end

  describe "concurrency" do
    it "overlaps slow requests across worker threads" do
      app = lint(lambda do |_env|
        sleep 0.25
        [200, {"content-type" => "text/plain"}, ["slept"]]
      end)

      with_server(app, workers: 1, threads: 4) do |host, port|
        codes = nil
        elapsed = timed do
          threads = 4.times.map do
            Thread.new { Net::HTTP.get_response(host, "/", port).code }
          end
          codes = threads.map(&:value)
        end

        expect(codes).to all(eq("200"))
        expect(elapsed).to be < 0.8, "4 x 0.25s requests on 4 workers took #{elapsed.round(2)}s"
      end
    end

    it "reuses a keep-alive connection for sequential requests" do
      app = lint(->(_env) { [200, {"content-type" => "text/plain"}, ["pong"]] })

      with_server(app) do |host, port|
        Net::HTTP.start(host, port) do |http|
          expect(http.get("/a").body).to eq("pong")
          expect(http.get("/b").body).to eq("pong")
        end
      end
    end
  end
end

# A Rack 3 streaming body that writes "streamed", raising after it when
# `raises`, and reports its close on `port`.
ReportingStreamingBody = Data.define(:port, :raises) do
  def call(stream)
    stream.write("streamed")
    raise "the streaming body failed" if raises
    stream.close
  end

  def close = port << :body_closed
end

# A partial hijack's callable that responds to close too: the app's to
# close, as the hijacked stream is.
ReportingHijack = Data.define(:port) do
  def call(stream)
    stream.write("hijacked")
    stream.close
  end

  def close = port << :hijack_closed
end

# The body a partial hijack's response carries, which the server ignores
# but closes.
ReportingIgnoredBody = Data.define(:port) do
  def to_ary = []

  def close = port << :ignored_body_closed
end
