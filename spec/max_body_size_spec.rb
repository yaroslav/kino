# frozen_string_literal: true

RSpec.describe "max_body_size" do
  # Echoes how many body bytes the app managed to read.
  let(:echo_len) do
    lambda do |env|
      body = env["rack.input"].read
      [200, {"content-type" => "text/plain"}, [body.bytesize.to_s]]
    end
  end

  it "rejects a truthfully-declared oversize body with a 413, before the app runs" do
    with_server(echo_len, max_body_size: 1024) do |host, port, _server|
      response = Net::HTTP.start(host, port) do |http|
        http.post("/", "x" * 4096, "content-type" => "text/plain")
      end

      expect(response.code).to eq("413")
    end
  end

  it "accepts a body within the limit" do
    with_server(echo_len, max_body_size: 1024) do |host, port, _server|
      response = Net::HTTP.start(host, port) do |http|
        http.post("/", "x" * 512, "content-type" => "text/plain")
      end

      expect(response.code).to eq("200")
      expect(response.body).to eq("512")
    end
  end

  it "is unlimited when set to nil" do
    with_server(echo_len, max_body_size: nil) do |host, port, _server|
      response = Net::HTTP.start(host, port) do |http|
        http.post("/", "x" * 100_000, "content-type" => "text/plain")
      end

      expect(response.code).to eq("200")
      expect(response.body).to eq("100000")
    end
  end

  it "aborts a chunked body that exceeds the limit mid-stream (no Content-Length to check up front)" do
    with_server(echo_len, max_body_size: 1024) do |_host, port, _server|
      # 8 KiB: well past the 1 KiB cap.
      status = post_chunked(port, "/", 8)

      # Either an explicit error status or a dropped connection; never a 200
      # with the full body accepted.
      expect(status).to satisfy { |line| line.nil? || !line.include?(" 200 ") }
    end
  end
end
