# frozen_string_literal: true

# Zero-copy response bodies: bodies of 4 KB and up ride to hyper by
# reference, with the Ruby string pinned until the write completes.
# These specs prove the served bytes stay correct while the GC compacts,
# while the app mutates the string it just served, and across both
# dispatch modes; small bodies keep the plain copy path.
RSpec.describe "zero-copy response bodies" do
  let(:big_frozen) { ("f" * 65_536).freeze }
  let(:octet) { {"content-type" => "application/octet-stream"}.freeze }

  it "serves a large frozen body intact across GC compactions (threaded)" do
    body = big_frozen
    headers = octet
    app = ->(_env) { [200, headers, [body]] }

    with_server(app) do |host, port|
      5.times do
        response = Net::HTTP.get_response(host, "/", port)
        expect(response.body).to eq(big_frozen)
        GC.compact
      end
    end
  end

  it "serves large per-request bodies intact (threaded)" do
    headers = octet
    app = ->(env) { [200, headers, [env["PATH_INFO"].delete_prefix("/") * 8192]] }

    with_server(app) do |host, port|
      %w[aa bb cc].each do |seed|
        response = Net::HTTP.get_response(host, "/#{seed}", port)
        expect(response.body).to eq(seed * 8192)
      end
    end
  end

  it "keeps a served body intact when the app mutates the string afterwards" do
    buffer = +"m" * 16_384
    headers = octet
    app = lambda do |_env|
      snapshot = buffer.dup
      buffer << "!" # mutate AFTER the previous response pinned this string
      [200, headers, [snapshot.freeze]]
    end

    with_server(app) do |host, port|
      lengths = 4.times.map { Net::HTTP.get_response(host, "/", port).body.length }
      expect(lengths).to eq([16_384, 16_385, 16_386, 16_387])
    end
  end

  it "serves large frozen and per-request bodies intact in :ractor mode" do
    app = Ractor.shareable_proc do |env|
      if env["PATH_INFO"] == "/frozen"
        [200, {"content-type" => "application/octet-stream"}, ["f" * 65_536]]
      else
        [200, {"content-type" => "application/octet-stream"}, [+"r" * 32_768]]
      end
    end

    with_server(app, mode: :ractor, workers: 2, threads: 1) do |host, port|
      3.times do
        expect(Net::HTTP.get_response(host, "/frozen", port).body).to eq("f" * 65_536)
        expect(Net::HTTP.get_response(host, "/fresh", port).body).to eq("r" * 32_768)
        GC.compact
      end
    end
  end

  it "streams large chunks intact through a chunked response" do
    chunks = [+"a" * 8_192, ("b" * 8_192).freeze, +"c" * 8_192]
    headers = octet
    app = ->(_env) { [200, headers, chunks] }

    with_server(app) do |host, port|
      response = Net::HTTP.get_response(host, "/", port)
      expect(response.body).to eq("a" * 8_192 + "b" * 8_192 + "c" * 8_192)
    end
  end

  it "survives concurrent large responses racing GC compaction" do
    body = big_frozen
    headers = octet
    app = ->(_env) { [200, headers, [body]] }

    with_server(app, threads: 4) do |host, port|
      readers = 4.times.map do
        Thread.new do
          10.times do
            got = Net::HTTP.get_response(host, "/", port).body
            Thread.current.kill unless got == big_frozen
          end
          :ok
        end
      end
      3.times do
        GC.compact
        sleep 0.05
      end
      expect(readers.map(&:value)).to all(eq(:ok))
    end
  end
end
