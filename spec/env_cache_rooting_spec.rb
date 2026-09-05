# frozen_string_literal: true

# The env string caches hand frozen strings built by one worker ractor to
# env hashes built by every other, and the LRU caches behind them insert
# and evict from all ractors in parallel. These specs drive that path
# harder than real traffic does: more distinct hosts, peers, and header
# values than the caches hold, from four ractors at once, with the GC
# running constantly. A lost or corrupted root shows up as a wrong or
# freed string (or a crash) in some env.
RSpec.describe "env string cache rooting" do
  it "builds the cache-backed env slice from the Host header or the URI authority" do
    from_host = Kino::Native._test_env_probe("app.example:8080", nil, "192.0.2.7", "probe/1")
    expect(from_host.slice("SERVER_NAME", "SERVER_PORT", "REMOTE_ADDR", "HTTP_USER_AGENT")).to eq(
      "SERVER_NAME" => "app.example", "SERVER_PORT" => "8080",
      "REMOTE_ADDR" => "192.0.2.7", "HTTP_USER_AGENT" => "probe/1"
    )
    expect(from_host).not_to have_key("HTTP_HOST")

    from_authority = Kino::Native._test_env_probe("ignored", "api.example:8443", "2001:db8::1", "probe/2")
    expect(from_authority.slice("SERVER_NAME", "SERVER_PORT", "HTTP_HOST", "REMOTE_ADDR")).to eq(
      "SERVER_NAME" => "api.example", "SERVER_PORT" => "8443",
      "HTTP_HOST" => "api.example:8443", "REMOTE_ADDR" => "2001:db8::1"
    )
    expect(from_authority.values).to all(be_frozen)
  end

  # A collection between each fresh string and the slab slot or env that
  # roots it for good, and between a cache read and the aset that follows
  # it: misses, hits, and the in-place upgrade of a Host-header entry to
  # an authority entry all get one.
  it "keeps values intact with a GC at every allocation on the cache paths" do
    GC.stress = true
    envs = 3.times.flat_map do
      [
        Kino::Native._test_env_probe("stress.example:8080", nil, "198.51.100.9", "stress/1"),
        Kino::Native._test_env_probe("ignored", "stress.example:8080", "198.51.100.9", "stress/1")
      ]
    end
    GC.stress = false
    GC.start

    envs.each_slice(2) do |from_host, from_authority|
      expect(from_host.slice("SERVER_NAME", "SERVER_PORT", "REMOTE_ADDR", "HTTP_USER_AGENT")).to eq(
        "SERVER_NAME" => "stress.example", "SERVER_PORT" => "8080",
        "REMOTE_ADDR" => "198.51.100.9", "HTTP_USER_AGENT" => "stress/1"
      )
      expect(from_authority["HTTP_HOST"]).to eq("stress.example:8080")
    end
  ensure
    GC.stress = false
  end

  it "keeps every cached string intact while four ractors insert and evict under GC pressure" do
    rounds = 400
    workers = Array.new(4) do |worker|
      Ractor.new(worker, rounds) do |worker, rounds|
        mismatches = []
        rounds.times do |index|
          n = worker * rounds + index
          host = "host#{n}.example:#{10_000 + n}"
          authority = index.odd? ? "auth#{n}.example:#{20_000 + n}" : nil
          ip = "10.#{worker}.#{index / 256}.#{index % 256}"
          agent = "agent-#{n}"
          env = Kino::Native._test_env_probe(host, authority, ip, agent)
          GC.start if (index % 16).zero?

          expected = {
            "SERVER_NAME" => authority ? "auth#{n}.example" : "host#{n}.example",
            "SERVER_PORT" => (authority ? 20_000 + n : 10_000 + n).to_s,
            "REMOTE_ADDR" => ip,
            "HTTP_USER_AGENT" => agent
          }
          expected["HTTP_HOST"] = authority if authority
          expected.each do |key, want|
            got = env[key]
            mismatches << [worker, index, key, got, want] unless got == want && got.frozen?
          end
        end
        mismatches
      end
    end

    expect(workers.flat_map(&:value)).to eq([])
  end
end
