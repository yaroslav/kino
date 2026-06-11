# frozen_string_literal: true

# The native layer swaps these two rack.input implementations invisibly
# per request (NullInput for bodyless requests, Input otherwise), so they
# must agree on IO#read semantics at EOF.
RSpec.describe "rack.input parity" do
  describe Kino::NullInput do
    let(:input) { Kino::NullInput::INSTANCE }

    it "returns '' for read and read(0), nil for read(n)" do
      expect(input.read).to eq("")
      expect(input.read(0)).to eq("")
      expect(input.read(10)).to be_nil
    end

    it "fills the caller's buffer, binary-encoded" do
      buffer = +"junk"
      expect(input.read(nil, buffer)).to equal(buffer)
      expect(buffer).to eq("")
      expect(buffer.encoding).to eq(Encoding::BINARY)
      expect(input.read(10, +"junk")).to be_nil
    end

    it "returns nil from gets and yields nothing from each" do
      expect(input.gets).to be_nil
      expect { |b| input.each(&b) }.not_to yield_control
    end
  end

  describe Kino::Input do
    it "agrees with NullInput at EOF over a live empty body" do
      app = lambda do |env|
        input = Kino::Input.new(env["kino.request"])
        reads = [input.read, input.read(0), input.read(10), input.gets]
        [200, {"content-type" => "text/plain"}, [reads.map(&:inspect).join(",")]]
      end

      with_server(app) do |host, port, _server|
        response = Net::HTTP.get_response(host, "/", port)

        expect(response.body).to eq('"","",nil,nil')
      end
    end
  end
end
