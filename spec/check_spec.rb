# frozen_string_literal: true

RSpec.describe Kino::Check do
  def findings_for(app)
    described_class.report(app)[:findings].map(&:to_s)
  end

  it "passes a shareable app" do
    app = Ractor.shareable_proc { |_env| [200, {}, []] }
    result = described_class.report(app)

    expect(result[:shareable]).to be(true)
    expect(result[:findings]).to be_empty
  end

  it "names the captured variable for a closure over mutable state" do
    cache = {}
    app = ->(_env) { [200, {}, [cache.size.to_s]] }

    lines = findings_for(app)
    expect(lines).to include(a_string_matching(/captures `cache` = \{\} \(Hash\) \(unshareable\)/))
    expect(lines.first).to include("check_spec.rb")
  end

  it "flags a proc whose self is unshareable" do
    holder = Object.new
    app = holder.instance_eval { proc { |_env| [200, {}, []] } }

    lines = findings_for(app)
    expect(lines).to include(a_string_matching(/self is not shareable.*Ractor\.shareable_proc/))
  end

  it "walks instance variables by path" do
    app = Object.new
    app.instance_variable_set(:@store, {"sessions" => +"mutable"})
    def app.call(_env) = [200, {}, []]

    lines = findings_for(app)
    expect(lines).to include(a_string_matching(/app\.@store/))
  end

  it "warns about class-level ivars on class-style apps" do
    klass = Class.new do
      @instance = {config: "mutable"}
      def self.call(_env) = [200, {}, []]
    end

    result = described_class.report(klass)
    expect(result[:shareable]).to be(false)
    expect(result[:findings].map(&:to_s))
      .to include(a_string_matching(/class-level ivar.*Ractor::IsolationError/))
  end

  it "passes a class-style app with no unshareable class state" do
    klass = Class.new do
      def self.call(_env) = [200, {}, []]
    end

    expect(described_class.report(klass)[:shareable]).to be(true)
  end

  it "descends into hashes and arrays" do
    app = Object.new
    app.instance_variable_set(:@routes, {"/" => [+"handler"]})

    lines = findings_for(app)
    expect(lines).to include(a_string_matching(/@routes/))
  end

  it "print_report returns true/false and prints findings" do
    out = StringIO.new
    ok = described_class.print_report(Ractor.shareable_proc { |_e| [200, {}, []] }, io: out)
    expect(ok).to be(true)
    expect(out.string).to include("Ractor-shareable")

    out = StringIO.new
    state = {}
    ok = described_class.print_report(->(_e) { [200, {}, [state.size.to_s]] }, io: out)
    expect(ok).to be(false)
    expect(out.string).to include("NOT Ractor-shareable")
    expect(out.string).to include("captures `state`")
    expect(out.string).to include("hints:")
  end
end
