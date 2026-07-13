# frozen_string_literal: true

require "open3"
require "tmpdir"

RSpec.describe "native extension Makefile" do
  it "uses an absolute echo command so Bundler binstubs cannot hijack install output" do
    repo = File.expand_path("..", __dir__)

    Dir.mktmpdir do |dir|
      _out, err, status = Open3.capture3(
        {"RB_SYS_TEST" => "1"},
        Gem.ruby,
        "-I#{repo}",
        File.join(repo, "ext/kino/extconf.rb"),
        chdir: dir
      )

      expect(status).to be_success, err
      makefile = File.read(File.join(dir, "Makefile"))
      expect(makefile).to include("ECHO = $(ECHO1:0=@ /bin/echo)")
      expect(makefile).not_to include("ECHO = $(ECHO1:0=@ echo)")
    end
  end
end
