# frozen_string_literal: true

require "timeout"
require "tmpdir"

# Scenarios that run in a Ruby of their own: one that faults, wedges or
# changes Kino for good (a prepended module) cannot take the suite with it.
module Scenarios
  # What a scenario run by run_bounded starts with.
  def scenario_prelude = <<~RUBY
    Warning[:experimental] = false
    $stdout.sync = true
    require "kino"
    require "async"
    require "json"
    require "net/http"
  RUBY

  # Runs `script` in a Ruby of its own, killed past `within` seconds,
  # which fails naming what it last wrote: [its exit status, its stdout,
  # its stderr].
  def run_bounded(script, within: 30)
    Dir.mktmpdir do |dir|
      out, err = File.join(dir, "out"), File.join(dir, "err")
      pid = spawn(Gem.ruby, "-I", File.expand_path("../../lib", __dir__), "-e", script, out: out, err: err)
      status = begin
        Timeout.timeout(within) { Process.wait2(pid).last }
      rescue Timeout::Error
        Process.kill(:KILL, pid)
        Process.wait(pid)
        raise "the scenario did not end within #{within} s; its stderr ends: #{File.read(err)[-2000..] || File.read(err)}"
      end
      [status, File.read(out), File.read(err)]
    end
  end
end

RSpec.configure do |config|
  config.include Scenarios
end
