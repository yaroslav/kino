# frozen_string_literal: true

# Probes whether the current Rails (edge) can be served from worker
# Ractors yet, and reports the exact blocker if not:
#   bundle exec ruby ractor_probe.rb
#
# When this script prints SUCCEEDED, switch kino.rb to `mode :ractor`.
Warning[:experimental] = false

require "kino"
require_relative "app"

puts "Rails #{Rails.version} on Ruby #{RUBY_VERSION}"

begin
  app = Ractor.make_shareable(Rails.application)
  puts "Ractor.make_shareable(Rails.application) SUCCEEDED — trying a real request"
rescue Ractor::Error, TypeError, FrozenError => e
  puts "blocker at make_shareable (#{e.class}):"
  puts "  #{e.message.lines.first&.strip}"
  exit 1
end

server = Kino::Server.new(app, mode: :ractor, workers: 2, threads: 1).start
require "net/http"
response = Net::HTTP.get_response("127.0.0.1", "/", server.port)
puts "ractor-mode response: HTTP #{response.code} #{response.body&.slice(0, 60)}"
server.shutdown
