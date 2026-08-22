# frozen_string_literal: true

require_relative "lib/kino/version"

Gem::Specification.new do |spec|
  spec.name = "kino"
  spec.version = Kino::VERSION
  spec.authors = ["Yaroslav Markin"]
  spec.email = ["yaroslav@markin.net"]

  spec.summary = "High-performance Ractor web server for Ruby."
  spec.description = "A high-performance Ractor web server for Ruby 4.0+: Rack 3-based, " \
                     "with a Rust tokio/hyper front-end and Ractor-parallel Ruby workers " \
                     "and threaded fallback mode."
  spec.homepage = "https://github.com/yaroslav/kino"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 4.0"
  spec.metadata["allowed_push_host"] = "https://rubygems.org"
  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/yaroslav/kino"
  spec.metadata["changelog_uri"] = "https://github.com/yaroslav/kino/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://rubydoc.info/gems/kino"

  spec.metadata["rubygems_mfa_required"] = "true"

  # Explicit globs, no git shellout: the gemspec must evaluate identically
  # from a packaged gem, a git/path bundler source, and a slim container.
  spec.files = Dir["lib/**/*.rb"] +
    Dir["lib/kino/templates/*.tt"] +
    Dir["exe/*"] +
    Dir["ext/kino/**/*.{rb,rs,toml}"] +
    Dir["sig/**/*.rbs"] +
    Dir["doc/*.md"] +
    %w[README.md CHANGELOG.md LICENSE.txt Cargo.toml Cargo.lock .yardopts]
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/kino/extconf.rb"]

  spec.add_dependency "logger", ">= 1.6"
  spec.add_dependency "rack", ">= 3.1"
  spec.add_dependency "rb_sys", "~> 0.9.128"

  spec.add_development_dependency "rackup", "~> 2.2"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler"
  spec.add_development_dependency "rbs", "~> 4.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "standard", "~> 1.50"
  spec.add_development_dependency "yard", "~> 0.9"
end
