# frozen_string_literal: true

require "bundler/gem_tasks"
require "rb_sys/extensiontask"

# Dev-only tooling is absent in the rb-sys-dock cross-build containers
# (they install a slim bundle); the Rakefile must still load there.
begin
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

begin
  require "standard/rake"
rescue LoadError
end

# NOTE: no `task build: :compile` here, deliberately. Inside rb-sys-dock,
# the packaging task chain runs with cross-compile env set; a host-platform
# compile hooked onto `build` then links host objects with the cross
# target dir and fails. `gem install` of the source gem compiles via
# extconf.rb regardless, so nothing needs the hook.

GEMSPEC = Gem::Specification.load("kino.gemspec")

RbSys::ExtensionTask.new("kino", GEMSPEC) do |ext|
  ext.lib_dir = "lib/kino"
end

# Workaround for cross-compiling native gems inside rb-sys-dock with a
# recent rubygems. Gem::PackageTask makes the native gem file depend on
# spec.files DIRECTLY (ignoring rake-compiler's cleared package_files),
# which drags in the bare lib/kino/kino.so path. rake-compiler wires that
# path to the HOST platform's copy task (the cross definition skips it:
# "unless task_defined?"), so packaging a Linux cross gem triggers a host
# compile whose cross-poisoned env links wrong-format objects. Re-point
# the bare path at the cross copy tasks; the staged cross binary is what
# the gem packs anyway. No effect outside the dock (RUBY_TARGET unset).
if (target = ENV["RUBY_TARGET"]) && Rake::Task.task_defined?("lib/kino/kino.so")
  cross_copies = Rake::Task.tasks.map(&:name)
    .grep(/\Acopy:kino:#{Regexp.escape(target)}:/)
  unless cross_copies.empty?
    Rake::Task["lib/kino/kino.so"].prerequisites.replace(cross_copies)
  end
end

desc "Sync ext/kino/Cargo.toml [package] version from Kino::VERSION"
task :sync_cargo_version do
  require_relative "lib/kino/version"
  cargo_toml = File.expand_path("ext/kino/Cargo.toml", __dir__)
  toml = File.read(cargo_toml)
  # Only the [package] version is line-anchored; dependency versions are
  # inline tables and don't match.
  updated = toml.sub(/^version = "[^"]*"$/, %(version = "#{Kino::VERSION}"))
  unless updated == toml
    File.write(cargo_toml, updated)
    puts "ext/kino/Cargo.toml version -> #{Kino::VERSION}"
  end
end

task compile: :sync_cargo_version

desc "Run Rust unit tests (cargo test)"
task :cargo_test do
  sh "cargo test --manifest-path #{File.expand_path("ext/kino/Cargo.toml", __dir__)}"
end

desc "Validate RBS signatures (sig/)"
task :rbs_validate do
  # -r logger: Kino::Logger subclasses ::Logger from the logger gem.
  sh "rbs -r logger -I sig validate"
end

task default: %i[compile cargo_test spec rbs_validate standard]
