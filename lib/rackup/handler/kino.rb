# frozen_string_literal: true

module Rackup
  module Handler
    # The Rack handler: lets any host that speaks the rackup protocol boot
    # Kino, which is what `rackup -s kino` and `rails server -u kino` do.
    # Loaded on demand by Rackup::Handler.get(:kino), so Rackup itself is
    # already defined here; Kino is required only when the host calls in.
    module Kino
      # Host option name => Kino setting plus the coercion it needs: rackup
      # hands `-O NAME=VALUE` values (and its own -p) over as strings.
      OPTION_MAP = {
        Host: [:bind, ->(value) { value.to_s }],
        Port: [:port, ->(value) { Integer(value) }],
        Workers: [:workers, ->(value) { Integer(value) }],
        Threads: [:threads, ->(value) { Integer(value) }],
        Mode: [:mode, ->(value) { value.to_sym }]
      }.freeze
      private_constant :OPTION_MAP

      # Boot a server for `app` and block until it shuts down, the way the
      # `kino` executable does (banner, INT/TERM drain, stats on USR1).
      #
      # @param app [#call] the Rack application the host built
      # @param options [Hash] the host's options (see {.server_options})
      # @yield [server] the built, not yet started server, for hosts that
      #   want a handle on it
      # @return [::Kino::Server] the stopped server, after shutdown
      def self.run(app, **options)
        require "kino"
        server = ::Kino::Server.new(app, **server_options(options))
        yield server if block_given?
        server.run
      end

      # The `-O NAME=VALUE` options `rackup -s kino --help` lists (rackup
      # shows its own -o/-p in place of Host and Port).
      # @return [Hash{String => String}]
      def self.valid_options
        {
          "Host=HOST" => "Address to bind (default: 127.0.0.1)",
          "Port=PORT" => "Port to listen on (default: 9292)",
          "Workers=COUNT" => "Workers: ractors in :ractor mode, thread groups in :threaded (default: one per CPU)",
          "Threads=COUNT" => "Threads per worker (default: 1 in :ractor, 3 in :threaded)",
          "Mode=MODE" => "auto | ractor | threaded (default: auto)",
          "Config=PATH" => "Kino config file (default: kino.rb, then config/kino.rb)"
        }
      end

      # Translate host options into {::Kino::Server#initialize} kwargs.
      # Precedence: options the user typed > the config file > defaults the
      # host supplied (rackup's and Rails' own Host and Port) > Kino's
      # defaults. Hosts that say which options were typed pass
      # `user_supplied_options`; when that list is absent every option
      # counts as typed. Keys outside OPTION_MAP (the host's bookkeeping:
      # environment, pid, config, ...) are ignored.
      #
      # @param options [Hash{Symbol => Object}]
      # @return [Hash{Symbol => Object}]
      def self.server_options(options)
        require "kino"
        options = options.dup
        host_defaults = {}
        if (typed = options.delete(:user_supplied_options))
          (options.keys - typed).each { |key| host_defaults[key] = options.delete(key) }
        end

        config = ::Kino::Configuration.new
        path = options.delete(:Config) || host_defaults.delete(:Config) || ::Kino::Configuration.default_path
        config.load_file(path) if path
        translate(host_defaults).each { |key, value| config.set(key, value) unless config.set?(key) }
        config.merge!(translate(options))
        config.set(:port, ::Kino::Configuration::DEFAULT_SERVING_PORT) unless config.set?(:port)
        config.server_options
      end

      def self.translate(options)
        options.filter_map do |key, value|
          setting, coerce = OPTION_MAP[key]
          [setting, coerce.call(value)] if setting
        end.to_h
      end
      private_class_method :translate
    end

    register :kino, Kino
  end
end
