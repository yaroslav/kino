# frozen_string_literal: true

require "optparse"
require_relative "version"

module Kino
  # The `kino` executable (CLI.start) plus startup presentation shared with
  # Server.run: the banner, ANSI styling, and the exit credit. Nothing here
  # is part of the serving API. (The native layer has a twin of `paint` in
  # style.rs for the few places Rust writes to the terminal.)
  #
  # This file deliberately loads no native code: `require "kino"` happens
  # inside the actions that need it, so --help and --version stay instant.
  module CLI
    # The plain banner art: "Kino" in TheDraw's Mindbenders font; {motd}
    # adds the original three-tone shading.
    MOTD = <<~BANNER
      ggg    .o
      $$$_,o$$P aaa $$$eea,.   .,aaa,.
      %$$`4eP'  $$$ $$$``$$$% $$$```$$$
      $$$--`$$o ggg $$$---$$$ $$$---$$$
      $$$ ░ $$$ $$$ $$$ ░ $$$ $$$ ░ $$$
      $$$---$$$ $$$ $$$---$$$ $$$---$$$
      $$$   $$$ $$' $$$   $$$ ^$$aaaS$'
    BANNER

    # Tone stencil aligned with MOTD, character by character: 1 bright
    # white, 2 light gray, 3 dark gray; spaces follow the art.
    MOTD_TONES = <<~BANNER
      111    11
      111111111 111 11111111   1111111
      11111111  111 111111111 111111111
      111331111 111 111333111 111333111
      111 3 322 122 111 3 322 112 3 122
      111333322 223 111333223 123333223
      112   233 333 222   333 122233332
    BANNER

    # The basic-palette SGR code for each stencil tone.
    TONE_SGR = {"1" => "97", "2" => "37", "3" => "90"}.freeze

    private_constant :MOTD_TONES, :TONE_SGR

    module_function

    # True when output to `io` may use ANSI styling.
    # @param io [IO]
    # @return [Boolean]
    def color?(io = $stdout)
      io.tty? && ENV["NO_COLOR"].nil? && ENV["TERM"] != "dumb"
    end

    # Wrap `text` in an SGR code ("1" bold, "31" red, "38;5;N" 256-color),
    # resetting at the end; plain when `io` is not a color terminal.
    #
    # @param code [String] an SGR code
    # @param text [String]
    # @param io [IO] the stream the text is destined for (gates coloring)
    # @return [String]
    def paint(code, text, io: $stdout)
      color?(io) ? "\e[#{code}m#{text}\e[0m" : text
    end

    # Startup-output styling, same gray family as the banner.
    # @return [String]
    def dim(text, io: $stdout)
      paint("38;5;243", text, io: io)
    end

    # Bold styling for headings and the Action!/Fin. bookends.
    # @return [String]
    def bold(text, io: $stdout)
      paint("1", text, io: io)
    end

    # Errors are red (gated on stderr unless another io is given).
    # @return [String]
    def red(text, io: $stderr)
      paint("91", text, io: io)
    end

    # The banner with its three-tone shading applied per character.
    # @param color [Boolean]
    # @return [String]
    def motd(color: color?)
      return MOTD unless color

      MOTD.lines.zip(MOTD_TONES.lines).map do |art, tones|
        current = nil
        line = art.chomp.each_char.with_index.map { |char, i|
          sgr = TONE_SGR[tones.to_s[i]]
          if char != " " && sgr && sgr != current
            current = sgr
            "\e[#{sgr}m#{char}"
          else
            char
          end
        }.join
        "#{line}\e[0m\n"
      end.join
    end

    # One-line stats dump (the SIGUSR1 handler's output). Excludes
    # worker_status: it's an array with one entry per execution slot, and
    # printing it inline would break the one-line contract (see /stats for
    # per-worker detail).
    # @param stats [Hash{Symbol => Object}] see {Kino::Server#stats}
    # @return [String]
    def stats_line(stats)
      dim("Kino stats: #{stats.except(:worker_status).map { |k, v| "#{k}=#{v.inspect}" }.join(" ")}")
    end

    # The two banner halves around Server#start: credits before, the ready
    # block plus a bold "Action!" after, once mode and port are known.
    # Server.run is the one caller; the kino CLI funnels into it.
    # @return [void]
    def opening_credits
      puts motd
      puts dim("\nKino #{VERSION} presents:")
    end

    # @param server [Kino::Server] a started server
    # @return [void]
    def action!(server)
      puts dim("- mode:      #{server.mode}")
      puts dim("- listening: http#{"s" if server.tls?}://#{server.bind}:#{server.port}")
      puts dim("- Ctrl-C to drain and stop")
      puts "\n#{bold("Action!")}\n\n"
    end

    # Roll credits when the process ends: normal exit or crash (at_exit
    # also runs after an uncaught exception; only a force-exit skips it).
    # @return [void]
    def fin_at_exit
      return if @fin_registered

      @fin_registered = true
      at_exit { $stdout.puts bold("\nFin.\n") }
    end

    # The `kino` executable: parse flags, then init/check/serve. Returns
    # the process exit status (exe/kino passes it to Kernel#exit), except
    # for -v and -h, which print and exit in place per optparse convention.
    #
    # @param argv [Array<String>] command-line arguments (consumed)
    # @return [Integer] process exit status
    def start(argv)
      options = {overrides: {}}
      parser = option_parser(options)
      parser.parse!(argv)

      return write_sample(options[:init_path]) if options[:init_path]

      config = resolve_config(options)

      # Precedence for the rackup file: positional arg > `rackup` in config > config.ru
      rackup_file = argv.first || config[:rackup] || "config.ru"
      unless File.exist?(rackup_file)
        warn red("Kino: #{rackup_file} not found")
        puts
        print_help(parser)
        return 1
      end

      ENV["RACK_ENV"] ||= config[:environment] if config[:environment]

      app = Rack::Builder.parse_file(rackup_file)
      app = app.first if app.is_a?(Array) # rack < 3 compat

      return Check.print_report(app) ? 0 : 1 if options[:check]

      serve(app, config)
      0
    end

    # Bun-style colored help, generated from the parser's own switch list
    # so it can never drift from the real options.
    def print_help(parser)
      puts "#{bold("Kino")}#{dim(": high-performance Ractor web server for Ruby")}"
      puts
      puts "#{bold("Usage:")} kino #{paint("36",
        "[options]")} #{paint("36",
          "[rackup file]")}#{dim("   (default: config.ru)")}"
      puts
      puts bold("Options:")
      parser.top.list.each do |switch|
        next unless switch.is_a?(OptionParser::Switch) && switch.desc.any?

        flags = [*switch.short, *switch.long].join(", ")
        flags += " #{switch.arg.strip}" if switch.arg
        puts "  #{paint("36", flags.ljust(24))} #{dim(switch.desc.join(" "))}"
      end
      puts
      puts bold("Examples:")
      puts "  #{paint("36", "kino --init")}#{dim("              write a documented kino.rb")}"
      puts "  #{paint("36", "kino")}#{dim("                     serve config.ru on :9292")}"
      puts "  #{paint("36", "kino --check app.ru")}#{dim("      explain Ractor-shareability")}"
    end

    def option_parser(options)
      OptionParser.new do |opts|
        opts.banner = "Usage: kino [options] [rackup file (default: config.ru)]"
        opts.on("-C", "--config FILE", "Config file (default: kino.rb if present)") { |v| options[:config_file] = v }
        opts.on("--init [PATH]", "Write a commented sample config (default: kino.rb) and exit") do |v|
          options[:init_path] = v || "kino.rb"
        end
        opts.on("--check", "Load the app and report Ractor-shareability, then exit") { options[:check] = true }
        opts.on("-b", "--bind HOST", "Bind address") { |v| options[:overrides][:bind] = v }
        opts.on("-p", "--port PORT", Integer, "Port") { |v| options[:overrides][:port] = v }
        opts.on("-w", "--workers COUNT", Integer, "Worker count") { |v| options[:overrides][:workers] = v }
        opts.on("-t", "--threads COUNT", Integer, "Threads per worker") { |v| options[:overrides][:threads] = v }
        opts.on("-m", "--mode MODE", "auto | ractor | threaded") { |v| options[:overrides][:mode] = v.to_sym }
        opts.on("-v", "--version") do
          puts "kino #{VERSION}"
          exit
        end
        opts.on_tail("-h", "--help", "Show this help") do
          print_help(opts)
          exit
        end
      end
    end

    def write_sample(path)
      require "kino"
      Configuration.write_sample(path)
      puts "Kino: wrote sample config to #{path}"
      0
    rescue Kino::Error => e
      warn red("kino: #{e.message}")
      1
    end

    # Resolve the full configuration once: file + CLI flag overrides.
    def resolve_config(options)
      require "kino"
      require "rack"

      config_file = options[:config_file]
      config_file ||= ("kino.rb" if File.exist?("kino.rb"))

      config = Configuration.new
      config.load_file(config_file) if config_file
      config.merge!(options[:overrides])
      # Default port 9292 when neither the file nor a flag chose one.
      config.set(:port, 9292) unless config.set?(:port)
      config
    end

    def serve(app, config)
      Server.run(app, **config.server_options)
    end

    private_class_method :print_help, :option_parser, :write_sample,
      :resolve_config, :serve
  end
end
