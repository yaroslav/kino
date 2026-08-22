# frozen_string_literal: true

module Kino
  # Server log lines: the lifecycle notices, crash and respawn reports,
  # hook failures, the failed-request report, and whatever apps write to
  # `rack.errors`, all in one shape:
  #
  #   kino[4213] worker-3: after_worker_boot hook raised RuntimeError: boom
  #
  # The label is syslog's `ident[pid]` tag plus the source that spoke: the
  # worker ractor and/or thread by name, `main` for neither. On color
  # terminals the label is dim, yellow, or red by level; the message stays
  # plain. Notes go to stdout, warnings and errors to stderr.
  #
  # Hooks may log through here too (`Kino::Log.info "cache warm"`). Every
  # method is safe inside a worker ractor: the line is handed to the
  # native layer, which owns the streams, so no ractor touches `$stdout`
  # or `$stderr` itself.
  module Log
    # Frames shown in a failed-request report before the rest are folded.
    FRAMES = 12

    # The working directory at boot, stripped from backtrace frames so the
    # app's own code reads `app/controllers/x.rb:9` rather than an
    # absolute path (frozen: worker ractors read it).
    WORKING_DIR = File.join(Dir.pwd, "").freeze

    module_function

    # @param message [#to_s]
    # @return [void]
    def info(message)
      Native.log_line("info", source, message.to_s)
    end

    # @param message [#to_s]
    # @return [void]
    def warn(message)
      Native.log_line("warn", source, message.to_s)
    end

    # @param message [#to_s]
    # @return [void]
    def error(message)
      Native.log_line("error", source, message.to_s)
    end

    # The failed-request report: the request line, the error, and where it
    # raised in the app, then the backtrace with the app's own frames
    # first (relative to the working directory) and the rest folded.
    #
    #   500 GET /boom · RuntimeError: kaboom (app.rb:12:in 'explode')
    #       app.rb:12:in 'explode'
    #       /gems/rack-3.2.7/lib/rack/builder.rb:...
    #       … 38 more
    #
    # @param error [Exception]
    # @param env [Hash] the Rack env of the failed request
    # @param status [Integer] the status the client got
    # @return [void]
    def exception(error, env, status: 500)
      frames, depth = trace(error)
      site = frames.first ? " (#{frames.first})" : ""
      lines = ["#{status} #{env["REQUEST_METHOD"]} #{env["PATH_INFO"]} · #{error.class}: #{error.message}#{site}"]
      frames.each { |frame| lines << "    #{frame}" }
      lines << "    … #{depth - frames.size} more" if depth > frames.size
      error(lines.join("\n"))
    end

    # The `kino[<pid>] <source>:` tag a line from here carries.
    # @return [String]
    def label
      "kino[#{Process.pid}] #{source}:"
    end

    # Who is speaking: the ractor's name, the thread's name, both joined
    # with a slash, or `main` when neither is named. Kino names its
    # worker ractors and threads `worker-N`.
    # @return [String]
    def source
      parts = [Ractor.current.name, Thread.current.name].compact
      parts.empty? ? "main" : parts.join("/")
    end

    # The backtrace as [frames, depth]: each frame relativized to the
    # working directory, the app's own frames floated to the front (the
    # raise site in your code reads first; gem and stdlib frames keep
    # their order below), capped at FRAMES; depth is the real length.
    def trace(error)
      raw = error.backtrace || []
      app, rest = raw.map { |frame| frame.delete_prefix(WORKING_DIR) }.partition { |frame| app_frame?(frame) }
      [(app + rest).first(FRAMES), raw.size]
    end

    # A project-relative path (the working-directory prefix came off, so
    # it does not start with `/`) that is not a synthetic frame (`(eval)`,
    # `<internal:...>`). Gem and stdlib frames stay absolute.
    def app_frame?(frame)
      !frame.start_with?("/", "<", "(")
    end

    private_class_method :trace, :app_frame?
  end
end
