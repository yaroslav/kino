# frozen_string_literal: true

module Kino
  # The shareability doctor behind `kino --check`: explains WHY an app
  # can't run in :ractor mode, instead of leaving you to decode
  # Ractor::IsolationError one ivar at a time.
  #
  # The walk is strictly non-mutating: Ractor.make_shareable would freeze
  # the user's object graph, so we never call it. Instead we recurse into
  # whatever Ractor.shareable? rejects and name the leaves: instance
  # variables by path, proc captures by variable name and definition site,
  # and the class-ivar trap that bites class-style apps (a Class is always
  # "shareable", but reading its unshareable ivars from a worker ractor
  # raises on the first request).
  module Check
    # Stop after this many findings: the first few name the problem.
    MAX_FINDINGS = 20
    # Walk budget, so a pathological object graph cannot hang the check.
    MAX_NODES = 5_000

    # One named blocker: a path into the object graph plus what is wrong
    # there.
    Finding = Struct.new(:path, :message, keyword_init: true) do
      # @return [String] "path — message", as printed by the CLI
      def to_s
        "#{path} — #{message}"
      end
    end

    module_function

    # @param app [#call] a Rack application (or a Class/Module used as one)
    # @return [Hash] +{shareable: Boolean, findings: Array<Finding>}+
    def report(app)
      findings = []
      seen = {}.compare_by_identity
      budget = {nodes: 0}

      if app.is_a?(Module)
        # Classes/modules pass Ractor.shareable? unconditionally, but their
        # unshareable class-level state is main-ractor-only at runtime.
        scan_module(app, "app (#{app.inspect})", findings)
        {shareable: findings.empty?, findings: findings}
      elsif Ractor.shareable?(app)
        {shareable: true, findings: []}
      else
        walk(app, "app", findings, seen, budget)
        findings << Finding.new(path: "app", message: unshareable_note(app)) if findings.empty?
        {shareable: false, findings: findings}
      end
    end

    # Pretty-printed report; returns true when the app is ractor-ready.
    # @param app [#call] a Rack application
    # @param io [IO] where to print
    # @return [Boolean]
    def print_report(app, io: $stdout)
      result = report(app)
      if result[:shareable]
        io.puts CLI.paint("32", "check: app is Ractor-shareable — mode :ractor will work", io: io)
        true
      else
        io.puts CLI.red("check: app is NOT Ractor-shareable", io: io)
        result[:findings].each { |finding| io.puts "  - #{finding}" }
        io.puts dim_hint(io)
        false
      end
    end

    def dim_hint(io)
      CLI.dim(
        "  hints: freeze config at boot; build endpoints with " \
        "Ractor.shareable_proc; keep per-worker resources in " \
        "Ractor.store_if_absent; or run mode :threaded.",
        io: io
      )
    end

    # Recurse into an unshareable object and name its blockers. Shareable
    # objects return immediately, so callers never need their own guard.
    def walk(obj, path, findings, seen, budget)
      return if Ractor.shareable?(obj)
      return if findings.size >= MAX_FINDINGS
      return if seen[obj]
      seen[obj] = true
      return if (budget[:nodes] += 1) > MAX_NODES

      case obj
      when Proc
        scan_proc(obj, path, findings, seen, budget)
      when Hash
        scan_ivars(obj, path, findings, seen, budget)
        obj.each do |key, value|
          walk(key, "#{path} key #{key.inspect}", findings, seen, budget)
          walk(value, "#{path}[#{key.inspect}]", findings, seen, budget)
        end
        report_leaf(obj, path, findings)
      when Array
        scan_ivars(obj, path, findings, seen, budget)
        obj.each_with_index do |value, index|
          walk(value, "#{path}[#{index}]", findings, seen, budget)
        end
        report_leaf(obj, path, findings)
      else
        scan_ivars(obj, path, findings, seen, budget)
        report_leaf(obj, path, findings)
      end
    end

    # A finding is recorded only for leaves: unshareable objects whose
    # innards gave us nothing more specific to point at.
    def report_leaf(obj, path, findings)
      return if obj.instance_variables.any? || obj.is_a?(Proc)
      return if (obj.is_a?(Hash) || obj.is_a?(Array)) && !obj.frozen?

      findings << Finding.new(path: path, message: unshareable_note(obj))
    end

    def scan_ivars(obj, path, findings, seen, budget)
      obj.instance_variables.each do |name|
        value = obj.instance_variable_get(name)
        next if Ractor.shareable?(value)

        findings << Finding.new(
          path: "#{path}.#{name}",
          message: unshareable_note(value)
        )
        walk(value, "#{path}.#{name}", findings, seen, budget)
        break if findings.size >= MAX_FINDINGS
      end
      unless obj.frozen? || obj.is_a?(Proc) || obj.is_a?(Module)
        findings << Finding.new(path: path, message: "#{obj.class} instance is not frozen")
      end
    end

    def scan_proc(proc_obj, path, findings, seen, budget)
      where = proc_obj.source_location&.join(":") || "native"
      binding = begin
        proc_obj.binding
      rescue
        nil
      end
      return unless binding

      receiver = binding.receiver
      unless Ractor.shareable?(receiver)
        findings << Finding.new(
          path: "#{path} (Proc at #{where})",
          message: "self is not shareable: #{brief(receiver)} — use Ractor.shareable_proc"
        )
      end
      binding.local_variables.each do |name|
        value = binding.local_variable_get(name)
        next if Ractor.shareable?(value)

        findings << Finding.new(
          path: "#{path} (Proc at #{where})",
          message: "captures `#{name}` = #{brief(value)} (unshareable)"
        )
        walk(value, "#{path} capture `#{name}`", findings, seen, budget)
        break if findings.size >= MAX_FINDINGS
      end
    end

    def scan_module(mod, path, findings)
      mod.instance_variables.each do |name|
        value = mod.instance_variable_get(name)
        next if Ractor.shareable?(value)

        findings << Finding.new(
          path: "#{path}.#{name}",
          message: "class-level ivar holds #{brief(value)} — classes pass " \
                   "Ractor.shareable?, but reading this from a worker ractor " \
                   "raises Ractor::IsolationError on the first request"
        )
        break if findings.size >= MAX_FINDINGS
      end
    end

    def unshareable_note(obj)
      if obj.frozen?
        "#{brief(obj)} is frozen but holds unshareable contents"
      else
        "#{brief(obj)} is not frozen"
      end
    end

    def brief(obj)
      inspected = obj.inspect
      inspected = "#{inspected[0, 60]}..." if inspected.size > 60
      "#{inspected} (#{obj.class})"
    rescue
      "#<#{obj.class}>"
    end

    private_class_method :dim_hint, :walk, :report_leaf, :scan_ivars,
      :scan_proc, :scan_module, :unshareable_note, :brief
  end
end
