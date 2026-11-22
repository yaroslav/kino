# frozen_string_literal: true

# Examples that run in both worker modes.
module EachMode
  # The block's examples once per mode, in a context that names it.
  def each_mode(&examples)
    %i[ractor threaded].each do |mode|
      context("in #{mode} mode") { class_exec(mode, &examples) }
    end
  end
end

RSpec.configure do |config|
  config.extend EachMode
end
