# frozen_string_literal: true

# Minimal Rails (edge) app: railties + action_controller, no Active Record,
# no reloading; the most Ractor-friendly configuration Rails offers today.
require "rails"
require "action_controller/railtie"
require "fileutils"

# Rails' default logger writes to log/<env>.log; make sure the directory
# exists so Rails doesn't fall back to a stderr logger with a warning.
FileUtils.mkdir_p(File.expand_path("log", __dir__))

class HelloController < ActionController::Base
  def index
    render plain: "Hello from Rails #{Rails.version}"
  end
end

class HelloApp < Rails::Application
  config.load_defaults Rails::VERSION::STRING.to_f.floor(1)
  config.eager_load = true
  config.enable_reloading = false
  config.consider_all_requests_local = false
  config.secret_key_base = "kino-rails-hello-not-a-secret"

  # Request log to BOTH log/<env>.log and stdout (a single-file app has no
  # config/environments/production.rb, which is where the usual
  # RAILS_LOG_TO_STDOUT handling lives, so broadcast explicitly, like the
  # generated template does). Kino's own lifecycle output is separate.
  config.logger = ActiveSupport::BroadcastLogger.new(
    ActiveSupport::Logger.new(File.expand_path("log/#{ENV.fetch("RACK_ENV", "production")}.log", __dir__)),
    ActiveSupport::Logger.new($stdout)
  )
  config.hosts.clear
end

HelloApp.initialize!

HelloApp.routes.draw do
  root to: "hello#index"
end
