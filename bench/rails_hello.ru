# frozen_string_literal: true

# Frozen subject of the published Rails numbers (examples/rails-hello has since moved to Ractor mode).
require "rails"
require "action_controller/railtie"

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
  config.logger = ActiveSupport::Logger.new($stdout)
  config.hosts.clear
end

HelloApp.initialize!

HelloApp.routes.draw do
  root to: "hello#index"
end

run HelloApp
