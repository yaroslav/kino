# frozen_string_literal: true

# Minimal Rails (edge) app for Kino's :ractor mode; config.ru calls HelloApp.ractorize!.
# Ractor-safe settings follow https://github.com/Shopify/ractor-safe-rails
ENV["RAILS_ENV"] ||= "production" # the ractorized app only boots in production
require "bundler/setup"
require "rails"
require "active_record/railtie"
require "action_controller/railtie"
require "fileutils"

# No database.yml in a single file. max_connections must cover workers x threads.
# No busy timeout: Rails runs all Ractor DB calls on one main-Ractor thread, so a wait stalls every worker.
FileUtils.mkdir_p(File.expand_path("storage", __dir__))
ENV["DATABASE_URL"] = "sqlite3:#{File.expand_path("storage/hello.sqlite3", __dir__)}?max_connections=64"

# Rails makes its boot-time procs shareable only when this is set.
ActiveSupport::Ractors.unshareable_proc_action = :raise

# Defined before initialize!: the eager-loading boot looks up each route's controller.
class HelloController < ActionController::Base
  def index
    render plain: "Hello from Rails #{Rails.version}"
  end
end

class PostsController < ActionController::Base
  skip_forgery_protection

  def index
    posts = Post.order(id: :desc).limit(10)
    render_json({count: Post.count, posts: posts.as_json(only: %i[id body])})
  end

  def create
    post = Post.new(body: params.require(:body))
    if post.save
      render_json(post.as_json(only: %i[id body]), status: :created)
    else
      render_json({errors: post.errors.full_messages}, status: :unprocessable_content)
    end
  end

  private

  # Not `render json:`: that renderer is an unshareable block, uncallable from workers.
  def render_json(payload, status: :ok)
    render body: payload.to_json, content_type: "application/json", status: status
  end
end

class Post < ActiveRecord::Base
  validates :body, length: {maximum: 280}
end

class HelloApp < Rails::Application
  config.load_defaults Rails::VERSION::STRING.to_f.floor(1)
  config.eager_load = true
  config.enable_reloading = false
  config.consider_all_requests_local = false
  config.secret_key_base = "kino-rails-hello-not-a-secret"
  config.hosts.clear

  # A plain Logger holds a Monitor, which ractorize! cannot make shareable.
  config.logger = ActiveSupport::TaggedLogging.ractor_logger($stdout)
  config.public_file_server.enabled = false
  config.cache_store = :null_store
  # Keeps I18n.fallbacks off a class variable that worker Ractors cannot read.
  config.i18n.fallbacks = true

  # Appended: boot freezes the route set, so a routes.draw after initialize! fails.
  routes.append do
    root to: "hello#index"
    resources :posts, only: %i[index create]
  end
end

HelloApp.initialize!

ActiveRecord::Base.connection_pool.with_connection do |connection|
  connection.create_table(:posts, if_not_exists: true) do |t|
    t.string :body, null: false
    t.timestamps
  end
end
Post.create!(["First post", "Second post", "Third post"].map { {body: it} }) if Post.none?
# In the main Ractor even when nothing was seeded; a worker cannot define them.
Post.define_attribute_methods

# The gem-path filter (String#sub in a shareable proc) crashes Ruby 4.0.9 on concurrent errors.
Rails.backtrace_cleaner.remove_filters!
