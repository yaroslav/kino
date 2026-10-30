# frozen_string_literal: true

require_relative "app"

HelloApp.ractorize!
# Lets worker Ractors reach I18n's backend (validation messages, I18n.t).
require "i18n/ractorize"

# The instance is shareable; the class keeps unshareable ivars (see kino --check).
run HelloApp.instance
