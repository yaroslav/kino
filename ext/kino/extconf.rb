# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("kino/kino")

# Ruby's mkmf emits `ECHO = ... @ echo`.
# Under `bundle exec`, RubyGems' bin directory can sit before system paths;
# an unrelated gem executable named `echo` then hijacks native-extension
# install status lines. Use the system command directly,
# keeping the generated Makefile independent of PATH collisions.
makefile = File.read("Makefile")
makefile.sub!(/^ECHO = \$\(ECHO1:0=@ echo\)$/, "ECHO = $(ECHO1:0=@ /bin/echo)")
File.write("Makefile", makefile)
