# Contributing

Install dependencies:

```sh
bundle install
```

Run the full local check:

```sh
bundle exec rake
```

Clean generated build and test artifacts:

```sh
rm -rf tmp target lib/kino/kino.so .rspec_status
```

`lib/kino/kino.so`, `tmp/`, `target/`, and `.rspec_status` are generated
locally and ignored by git.
