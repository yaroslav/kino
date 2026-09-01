# Puma 8 preloads by default in cluster mode, and the CLI has --preload
# but no flag to turn it off. The NOpreload bench case loads this file to
# keep measuring a genuinely non-preloaded cluster.
preload_app! false
