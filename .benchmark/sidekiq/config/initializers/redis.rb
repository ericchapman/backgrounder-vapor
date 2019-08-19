require 'connection_pool'

BENCHMARK_REDIS = ConnectionPool.new(size: 10) {
  Redis.new(url: "redis://127.0.0.1:6379/0")
}
