module Backgrounder
  class BenchmarkHandler
    include Sidekiq::Worker
    def perform(*args)
      BENCHMARK_REDIS.with do |redis|
        future = nil
        redis.multi do |conn|
          tempId = SecureRandom.hex(5)
          conn.set(tempId, 10)
          conn.incrby(tempId, 5)
          future = conn.get(tempId)
          conn.del(tempId)
        end

        if future.value != "15"
          puts "error, expected 15, gpt #{future.value}"
        end
      end
    end
  end
end
