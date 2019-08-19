import Vapor
import RedisApi

/// Structure for storing queue related logic
///
public struct BackgrounderQueue {
   
    /// The name of the queue where scheduled jobs are stored
    static var scheduleQueue: String {
        return "schedule"
    }
    
    /// The name of the queue where error jobs are stored
    static var deadQueue: String {
        return "dead"
    }
    
    /// The name of the queue where retry jobs are stored
    static var retryQueue: String {
        return "retry"
    }
    
    /// The name of the queue instance
    public let name: String
    
    /// The redis name of the queue
    var rName: String {
        return self.name.toRedisQueueName
    }
    
    /// The redis connection object
    let connection: RedisConnection
    
    /// The Redis Client to use to query the queue
    var redis: RedisApi {
        return self.connection.redis
    }
    
    /// The worker
    var worker: Container {
        return self.connection.worker
    }
    
    /// Constructor
    init(name: String, redis connection: RedisConnection) {
        self.name = name
        self.connection = connection
    }
    
    /// Returns all of the known queues
    public static func all(redis connection: RedisConnection) -> Future<[BackgrounderQueue]> {
        return connection.redis.smembers(key: "queues").map { queues in
            return queues.map { BackgrounderQueue(name: $0, redis: connection) }
                .sorted(by: { $0.name < $1.name })  // Return the queues in alphabetical order for repeatibility
        }
    }
    
    /// Returns the size of the queue
    public var size: Future<Int> {
        return self.redis.llen(key: self.rName)
    }
    
    /// Calculates the queues latency
    public var latency: Future<TimeInterval?> {
        return self.redis.lrange(key: self.rName, start: -1, stop: -1)
            .map(to: TimeInterval?.self) { jobs in
                if let firstJob = jobs.first {
                    let job = try BackgrounderJob.fromRedis(firstJob)
                    return job.latency
                }
                return nil
        }
    }
    
    /// Clears the queue
    public func clear() -> Future<Void> {
        return self.redis.multi { conn in
            conn.del(keys: [self.rName])
            conn.srem(key: "queues", members: [self.name])
        }.flatten(on: self.worker).mapToVoid()
    }
}
