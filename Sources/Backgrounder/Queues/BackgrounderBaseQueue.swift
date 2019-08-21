import Vapor
import RedisApi

/// Structure for storing queue related logic
///
public class BackgrounderBaseQueue {
    
    /// Enumeration for the queue names
    enum QueueName {
        case schedule
        case dead
        case retry
        case queue(name: String)
        
        /// Returns the name of the queue
        var toQueueName: String {
            switch self {
            case .schedule: return "schedule"
            case .dead: return "dead"
            case .retry: return "retry"
            case .queue(let name): return name
            }
        }
        
        /// Returns the name of the queue as a redis key
        var toQueueKey: String {
            switch self {
            case .schedule: return "schedule"
            case .dead: return "dead"
            case .retry: return "retry"
            case .queue(let name): return "queue:\(name)"
            }
        }
        
        /// Creates the enum from the string
        static func fromString(_ string: String) -> QueueName {
            switch string {
            case "schedule": return .schedule
            case "dead": return .dead
            case "retry": return .retry
            default: return .queue(name: string.replacingOccurrences(of: "queue:", with: ""))
            }
        }
    }
    
    /// The name of the queue instance
    public var name: String { return self.queueName.toQueueName }
    
    /// The name of the queue key in redis
    var nameKey: String { return self.queueName.toQueueKey }
    
    /// The queue name parametere
    let queueName: QueueName
    
    /// The redis connection object
    let conn: RedisConnection
    
    /// The Redis Client to use to query the queue
    var redis: RedisApi { return self.conn.redis }
    
    /// The worker
    var worker: Container { return self.conn.worker }
    
    /// The event loop
    var eventLoop: EventLoop { return self.worker.eventLoop }
    
    /// Initializer
    init(name: QueueName, redis conn: RedisConnection) {
        self.queueName = name
        self.conn = conn
    }

    /// Returns the size of the queue
    ///
    /// - note: Override in subclass
    ///
    public var size: Future<Int> {
        return self.eventLoop.newSucceededFuture(result: 0)
    }

    /// Deletes a job from the queue
    ///
    /// - parameters:
    ///   - job: The job to remove from the queue
    ///
    /// - returns: 'true' if the job was deleted
    ///
    @discardableResult
    public func delete(job: BackgrounderJob) -> Future<Bool> {
        return self.eventLoop.newSucceededFuture(result: false)
    }
}
