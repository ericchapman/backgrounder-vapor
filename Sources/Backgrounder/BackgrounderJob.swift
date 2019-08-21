import Vapor

/// The Details about the Job
///
public class BackgrounderJob {
    
    /// The job id
    public private(set) var id: String
    
    /// The time the job was created
    public private(set) var createdAt: Date
    
    /// The time the job was enqueued
    public private(set) var enqueuedAt: Date?
    
    /// The name of the class
    public private(set) var className: String
    
    /// The queue to submit the job to
    public private(set) var queue: String
    
    /// The arguments for the job
    public private(set) var args: [Any]
    
    /// The time the job should run
    public var runAt: Date? { didSet { self.redisData = nil } }
    
    /// The error message
    public private(set) var errorMessage: String? { didSet { self.redisData = nil } }

    /// The error class
    public private(set) var errorClass: String? { didSet { self.redisData = nil } }
    
    /// The time the job last failed
    public private(set) var failedAt: Date?
    
    /// True if the job should retry
    public private(set) var retry: Bool { didSet { self.redisData = nil } }
    
    /// The number of times the job was retried
    public private(set) var retryCount: Int=0 { didSet { self.redisData = nil } }
    
    /// The time the job was last retried
    public private(set) var retriedAt: Date?
    
    /// Serialed version of the job
    public var redisData: String?
    
    /// Calculates the queues latency
    public var latency: TimeInterval {
        let now = Date()
        return now.toEpoch-(self.enqueuedAt ?? now).toEpoch
    }
    
    /// Constructor
    init(className: String, args: [Any], id: String?=nil, queue: String?=nil, runAt: Date?=nil, retry: Bool?=nil) {
        self.className = className
        self.id = id ?? UUID().JID
        self.createdAt = Date()
        self.queue = queue ?? "default"
        self.args = args
        self.runAt = runAt
        self.retry = retry ?? false
    }
    
    /// Instantiates the handler class from the name
    var handlerClass: AnyClass? {
        return self.className.toClass
    }
    
    /// Method to record an error ocurred
    ///
    /// - parameters:
    ///   - error: The error to capture with the errored job
    ///
    func recordError(_ error: Error) {
        self.failedAt = Date()
        self.errorMessage = "\(error)"
        self.errorClass = String(describing: error)
    }
    
    /// Method to request a retry.  It will signal if the count is
    /// still lower than the max and increment the tries
    ///
    /// - parameters:
    ///   - max: The maximum number of retries
    /// - returns: true is we should retry
    ///
    func shouldRetry(max: Int) -> Bool {
        if self.retry {
            self.retryCount += 1
            self.retriedAt = Date()
            return self.retryCount <= max
        }
        return false
    }
    
    /// Marks the job as enqueud
    ///
    func enqueue() {
        self.runAt = nil
        self.enqueuedAt = Date()
    }
    
    /// Function to submit a job
    ///
    /// - parameters:
    ///   - worker: The worker to use to remove th job
    ///
    /// - returns: 'true' if the job was submitted
    ///
    @discardableResult
    func submit(on worker: Container) -> Future<Bool> {
        return RedisPooledConnection.openWithAutoClose(on: worker, as: .backgrounderRedis, closure: {
            (connection: RedisConnection) -> Future<Bool> in
            return BackgrounderQueue.push(redis: connection, jobs: [self]).map(to: Bool.self) { count in
                return count > 0
            }
        })
    }
    
    /// Function to delete the job
    ///
    /// - parameters:
    ///   - worker: The worker to use to remove th job
    /// - returns: Future representing true if this was deleted
    ///
    @discardableResult
    public func delete(on worker: Container) -> Future<Bool> {
        return RedisPooledConnection.openWithAutoClose(on: worker, as: .backgrounderRedis, closure: {
            (connection: RedisConnection) -> Future<Bool> in
            return BackgrounderQueue(name: self.queue, redis: connection).delete(job: self)
        })
    }
    
    /// Function to delete the scheduled job
    ///
    /// - parameters:
    ///   - worker: The worker to use to remove th job
    /// - returns: Future representing true if this was deleted
    ///
    @discardableResult
    public func deleteScheduled(on worker: Container) -> Future<Bool> {
        return RedisPooledConnection.openWithAutoClose(on: worker, as: .backgrounderRedis, closure: {
            (connection: RedisConnection) -> Future<Bool> in
            return BackgrounderScheduleQueue(redis: connection).delete(job: self)
        })
    }
}

extension Array where Element: BackgrounderJob {
    
    /// Function to submit multiple jobs
    ///
    /// - parameters:
    ///   - worker: The worker to use to remove th job
    ///
    /// - returns: 'true' if the jobs were submitted
    ///
    @discardableResult
    func submit(on worker: Container) -> Future<Bool> {
        return RedisPooledConnection.openWithAutoClose(on: worker, as: .backgrounderRedis, closure: {
            (connection: RedisConnection) -> Future<Bool> in
            return BackgrounderQueue.push(redis: connection, jobs: self).map(to: Bool.self) { count in
                return count == self.count
            }
        })
    }
}

extension BackgrounderJob: RedisObject {
    
    /// Converte the job to a hash that can be serialized
    ///
    public func toHash() -> [String:Any] {
        var hash: [String:Any] = [
            "jid": self.id,
            "created_at": self.createdAt.toEpoch,
            "queue": self.queue,
            "class": self.className.toRuby,
            "args": self.args,
            "retry": self.retry,
            "retry_count": self.retryCount 
        ]
        
        if let enqueuedAt = self.enqueuedAt?.toEpoch { hash["enqueued_at"] = enqueuedAt }
        if let retriedAt = self.retriedAt?.toEpoch { hash["retried_at"] = retriedAt }
        
        // Error Messages
        if let failedAt = self.failedAt?.toEpoch { hash["failed_at"] = failedAt }
        if let errorMessage = self.errorMessage { hash["error_message"] = errorMessage }
        if let errorClass = self.errorClass { hash["error_class"] = errorClass.toRuby }
        
        return hash
    }
    
    /// Converte the job to a hash that can be serialized
    ///
    public static func fromHash(hash: [String:Any]) -> BackgrounderJob? {
        if let id = hash["jid"] as? String,
            let className = hash["class"] as? String,
            let queue = hash["queue"] as? String {
            
            let job = BackgrounderJob(
                className: className.fromRuby,
                args: (hash["args"] as? [Any]) ?? [],
                id: id,
                queue: queue,
                retry: (hash["retry"] as? Bool))
            
            job.createdAt = (hash["created_at"] as? TimeInterval)?.fromEpoch ?? Date()
            job.enqueuedAt = (hash["enqueued_at"] as? TimeInterval)?.fromEpoch ?? Date()
            
            job.errorMessage = hash["error_message"] as? String
            job.errorClass = (hash["error_class"] as? String)?.fromRuby
            job.failedAt = (hash["failed_at"] as? TimeInterval)?.fromEpoch
            
            job.retryCount = hash["retry_count"] as? Int ?? 0
            job.retriedAt = (hash["retried_at"] as? TimeInterval)?.fromEpoch
            
            return job
        }
        
        return nil
    }
}
