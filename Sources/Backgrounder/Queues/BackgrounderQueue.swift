import Vapor
import RedisApi

/// Structure for storing queue related logic
///
public class BackgrounderQueue: BackgrounderBaseQueue {
    
    /// Constructor
    ///
    /// - parameters:
    ///   - name: The name of the queue
    ///   - redis: The redis connection
    ///
    public init(name: String, redis conn: RedisConnection) {
        super.init(name: .fromString(name), redis: conn)
    }

    /// Returns the number of jobs in the queue
    ///
    public override var size: Future<Int> {
        return self.redis.llen(key: self.nameKey)
    }
    
    /// Deletes a job from the queue
    ///
    /// - parameters:
    ///   - job: The job to delete
    ///
    /// - returns: 'true' if the job was deleted
    ///
    @discardableResult
    public override func delete(job: BackgrounderJob) -> Future<Bool> {
        return self.redis.lrem(key: self.nameKey, count: 1, value: job.toRedis).map(to: Bool.self) { count in
            return count > 0
        }
    }
    
    /// Returns the latency in the queue
    ///
    public var latency: Future<TimeInterval?> {
        return self.redis.lrange(key: self.nameKey, start: -1, stop: -1)
            .map(to: TimeInterval?.self) { jobs in
                if let firstJob = jobs.first {
                    let job = try BackgrounderJob.fromRedis(firstJob)
                    return job.latency
                }
                return nil
        }
    }
    
    /// Returns then names of all of the queues
    ///
    /// - parameters:
    ///   - redis: The redis connection
    ///
    /// - returns: Array of queues
    ///
    public static func all(redis conn: RedisConnection) -> Future<[BackgrounderQueue]> {
        return conn.redis.smembers(key: "queues").map { queues in
            return queues.map { BackgrounderQueue(name: $0, redis: conn) }
                .sorted(by: { $0.name < $1.name })  // Return the queues in alphabetical order for repeatibility
        }
    }
    
    /// Clears the queue
    ///
    public func clear() -> Future<Void> {
        return self.redis.multi { conn in
            conn.del(keys: [self.nameKey])
            conn.srem(key: "queues", members: [self.name])
            }.flatten(on: self.worker).mapToVoid()
    }

    /// Submits jobs to the queue
    ///
    /// - note: The "runAt" is ignored here so if the job was intended to be
    ///   schedule, it will not be
    ///
    /// - parameters:
    ///   - jobs: The jobs to push
    ///   - redis: An optional redis API object.  This is used if wrapping this
    ///     in a multi-block
    ///
    /// - returns: The number of jobs in the queue
    ///
    @discardableResult
    func push(jobs: [BackgrounderJob], redis: RedisApi?=nil) -> Future<Int> {
        // Set all of the jobs to enqueued
        jobs.forEach { $0.enqueue() }
        
        return self.push(jobs: jobs.map { $0.toRedis }, redis: redis)
    }
    
    /// Submits jobs to the queue
    ///
    /// - note: The "runAt" is ignored here so if the job was intended to be
    ///   schedule, it will not be
    ///
    /// - parameters:
    ///   - jobs: The jobs to push as strings
    ///   - redis: An optional redis API object.  This is used if wrapping this
    ///     in a multi-block
    ///
    /// - returns: The number of jobs in the queue
    ///
    @discardableResult
    func push(jobs: [String], redis: RedisApi?=nil) -> Future<Int> {
        let redis = redis ?? self.redis
        
        let promise = self.eventLoop.newPromise(of: Int.self)
        
        // Use a multi block to add the queue to the list while submitting the job
        redis.multi { multi in
            multi.sadd(key: "queues", members: [self.name])
            multi.lpush(key: self.nameKey, values: jobs).do { count in
                promise.succeed(result: count)
                }.catch { error in promise.fail(error: error) }
        }
        
        return promise.futureResult
    }

    /// Submits jobs to the different queues
    ///
    /// - note: This method will check the "runAt" and automatically schedule
    ///   the jobs if it is in the future
    ///
    /// - parameters:
    ///   - redis: Redis connection object
    ///   - jobs: The jobs to push
    ///   - queue: Optional queue name to submit all jobs to a specific queue
    ///
    /// - returns: The number of jobs in the queue
    ///
    @discardableResult
    static func push(redis conn: RedisConnection, jobs: [BackgrounderJob], queue: String?=nil) -> Future<Int> {
        
        var scheduledJobs = [BackgrounderJob]()
        let groups: Dictionary<String, [BackgrounderJob]>
        
        if let queueOverride = queue {
            groups = [queueOverride:jobs]
        }
        else {
            // Sort jobs into normal/scheduled buckets
            var normalJobs = [BackgrounderJob]()
            let now = Date()
            for job in jobs {
                if job.runAt != nil && job.runAt! > now {
                    scheduledJobs.append(job)
                }
                else {
                    job.enqueue()
                    normalJobs.append(job)
                }
            }
            
            // Group the jobs normal by queue
            groups = Dictionary(grouping: normalJobs, by: { $0.queue })
        }
        
        // Variables that will allow us to extract the queue totals from the response
        var totals = [Future<Int>]()
        
        // Create a redis multi for submission
        conn.redis.multi { multi in
            // Iterate through the groups of jobs per queue and submit
            for (queueName, jobs) in groups {
                let queue = BackgrounderQueue(name: queueName, redis: conn)
                totals.append(queue.push(jobs: jobs, redis: multi))
            }
            
            // If there are shceduled jobs, submit those as well
            if scheduledJobs.count > 0 {
                let queue = BackgrounderScheduleQueue(redis: conn)
                totals.append(queue.push(jobs: jobs, redis: multi))
            }}
        
        // Return the flattned totals that we got
        return totals.flatten(on: conn.worker).map(to: Int.self, { (resp: [Int]) -> Int in
            return resp.reduce(0, +)
        })
    }
    
    /// Pops the next ready job from the specified queues
    ///
    /// - parameters:
    ///   - redis: The redis connection object
    ///   - queues: The queues to check for new values in
    ///   - timeout: Number of seconsd to wait before timing out
    ///
    /// - returns: The job that was popped and which queue it was popped from
    ///
    static func popNext(redis conn: RedisConnection, queues: [String], timeout: Int) -> Future<(BackgrounderJob, String)?> {
        // Normalize the queue names
        let lists = queues.map { QueueName.fromString($0).toQueueKey }
        
        // Tries to pop a job out of the queues
        return conn.redis.brpop(keys: lists, timeout: timeout).map(to: (BackgrounderJob, String)?.self) { data in
            if let info = data {
                // Return the name of the job and the queue it was popped from
                let job = try BackgrounderJob.fromRedis(info.1)
                let queue = QueueName.fromString(info.0).toQueueName
                return (job, queue)
            }
            return nil
        }
    }
}
