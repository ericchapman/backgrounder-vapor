import Vapor
import RedisApi

/// Structure for storing queue related logic
///
public class BackgrounderSortedSetQueue: BackgrounderBaseQueue {

    /// Returns the size of the queue
    ///
    public override var size: Future<Int> {
        return self.redis.zcount(key: self.nameKey, min: .min, max: .max)
    }
    
    /// Deletes a job
    ///
    /// - parameters:
    ///   - job: The job to remove from the queue
    ///
    /// - returns: 'true' if the job was deleted
    ///
    @discardableResult
    public override func delete(job: BackgrounderJob) -> Future<Bool> {
        return self.redis.zrem(key: self.nameKey, members: [job.toRedis]).map(to: Bool.self) { count in
            return count > 0
        }
    }
    
    /// Pushes a job to the queue
    ///
    /// - parameters:
    ///   - jobs: The jobs to psuh to the queue
    ///   - runAt: An optional date used to specify the "run at" time for all jobs
    ///   - redis: An optional redis API object.  This is used if wrapping this
    ///     in a multi-block
    ///
    /// - returns: The number of jobs in the queue
    ///
    @discardableResult
    func push(jobs: [BackgrounderJob], runAt: Date?=nil, redis: RedisApi?=nil) -> Future<Int> {
        
        // Convert the jobs to score, values tuples
        let now = Date()
        let items = jobs.map { (job: BackgrounderJob) -> (Date, BackgrounderJob) in
            return (runAt ?? job.runAt ?? now, job)
        }
        
        // Push the jobs to Redis
        return self.push(jobs: items, redis: redis)
    }
    
    /// Pushes a job/time pairs to the queue
    ///
    /// - parameters:
    ///   - jobs: The jobs to psuh to the queue
    ///   - redis: An optional redis API object.  This is used if wrapping this
    ///     in a multi-block
    ///
    /// - returns: The number of jobs in the queue
    ///
    @discardableResult
    func push(jobs: [(Date, BackgrounderJob)], redis: RedisApi?=nil) -> Future<Int> {
        let redis = redis ?? self.redis
        
        // Convert the jobs to score, values tuples
        let items = jobs.map { time, job in
            return (Double(time.toEpoch), job.toRedis)
        }
        
        // Push the jobs to Redis
        return redis.zadd(key: self.nameKey, scoreMemberPairs: items)
    }
 
    /// Pops the next ready job from the queue
    ///
    /// - parameters:
    ///   - since: The date where any jobs that are older than are returned
    ///
    /// - returns: The job that was popped
    ///
    func popNext(since: Date?=nil) -> Future<BackgrounderJob?> {
        let date = since ?? Date()
        
        // Fetch the first item in the stack
        return self.redis.zrangebyscore(key: self.nameKey, min: .min, max: .value(date.toEpoch), limit: (0,1))
            .flatMap(to: BackgrounderJob?.self) { (items: [String]) in
                
                // If there is a job, start the return logic
                if let data = items.first {
                    
                    // Try to remove the entry before enqueueing.  This is an
                    // adhoc way to create a mutex.  Basically, don't add it
                    // unless it wasn't already removed by another worker
                    return self.redis.zrem(key: self.nameKey, members: [data]).map(to: BackgrounderJob?.self) { removed in
                        
                        if removed > 0 {
                            return try BackgrounderJob.fromRedis(data)
                        }
                        return nil
                    }
                }
                
                // If we made it this far then no jobs were popped
                return self.eventLoop.newSucceededFuture(result: nil)
        }
    }
    
    /// Continuosly pops ready jobs until there are none left in the queue
    ///
    /// - parameters:
    ///   - since: The date where any jobs that are older than are returned
    ///   - callback: Calls back every time a new job is popped
    ///
    /// - returns: The number of items that were popped from the queue
    ///
    func popAll(since: Date?=nil, callback: @escaping (BackgrounderJob) -> Future<Void>) -> Future<Int> {
        let promise = self.eventLoop.newPromise(of: Int.self)
        
        var count = 0
        
        // Local function to wrap calling pop
        func singlePop() {
            _ = self.eventLoop.submit {
                self.popNext().do { job in
                    
                    // If the job is nil, then we are done
                    guard let job = job else {
                        promise.succeed(result: count)
                        return
                    }
                    
                    // Increment the counter
                    count += 1
                    
                    // If we made it this far, return the job and call the method again
                    callback(job).do { singlePop() }.catch { error in promise.fail(error: error) }
                    
                }.catch { error in promise.fail(error: error) }
            }
        }
        
        // Start popping the jobs
        singlePop()
        
        return promise.futureResult
    }
}

/// Creates a handle to the 'schedule' queue
///
public class BackgrounderScheduleQueue: BackgrounderSortedSetQueue {
    
    /// Constructor
    ///
    public init(redis conn: RedisConnection) {
        super.init(name: .schedule, redis: conn)
    }
}

/// Creates a handle to the 'retry' queue
///
public class BackgrounderRetryQueue: BackgrounderSortedSetQueue {
    
    /// Constructor
    ///
    public init(redis conn: RedisConnection) {
        super.init(name: .retry, redis: conn)
    }
}

/// Creates a handle to the 'dead' queue
///
public class BackgrounderDeadQueue: BackgrounderSortedSetQueue {
    
    /// Constructor
    ///
    public init(redis conn: RedisConnection) {
        super.init(name: .dead, redis: conn)
    }
}
