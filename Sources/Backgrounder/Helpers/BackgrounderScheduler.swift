import Vapor
import RedisApi

/// Class to handle pushing jobs to the worker
///
class BackgrounderScheduler: BackgrounderHelper {
    
    /// Schedules a job to be executed later
    ///
    /// - parameters:
    ///   - job: The job to schedule
    ///   - queue: The name of the queue that holds the scheduled jobs
    ///   - runAt: The time to run at.  It will override
    ///            the ones in the jobs
    ///
    /// - returns: The number of jobs added to the queue
    ///
    func schedule(job: BackgrounderJob, queue: String, runAt: Date?=nil) -> Future<Int> {
        return self.schedule(jobs: [job], queue: queue, runAt: runAt)
    }
    
    /// Deletes a scheduled job
    ///
    /// - parameters:
    ///   - job: The job to remove from the enqueue queue
    ///   - queue: The name of the queue to remove it from
    ///
    /// - returns: The number of jobs removed from the queue
    ///
    func delete(job: BackgrounderJob, queue: String) -> Future<Int> {
        return self.redis.zrem(key: queue, members: [job.toRedis])
    }

    /// Schedules multiple jobs to be executed later
    ///
    /// - note: This method will still schedule the job, even if runAt
    ///         is in the past
    ///
    /// - parameters:
    ///   - jobs: The jobs to schedule
    ///   - queue: The name of the queue that holds the scheduled jobs
    ///   - runAt: The time to run at.  It will override
    ///            the ones in the jobs
    ///
    /// - returns: The number of jobs added to the queue
    ///
    func schedule(jobs: [BackgrounderJob], queue: String, runAt: Date?=nil) -> Future<Int> {
        // Convert the jobs to score, values tuples
        let now = Date()
        let items = jobs.map { (job: BackgrounderJob) -> (Double, String) in
            // Override time takes priority, then job time.
            let time = runAt ?? job.runAt ?? now
            return (Double(time.toEpoch), job.toRedis)
        }
        
        // Push the jobs to Redis
        return self.redis.zadd(key: queue, scoreMemberPairs: items)
    }
    
    /// Schedules jobs using a multi-call
    ///
    /// - note: This method will still schedule the job, even if runAt
    ///         is in the past
    ///
    /// - parameters:
    ///   - multi: A redis multi handle
    ///   - jobs: The jobs to schedule
    ///   - queue: The name of the queue that holds the scheduled jobs
    ///   - runAt: The time to run at.  It will override
    ///            the ones in the jobs
    ///
    /// - returns: The number of jobs added to the queue
    ///
    static func schedule(multi: RedisApi, jobs: [BackgrounderJob], queue: String, runAt: Date?=nil) -> Future<Int> {
        // Convert the jobs to score, values tuples
        let now = Date()
        var args = [(Double, String)]()
        jobs.forEach { job in
            // Override time takes priority, then job time.
            let time = runAt ?? job.runAt ?? now
            args.append((Double(time.toEpoch), job.toRedis))
        }
        
        // Push the jobs to Redis
        return multi.zadd(key: queue, scoreMemberPairs: args)
    }
}
