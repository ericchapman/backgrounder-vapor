import Vapor
import RedisApi

/// Class to handle pushing jobs to the worker
///
class BackgrounderSubmitter: BackgrounderHelper {
    
    /// Submits a job to the workers using an existing redis API
    ///
    /// - parameters:
    ///   - redis: Redis API object
    ///   - jobStrings: The jobs to push as strings
    ///   - queue: The queue to submit to
    /// - returns: A Future with the number of jobs in the queue
    ///
    @discardableResult
    static func submit(redis: RedisApi, jobStrings: [String], queue: String) -> Future<Int> {
        return redis.lpush(key: queue, values: jobStrings)
    }
    
    /// Submits a job to the workers
    ///
    /// - parameters:
    ///   - job: The job to push
    ///   - queue: The queue to submit to (overrides job one)
    /// - returns: A Future with the number of jobs in the queue
    ///
    @discardableResult
    func submit(job: BackgrounderJob, queue: String?=nil) throws -> Future<Int> {
        return try self.submit(jobs: [job], queue: queue)
    }    
    
    /// Deletes a submitted job
    ///
    /// - parameters:
    ///   - job: The job to delete
    ///   - queue: The queue to delete it from
    /// - returns: A Future with the number of jobs removed
    ///
    @discardableResult
    func delete(job: BackgrounderJob, queue: String?=nil) -> Future<Int> {
        let queueName = (queue ?? job.queue).toRedisQueueName
        return self.redis.lrem(key: queueName, count: 1, value: job.toRedis)
    }
    
    /// Submits multiple jobs to the workers
    ///
    /// - parameters:
    ///   - jobs: The jobs to push
    ///   - queue: The queue to submit to (overrides job one)
    /// - returns: A Future with the number of jobs in the queue(s)
    ///
    @discardableResult
    func submit(jobs: [BackgrounderJob], queue: String?=nil) throws -> Future<Int> {
        
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
         self.redis.multi { conn in
            // Iterate through the groups of jobs per queue and submit
            for (queue, jobs) in groups {
                conn.sadd(key: "queues", members: [queue])
                totals.append(type(of: self).submit(redis: conn, jobStrings: jobs.map { $0.toRedis }, queue: queue.toRedisQueueName))
            }
            
            // If there are shceduled jobs, submit those as well
            if scheduledJobs.count > 0 {
                totals.append(BackgrounderScheduler.schedule(
                    multi: conn,
                    jobs: scheduledJobs,
                    queue: BackgrounderQueue.scheduleQueue))
            }}
        
        // Return the flattned totals that we got
        return totals.flatten(on: self.worker).map(to: Int.self, { (resp: [Int]) -> Int in
            return resp.reduce(0, +)
        })
    }
}
