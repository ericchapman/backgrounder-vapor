import Vapor

/// Helper class that provides enqueue functionality for moving
/// scheduled jobs to their dispatch queue when they are ready
/// to run
///
class BackgrounderEnqueuer: BackgrounderHelper {
    
    /// Moves the scheduled jobs that are ready from the queue to the
    /// appropriate worker queue
    ///
    /// - parameters:
    ///   - queue: The name of the queue to enqueue from.  The job will
    ///            have the name of the queue to enqueue to
    /// - returns: A Future of the number of jobs that were enqueued
    ///
    /// - note: The number of jobs enqueued is not 100% accurate.  If another
    ///         worker enqueues the job while this worker was trying to, this
    ///         worker will still count it.  It will basically be counted by
    ///         bother workers
    ///
    /// - note: The algorithm for this is designed to operate in parallel
    ///         with other workers performing the same action.  It will
    ///         ensure that another worker has not already enqueued the
    ///         job and it will do its best to minimize the chance of the
    ///         job being lost do to a worker being unexpectably shut down
    ///
    func enqueue(queue: String) -> Future<Int> {
        let promise = self.worker.eventLoop.newPromise(Int.self)
        
        self.enqueueItems(queue: queue) { count in
            promise.succeed(result: count)
        }
        
        return promise.futureResult
    }
    
    /// Function to handle the nesting of a single pop/push operation from
    /// the scheduled queue to the run queue
    ///
    /// - parameters:
    ///   - queue: The scheduled queue to check
    /// - returns: '1' if a job was enqueud, 0 otherwise
    ///
    private func enqueueItem(queue: String) -> Future<Int> {
        // Fetch the first item in the stack
        return self.redis.zrangebyscore(key: queue, min: .min, max: .value(Date().toEpoch), limit: (0,1))
            .flatMap(to: Int.self) { (items: [String]) in
                
                // Get the job
                if items.count > 0 {
                    let data = items[0]
                    
                    // Try to remove the entry before enqueueing.  This is an
                    // adhoc way to create a mutex.  Basically, don't add it
                    // unless it wasn't already removed by another worker
                    return self.redis.zrem(key: queue, members: [data])
                        .flatMap(to: Int.self) { removed in
                        
                        // Make sure we removed it
                        if removed > 0 {
                            do {
                                // Deserialize the job so we can put it in the correct queue
                                let job = try BackgrounderJob.fromRedis(data)
                                job.enqueue()
                                
                                // Push the job to Redis.  Still return 1 since there might be values left
                                let submitter = BackgrounderSubmitter(redis: self.connection)
                                return try submitter.submit(job: job).transform(to: 1)
                            } catch {
                                print("\(error)")
                            }
                        }
                        
                        // If we made it this far, the job was enqueued by another worker.
                        // We will still return 1 to let the caller know it was added, just
                        // not by us
                        return self.eventLoop.future(1)
                    }
                }
                
                // If we made it this far, the job wasn't added
                return self.eventLoop.future(0)
        }
    }
    
    /// Function to handle continuously calling enqueu if more items are
    /// available
    ///
    /// - parameters:
    ///   - queue: The scheduled queue to check
    ///   - count: The current count when entering this method
    ///   - closure: callback with final value when finished
    ///
    private func enqueueItems(queue: String, count: Int=0, closure: @escaping (Int)->()) {
        
        // Iterate until there are no more entries left in the queue
        _ = self.enqueueItem(queue: queue).map(to: Int.self) { enqueued in
            // If a value was enqueued last time, try again
            if enqueued > 0 {
                self.enqueueItems(queue: queue, count: count+1, closure: closure)
            }
            else {
                closure(count)
            }
            
            return enqueued
        }
    }
}
