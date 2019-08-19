import Vapor

class BackgrounderDispatcher: BackgrounderHelper {
    
    /// Waits for a job to appear in the queues
    ///
    /// - parameters:
    ///   - queues: The queues to wait for a value for
    ///   - timeout: The number of seconds to timeout
    /// - returns: A future with the queue and job
    ///
    public func dequeue(queues: [String], timeout: Int) -> Future<(String, BackgrounderJob)?> {
        
        // Normalize the queue names
        let lists = queues.map { $0.toRedisQueueName }
        
        // Tries to pop a job out of the queue
        return self.redis.brpop(keys: lists, timeout: timeout).map(to: (String, BackgrounderJob)?.self) { data in
            if let info = data {
                do {
                    let job = try BackgrounderJob.fromRedis(info.1)
                    
                    // Get the queue name and the job
                    return (info.0.fromRedisQueueName, job)
                } catch {
                    print("\(error)")
                }
            }
            return nil
        }
    }
}
