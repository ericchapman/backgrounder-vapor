import Service

extension EventLoop {

    /// Create a timer-like method that delays execution some
    /// amount of time
    ///
    /// - note: This basically wraps the schedule task to return a promise
    ///
    public func execute(in delay: TimeAmount, _ task: @escaping ()->()) -> Future<Void> {
        let promise = self.newPromise(Void.self)
        
        self.scheduleTask(in: delay) {
            // Execute the task
            task()
            
            // Fulfill the promise
            promise.succeed()
        }
        
        return promise.futureResult
    }
}
