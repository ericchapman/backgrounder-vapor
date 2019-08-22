import Vapor
import RedisApi

class BackgrounderWorker {
    
    /// The Worker is the the main job executor in each thread.  It has the
    /// following tasks
    ///
    ///  - Start
    ///  - Loop Until Exit
    ///    -
    
    /// The worker this process is executing on
    let container: Container
    
    /// The event loop for the worker
    var eventLoop: EventLoop {
        return self.container.eventLoop
    }
    
    /// The ID of the worker
    let id: String

    /// The logger for the worker
    let logger: Logger
    
    /// The config for the worker
    let config: BackgrounderConfig
    
    /// The redis client
    var connection: RedisConnection?
    
    /// The promise that this process will fulfill on exit
    let promise: Promise<String>
    
    /// Flag signalling if we are polling Redis
    var isPollingRedis = false
    
    /// Flag signalling if the process should exit
    var shouldExit = false
    
    /// Flag signalling if the process should "quiet" which will
    /// stop it from looking for new jobs
    var shouldQuiet = false { didSet {
        if self.shouldQuiet == oldValue { return }
        
        // If the process turned quiet off, start the next iteration
        // if there are no other jobs running and we aren't already
        // polling on Redis
        if !self.shouldQuiet && self.runningJobsCount == 0 && !self.isPollingRedis {
            self.nextIteration()
        }
        } }
    
    /// The jobs that are currently running (ID -> JOB) pair
    var runningJobs: [String:(BackgrounderJob, Date)] = [:]
    
    /// Jobs Count
    var runningJobsCount: Int { return self.runningJobs.count }
    
    /// Processed counter
    var processedCount = BackgrounderCounter()
    
    /// Failed counter
    var failedCount = BackgrounderCounter()
    
    /// Errors that need to be pushed to the error queue
    var errors: [BackgrounderJob] = []
    
    /// Retries that need to be pushed to the retry queue
    var retries: [BackgrounderJob] = []
    
    /// Time for the next maintenance
    var maintenanceAt: Date
    
    /// Signalling that we are currently establishing a connection
    var establishingConnection: Bool = false
    
    /// The maximum number of jobs to execute in parallel
    var maxJobs: Int {
        return self.config.maxJobsPerWorker
    }
    
    /// Errors
    enum WorkerError: Error {
        case couldNotInstantiateHandler(className: String)
        case jobTimeoutReached(seconds: Int)
    }
    
    /// Constructor
    init(on container: Container, config: BackgrounderConfig) {
        self.container = container
        self.id = UUID().TID
        self.config = config
        self.promise = container.eventLoop.newPromise(String.self)
        self.maintenanceAt = Date.random(in: self.config.maintenanceInterval)
        
        self.logger = BackgrounderLogger(
            level: self.config.logLevel,
            prefix: "\(ProcessInfo.processInfo.processIdentifier) TID-\(self.id)",
            detailed: self.config.detailedLogging)
    }
    
    /// Stops the process
    ///
    func stop() {
        self.logger.debug("received stop request")
        
        // Kill the health check
        self.healthCheckTask?.cancel()
        self.healthCheckTask = nil
        
        self.shouldExit = true
        
        // If we were quiet, kick off the "confirm stop" task manually
        if self.shouldQuiet {
            self.confirmStop()
        }
    }
    
    /// Method that starts the process
    ///
    /// - returns: Future with the worker ID
    func start() -> Future<String> {
        self.logger.debug("started")
        
        // Start the connection establishment logic
        self.connectionEntry()
        
        return self.promise.futureResult
    }
    
    /// Wait for a Job from the queues
    ///
    /// - returns: Returns a future with the next job from the queue
    ///
    private func nextIteration() {
        
        // Logic to make sure we don't re-enter this logic when
        // establishing a new connection of if we are already polling redis
        if self.establishingConnection == true || self.isPollingRedis { return }
        
        self.isPollingRedis = true
        
        // Run every iteration items first before checking for new items
        self.everyIteration {
            
            // If we should exit, call confirm stop and exit.  We don't want
            // to pop any more items
            if self.shouldExit || self.shouldQuiet {
                if self.shouldExit {
                    // Call the confirm stop method
                    self.confirmStop()
                }
                
                self.isPollingRedis = false
                return
            }
            
            self.logger.verbose("start queue check")
            
            // Generate a new list of queues.  This is to support weighted queues
            // where the order needs to change every time
            let queues = self.config.queues.generate()
            
            // Make sure the Redis Connection exists
            guard let connection = self.connection else { return }
            
            // Call the qeque method
            BackgrounderQueue.popNext(
                redis: connection,
                queues: queues,
                timeout: self.config.blockingTimeout).map { response in
                    self.isPollingRedis = false
                    
                    self.logger.verbose("stop queue check")
                    
                    if let response = response {
                        // Perform the job
                        self.performJob(response.0)
                    }
                    else {
                        // Else check again for a new job
                        self.nextIteration()
                    }
                }.catch { (error: Error) in
                    self.isPollingRedis = false
                    if self.establishingConnection == true { return }
                    self.connectionEntry(error: error)
            }
        }
    }
    
    /// Performs the Job
    ///
    /// - returns: Returns false if the job could not be started
    ///
    private func performJob(_ job: BackgrounderJob) {
        
        // Capture the start time
        let startTime = Date()

        // Report the job start
        self.jobStarted(job, startTime: startTime)
        
        // Create a job logger
        let logger = BackgrounderLogger(
            level: self.config.logLevel,
            prefix: "\(ProcessInfo.processInfo.processIdentifier) TID-\(self.id) \(job.className.toRuby) JID-\(job.id)",
            detailed: self.config.detailedLogging)
        
        // Log the job start
        logger.info("start")
        
        // Make sure the job implements the "BackgroundHandler" class
        guard let handler = job.handlerClass.self as? BackgrounderHandler.Type else {
            self.jobError(job, WorkerError.couldNotInstantiateHandler(className: job.className))
            return
        }

        // Perform the job
        do {
            try handler.init(on: self.container, logger: logger).perform(args: job.args).do {
                // Report the job end
                self.jobEnded(job)
                
                // Get the end time of the job
                let elapsedTime = Date().timeIntervalSince(startTime)
                
                // Log the job completion
                logger.info("done: \(String(format: "%.3f", elapsedTime)) sec")
                
                }.catch { error in self.jobError(job, error) }
        } catch { self.jobError(job, error) }
    }
    
    /// Reports a job start
    ///
    /// - parameters:
    ///   - job: The job that started running
    ///   - startTime: The time the job started at
    ///
    /// - returns: start time of the job
    ///
    private func jobStarted(_ job: BackgrounderJob, startTime: Date) {

        // Add the job to the running jobs variable
        self.runningJobs[job.id] = (job, startTime)
        
        // We moved this to happen first so we can immdiately start getting
        // the next job before this one executes
        if self.runningJobsCount < self.maxJobs {
            self.nextIteration()
        }
        
        self.logger.verbose("\(self.runningJobsCount) jobs pending")
    }
    
    /// Reports a job end.
    ///
    /// - parameters:
    ///   - job: The job that stopped running
    ///
    private func jobEnded(_ job: BackgrounderJob) {
        
        // If the job is not in the running jobs, it has already ended
        // and so we should try to delete it from the retry queue (local
        // and in Redis) and exit
        //
        // Note that this situation can occur if the job timed out and
        // then eventually finished
        if self.runningJobs[job.id] == nil {
            self.retries.remove(job: job)
            if let connection = self.connection {
                BackgrounderRetryQueue(redis: connection).delete(job: job)
            }
            return
        }

        // Increment the number of processed jobs
        self.processedCount.increment()
        
        // Remove the job from the running jobs variable
        self.runningJobs.removeValue(forKey: job.id)
        
        // If should exit, confirm stop
        if self.shouldExit {
            self.confirmStop()
        }
        
        // We had the max number of jobs, it means we skipped dispatching
        else if self.runningJobsCount == self.maxJobs-1 {
            // Else dispatch another job since this one completed
            self.nextIteration()
        }
    }
    
    /// Reports a job error.
    ///
    /// - parameters:
    ///   - job: The job that received the error
    ///   - error: The error that was received
    ///
    private func jobError(_ job: BackgrounderJob, _ error: Error) {
        
        // Log the error
        self.logger.warning("job raised exception: '\(job.toRedis)'")
        self.logger.warning("\(error)")
        
        // Increment the number of failed jobs
        self.failedCount.increment()
        
        // Check to see if it should be added as a retry or error
        if job.shouldRetry(max: self.config.maxRetries) {
            self.retries.append(job)
        }
        else {
            self.errors.append(job)
        }
        
        // Call job ended to start another one
        self.jobEnded(job)
    }
    
    /// Checks if all jobs have finished before confirming the stop
    ///
    private func confirmStop() {
        // If we should exit, wait for all of the remaining jobs to finish
        if self.shouldExit && self.runningJobsCount == 0 {
            self.logger.debug("stopped")
            
            // Close the Redis connection
            self.connection?.close()
            self.connection = nil
            
            // Confirm the completion of the process
            self.promise.succeed(result: self.id)
        }
    }
    
    /// Handles Connection Entry by making sure that Redis is reachable
    /// before letting the system continue
    ///
    /// - parameters:
    ///   - error: The error that was sensed
    ///   - delay: Time to sleep before making the call.  this is for recursion
    ///
    /// - note: This method is also the method that should be called
    ///         when the system loses the Redis connection.  It will
    ///         basically re-establish the connection and keep anything
    ///         else from running
    ///
    /// - note: All methods EXCEPT the "nextIteration" handler will
    ///         gracefully exit when an error occurs.  The iteration
    ///         handler will call this method which will essentially
    ///         prevent any further execution of the iteration handler
    ///         until the connection is resolved
    ///
    /// - note: All methods do their very best to make sure that no
    ///         data is lost during a connection issue.  the only
    ///         exception is if a job explicitely has retry = false,
    ///         this will fall on the floor
    ///
    private func connectionEntry(error: Error?=nil, delay: UInt32?=nil) {
        // Set the flag signalling we are handling connection issues
        self.establishingConnection = true
        
        // If error print it
        if let error = error {
            self.logger.error("connection error - \(error)")
            self.logger.debug("re-establishing redis connection")
        }
        
        // If delay, sleep
        if let delay = delay {
            sleep(delay)
            self.logger.debug("redis connection retry")
        }
        
        // Create reconnection helper
        func retryConnection() {
            self.connectionEntry(delay: UInt32(self.config.retryConnectionInterval))
        }
        
        // Close the old connection
        self.connection?.close()
        self.connection = nil
       
        // Kickoff Retry Logic until Redis Comes back up
        _ = RedisConnection.open(on: self.container, as: .backgrounderRedis).do { connection in
            self.connection = connection
            connection.redis.clientSetname("background-worker-\(self.id)").do { status in
                // Print a messsage stating the connection is present
                self.logger.debug("redis connection established")
                
                // Start the system
                self.establishingConnection = false
                self.nextIteration()
                
                }.catch { (error: Error) in retryConnection() }
            }.catch { (error: Error) in retryConnection() }
    }
    

    /// Health check task
    private var healthCheckTask: RepeatedTask?
    
    /// Function that is called to start the "health" check
    ///
    /// - parameters:
    ///   - delay: Number of seconds to periodically run the health check
    ///
    /// - note: This method is used to communicate information from this worker
    ///         to the main process.  This is called by the backgrounder "process"
    ///         and setup to run periodically
    ///
    func startHealthCheck(delay: Int, closure: @escaping (BackgrounderWorker, Int, Int, [String:[String:Any]])->()) {
        
        // Start the repeated health check task that runs every "delay" seconds.
        self.healthCheckTask = self.eventLoop.scheduleRepeatedTask(
            initialDelay: .seconds(delay),
            delay: .seconds(delay)) { (task: RepeatedTask) -> Void in
                self.runHealthCheck(closure: closure)
        }
    }
    
    /// Function that is called to force a healthcheck
    ///
    func runHealthCheck(closure: @escaping (BackgrounderWorker, Int, Int, [String:[String:Any]])->()) {
        
        // Check for jobs that have reached the job timeout
        let expireStartTime = self.config.jobTimeout.seconds.ago
        for (_, value) in self.runningJobs {
            if value.1 < expireStartTime {
                self.jobError(value.0, WorkerError.jobTimeoutReached(seconds: self.config.jobTimeout))
            }
        }
        
        // Get the information to pass to the main thread
        let processed = self.processedCount.reset()
        let failed = self.failedCount.reset()
        let jobs: [String:[String:Any]] = self.runningJobs.mapValues { job, startTime in
            ["queue": job.queue, "payload": job.toHash(), "run_at": startTime.toEpoch]
        }
        
        self.logger.verbose("health check PROCESSED(\(processed)) FAILED(\(failed)) JOBS(\(jobs.count))")
        
        // Report the status to the process
        closure(self, processed, failed, jobs)
    }
}

/// Private maintenance methods that are all encapsulated in the
/// "everyIteration" method
///
extension BackgrounderWorker {
    
    /// Uploads error/retry values using a transaction
    ///
    private func uploadFailedJobs(closure: @escaping ()->()) {
        // Make sure there is a Redis connection
        if let connection = self.connection {
            let totalErrors = self.errors.count
            let totalRetries = self.retries.count
            
            // Exit if there are no errors or retries to uplaod
            if totalErrors == 0 && totalRetries == 0 {
                closure()
                return
            }
            
            _ = connection.redis.pipeline { (conn: RedisApi) in
                
                // Add the errors to the pipeline
                if totalErrors > 0 {
                    BackgrounderDeadQueue(redis: connection).push(jobs: self.errors, runAt: Date(), redis: conn)
                }
                
                // Add the retries to the pipeline
                if totalRetries > 0 {
                    let retryTime = Date().addingTimeInterval(TimeInterval(self.config.retryInterval))
                    BackgrounderRetryQueue(redis: connection).push(jobs: self.retries, runAt: retryTime, redis: conn)
                }
                
                }.flatten(on: self.container).do { (responses: [RedisApiData]) in
                    self.logger.verbose("uploaded failures: ERROR(\(totalErrors)) RETRY(\(totalRetries))")
                    
                    // Remove the errors that were uploaded
                    if totalErrors > 0 {
                        self.errors = Array(self.errors.dropFirst(totalErrors))
                    }
                    
                    // Remove the retries that were uploaded
                    if totalRetries > 0 {
                        self.retries = Array(self.retries.dropFirst(totalRetries))
                    }
                    
                    closure()
                }.catch { (error: Error) in closure() }
        }
        else { closure() }
    }

    /// Checks scheduled jobs
    ///
    /// - note: This is a maintenance method
    ///
    private func checkScheduled(closure: @escaping ()->()) {
        // Make sure there is a Redis connection
        if let connection = self.connection {
            self.logger.verbose("checking scheduled queue")
            
            // Enqueue the scheduled jobs
            BackgrounderScheduleQueue(redis: connection).popAll { job in
                return BackgrounderQueue(name: job.queue, redis: connection).push(jobs: [job]).mapToVoid()
                }.do { count in
                    if count > 0 {
                        self.logger.debug("enqueued \(count) scheduled jobs")
                    }
                    closure()
                }.catch { error in closure() }
        }
        else { closure() }
    }
    
    /// Checks retried jobs
    ///
    /// - note: This is a maintenance method
    ///
    private func checkRetries(closure: @escaping ()->()) {
        // Make sure there is a Redis connection
        if let connection = self.connection {
            self.logger.verbose("checking retry queue")
            
            // Enqueue the retry jobs
            BackgrounderRetryQueue(redis: connection).popAll { job in
                return BackgrounderQueue(name: job.queue, redis: connection).push(jobs: [job]).mapToVoid()
                }.do { count in
                    if count > 0 {
                        self.logger.debug("enqueued \(count) retry jobs")
                    }
                    closure()
                }.catch { error in closure() }
        }
        else { closure() }
    }
    
    /// Method that exeecutes routine maintenance methods
    ///
    private func maintenance(closure: @escaping ()->()) {
        let isTime = Date() > self.maintenanceAt
        
        // Perform the maintenance if it is time OR we are going quiet
        // and need to flush everything out (going quiet signals a
        // power down)
        if (isTime || self.shouldQuiet) && self.config.shouldPerformMaintenance {
            self.logger.verbose("maintenance")
            
            if !self.shouldQuiet {
                
                // Pick the next time for maintenance
                self.maintenanceAt = Date.random(in: self.config.maintenanceInterval)
                
                // Enqueued and now ready scheduled jobs
                self.checkScheduled {
                    
                    // Enqueued and now ready retry jobs
                    self.checkRetries { closure() }
                }
            }
            else { closure() }
        }
        else { closure() }
    }
    
    /// Method that executes every run methods.  Items that you want to
    /// execute on every run should be added here
    ///
    private func everyIteration(closure: @escaping ()->()) {
        // Upload errors and retries as soon as possible.  We also want to do this BEFORE exiting
        self.uploadFailedJobs {
            
            // If should exit, skip the maintenance
            if self.shouldExit { closure() }
            else {
                // Maintenance is run periodically and will decide on it's
                // own if it is time or not to run
                self.maintenance() { closure() }
            }
        }
    }
}
