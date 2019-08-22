import Vapor
import RedisApi

public final class BackgrounderLauncher: Backgrounder, ServiceType {
    
    /// The "process" is the entry point of the backgrounder.  It performs
    /// the following functionality
    ///
    /// - Initializes the backgrounder
    /// - Spawns the workers
    /// - Periodically monitors the workers and reports global information
    ///   (jobs processed, failed, etc.) to Redis
    ///
    
    /// See `ServiceType`.
    public static var serviceSupports: [Any.Type] { return [Backgrounder.self] }
    
    /// See `ServiceType`.
    public static func makeService(for container: Container) throws -> BackgrounderLauncher {
        return try BackgrounderLauncher(config: container.make(), on: container)
    }
    
    /// Processed counter
    var processedCount = BackgrounderCounter()
    
    /// Failed counter
    var failedCount = BackgrounderCounter()
    
    /// Job Status [workerId][jobId][payload]
    var runningJobs = [String:[String:[String:Any]]]()
    
    /// Running Jobs Counter
    var runningJobsCount: Int {
        var count = 0
        for (_, value) in self.runningJobs {
            count += value.count
        }
        return count
    }
    
    /// Chosen configuration for this server.
    private let config: BackgrounderConfig
    
    /// The logger
    private var logger: Logger
    
    /// Container for setting on event loops.
    private weak var container: Container!
    
    /// Hold the current worker. Used for deinit.
    private var currentWorker: Worker!

    /// Hold the workers that are running on the workers
    private var workers: [BackgrounderWorker] = []
    
    /// Track how many processes are running
    private var runningProcesses = 0
    
    /// Variable tracking the last process count
    private var lastProcessed = 0
    
    /// Stop Promise
    private var stopPromise: Promise<Void>?
    
    /// Task that checks for jobs that have not completed
    private var stopTask: Scheduled<Void>?
    
    /// Heartbeat task
    var hearbeatTask: RepeatedTask?
    
    /// The time the process started
    let startedAt: Date

    /// The hostname
    let hostname: String
    
    /// The OS pid the process is executing in
    let pid: Int32
    
    /// Process "nonce" used in Sidekiq for logging
    let processNonce: String
    
    /// Serialed version of the process
    public var redisData: String?

    /// The Sidekiq identity of the process
    var identity: String {
        return "\(self.hostname):\(self.pid):\(self.processNonce)"
    }
    
    /// The event loop of the container
    var eventLoop: EventLoop {
        return self.container.eventLoop
    }
    
    /// A future that will be signalled when the server stops.
    public var onStop: Future<Void> {
        return self.stopPromise!.futureResult
    }
    
    /// Eumeration for the different states
    enum State {
        case idle
        case starting
        case running
        case stopping
    }
    
    var state: State = .idle
    
    /// Create a new launcher
    ///
    /// - parameters:
    ///     - config: Backgrounder preferences such as number of threads
    ///     - container: Root service-container to use for all event loops the server will create.
    ///
    public init(config: BackgrounderConfig, on container: Container) {
        self.startedAt = Date()
        self.hostname = ProcessInfo.processInfo.hostName
        self.pid = ProcessInfo.processInfo.processIdentifier
        self.processNonce = UUID().TID
        self.config = config
        self.container = container
        
        self.logger = BackgrounderLogger(
            level: self.config.logLevel,
            prefix: "\(self.pid)",
            detailed: self.config.detailedLogging)
    }

    /// Starts the backgrounder launcher
    ///
    public func start() throws -> EventLoopFuture<Void> {
        
        if self.state != .idle {
            self.logger.error("can only start a process that is idle")
        }
        
        self.state = .starting
        
        // Create the stop promise
        self.stopPromise = self.eventLoop.newPromise(of: Void.self)
        
        // Print startup message
        let console = try container.make(Console.self)
        console.print("Backgrounder Process \(self.pid) Started With Configuration")
        console.print("   - tag: \(self.config.tag)")
        console.print("   - stat TTL: \(self.config.statTTL) s")
        console.print("   - concurrency: \(self.config.concurrency)")
        console.print("   - max jobs per worker: \(self.config.maxJobsPerWorker)")
        console.print("   - max retries: \(self.config.maxRetries)")
        console.print("   - retry interval: \(self.config.retryInterval) s")
        console.print("   - retry connection interval: \(self.config.retryConnectionInterval) s")
        console.print("   - queues: \(self.config.queues)")
        console.print("   - labels: \(self.config.labels)")
        console.print("   - blocking timeout: \(self.config.blockingTimeout) s")
        console.print("   - job timeout: \(self.config.jobTimeout) s")
        console.print("   - health check interval: \(self.config.healthCheckInterval) s")
        console.print("   - maintenance interval: \(self.config.maintenanceInterval) s")
        console.print("   - should perform maintenance: \(self.config.shouldPerformMaintenance)")
        console.print("   - detailed logging: \(self.config.detailedLogging)")
        console.print("   - log level: \(self.config.logLevel)")
        
        self.logger.info("started")

        // Create this worker's own event loop group
        self.currentWorker = MultiThreadedEventLoopGroup(numberOfThreads: self.config.concurrency)
        
        // Kick off the processes
        for _ in 0..<self.config.concurrency {
            
            // Get the event loop
            let eventLoop = self.currentWorker.next()
            
            // Start the process
            eventLoop.submit {
                
                // Create the container
                let processContainer = self.container.subContainer(on: eventLoop)
                
                // Instantiate the process
                let newWorker = BackgrounderWorker(on: processContainer, config: self.config)
                
                // Pass process to the main event loop
                _ = processContainer.eventLoop.newSucceededFuture(result: newWorker)
                    .hopTo(eventLoop: self.container.eventLoop).map { worker in
                    
                        // Add the process to the processes array
                        self.workers.append(worker)
                        
                        // Increment the running process counter
                        self.runningProcesses += 1
                }
                
                // Start the process.  The Future will be called when the process is done
                _ = newWorker.start()
                    .hopTo(eventLoop: self.container.eventLoop).map { workerId in
                        
                        // Clear the running jobs for the worker
                        self.runningJobs[workerId] = [:]
                        
                        // Decrement the running process counter
                        self.runningProcesses -= 1
                        
                        // If there are no more running processes, confirm stop
                        if self.runningProcesses == 0 {
                            
                            // Stop the launcher
                            self.confirmStop()
                        }
                }
                
                // Setup the process health check.  This will have the workers periodically
                // report information back to the process
                newWorker.startHealthCheck(delay: self.config.healthCheckInterval) {
                    (worker: BackgrounderWorker, processed: Int, failed: Int, jobs: [String:[String:Any]]) in
                    // Update the worker helath
                    self.updateHealth(worker: worker, processed: processed, failed: failed, jobs: jobs)
                }
                
                }.catch { error in
                    self.logger.error("could not boot EventLoop: \(error).")
            }
        }
        
        self.state = .running

        // Register with the app
        if let app = self.container as? Application {
            app.runningBackgrounder = RunningBackgrounder(onStop: self.onStop, stop: self.stop)
        }

        // Start the heartbeat
        self.startHeartbeat()
        
        // Return the on stop future
        return self.onStop
    }
    
    /// Stops the server.
    ///
    @discardableResult
    public func stop() -> Future<Void> {
        
        if self.state != .running {
            self.logger.error("can only stop a process that is running")
            return .done(on: self.container)
        }
        
        self.state = .stopping
        
        self.logger.debug("stop requested")
        
        // Clears the heartbeat since we are shutting down
        self.clearHeartbeat()
        
        // Stop the processes
        self.forEachWorker { worker in
            
            // Force the workers to report a health check to make sure we have the most up to date info
            worker.runHealthCheck { (worker: BackgrounderWorker, processed: Int, failed: Int, jobs: [String : [String : Any]]) in
                // The information was received.  Pass it back to the main event loop
                self.updateHealth(worker: worker, processed: processed, failed: failed, jobs: jobs)
            }
            
            // Call stop on each worker
            worker.stop()
        }

        // Kick off a task to check that the workers have finished
        self.stopTask = self.eventLoop.scheduleTask(
            in: TimeAmount.seconds(self.config.killTimeout), {
                
                // Push all of the remaining jobs back to Redis
                _ = RedisPooledConnection.openWithAutoClose(on: self.container, as: .backgrounderRedis) {
                    (connection: RedisConnection) -> Future<Void> in
                    
                    return connection.redis.multi { (conn: RedisApi) in
                        
                        // Iterate through the running jobs and push them to redis
                        for (_, workerJobs) in self.runningJobs {
                            for (_, runningJob) in workerJobs {
                                if let queue = runningJob["queue"] as? String,
                                    let payload = runningJob["payload"] as? [String:Any] {
                                    
                                    // Push the job back to Redis
                                    let queue = BackgrounderQueue(name: queue, redis: connection)
                                    queue.push(jobs: [payload.toJson()], redis: conn)
    
                                    // Print an info message
                                    if let jobId = payload["jid"] as? String {
                                        self.logger.info("requed unfinished job JID-\(jobId)")
                                    }
                                }
                            }
                        }
                    }.flatten(on: self.container).mapToVoid()
                }
        })
        
        // Return the future that will be called when done
        return self.stopPromise!.futureResult
    }
    
    /// Confirms stop
    ///
    private func confirmStop() {
        
        // Kill the worker
        try! self.currentWorker?.syncShutdownGracefully()
        self.currentWorker = nil
        
        // Kill the stop task
        self.stopTask?.cancel()
        self.stopTask = nil
        
        // Confirm Stop and clear the promise
        self.stopPromise?.succeed()
        self.stopPromise = nil
        
        // Change state to idle
        self.state = .idle
        
        // Mark as stopped
        self.logger.info("stopped")
    }
    
    /// Method to quiet the server
    ///
    public func quiet() {
        self.shouldQuiet = true
    }
    
    /// Sets the server to quiet/not quiet.
    ///
    var shouldQuiet: Bool = false { didSet {
        // Stop the processes
        self.forEachWorker { worker in
            worker.shouldQuiet = self.shouldQuiet
        }
        } }
    
    /// Method to iterate through the workers
    ///
    func forEachWorker(closure: @escaping (BackgrounderWorker) -> ()) {
        self.workers.forEach { worker in
            _ = worker.eventLoop.submit {
                closure(worker)
            }
        }
    }
    
    /// Update worker health
    ///
    /// - note: This assumes the infor is being received from the worker event
    ///   loop and passed back to the launcher event loop for processing
    ///
    func updateHealth(worker: BackgrounderWorker, processed: Int, failed: Int, jobs:[String : [String : Any]]) {
        // The information was received.  Pass it back to the main event loop
        _ = worker.eventLoop.newSucceededFuture(result: (worker.id, processed, failed, jobs))
            .hopTo(eventLoop: self.container.eventLoop).map { id, processed, failed, jobs in
                
                // Store some statistics about the worker
                self.processedCount.increment(by: processed)
                self.failedCount.increment(by: failed)
                self.runningJobs[id] = jobs
        }
    }
}

extension BackgrounderLauncher: RedisObject {
    
    /// Converts the process to a hash
    ///
    public func toHash() -> [String:Any] {
        return [
            "hostname": self.hostname,
            "started_at": self.startedAt.toEpoch,
            "pid": self.pid,
            "tag": self.config.tag,
            "concurrency": self.config.concurrency,
            "queues": self.config.queues.list(),
            "labels": self.config.labels,
            "identity": self.identity,
        ]
    }
    
    /// Converts hash to process
    ///
    /// - note: We don't use this so just return nil
    ///
    public static func fromHash(hash: [String:Any]) -> BackgrounderLauncher? {
        return nil
    }
    
}

/// Heartbeat extensions
///
/// - note: This mimics sidekiq.  See
///   https://github.com/mperham/sidekiq/blob/master/lib/sidekiq/launcher.rb
///
extension BackgrounderLauncher {
  
    /// Returns the key for the stats
    var statKey: String {
        return self.identity
    }
    
    /// Returns the workers key for the stats
    var statWorkersKey:String {
        return "\(self.identity):workers"
    }
    
    /// Starts the heartbeat
    ///
    func startHeartbeat() {
        
        // Stop the existing heartbeat task if there is one
        self.hearbeatTask?.cancel()

        // Start the periodic process that initializes some values and sends the heartbeat
        self.hearbeatTask = self.eventLoop.scheduleRepeatedTask(
            initialDelay: TimeAmount.seconds(1),
            delay: TimeAmount.seconds(self.config.healthCheckInterval)) { (task: RepeatedTask) -> () in
                
                // If we are in debug mode or less, print the memory usage
                if self.config.logLevel.intValue <= LogLevel.debug.intValue {
                    do { self.logger.debug("Memory Usage: \(try Memory.usage()) MB") }
                    catch { self.logger.error("Memory Usage Error: \(error)") }   
                }

                // Get the counter values
                let processed = self.processedCount.reset()
                let failed = self.failedCount.reset()
                
                self.logger.debug("sending heartbeat")
                
                // Clear the heartbeat params
                _ = RedisPooledConnection.openWithAutoClose(on: self.container, as: .backgrounderRedis) {
                    (connection: RedisConnection) -> Future<Void> in
                    
                    return connection.redis.multi { (conn: RedisApi) in
                        
                        let date = "\(Date().toStatDate)"
                        
                        // Push the processed and failed stats
                        conn.incrby(key: "stat:processed", increment: processed)
                        conn.incrby(key: "stat:processed:\(date)", increment: processed)
                        conn.expire(key: "stat:processed:\(date)", seconds: self.config.statTTL)
                        
                        conn.incrby(key: "stat:failed", increment: failed)
                        conn.incrby(key: "stat:failed:\(date)", increment: failed)
                        conn.expire(key: "stat:failed:\(date)", seconds: self.config.statTTL)
                        
                        // Push the worker info
                        conn.del(keys: [self.statWorkersKey])
                        for worker in self.workers {
                            let workerId = worker.id
                            let workerJobs = self.runningJobs[workerId] ?? [String:Any]()
                            for (jobId, payload) in workerJobs {
                                // In Sidekiq there is only one job running per TID where-as in Vapor
                                // we have mutltiple.  We will address this by appending the jobId to the
                                // TID to create an entry for each job
                                if let payload = payload as? [String:Any] {
                                    let tid = "\(workerId):\(jobId)"
                                    conn.hset(key: self.statWorkersKey, field: tid, value: payload.toJson())
                                }
                            }
                        }
                        conn.expire(key: self.statWorkersKey, seconds: 60)
                        
                        }.flatten(on: self.container).flatMap(to: Void.self) { (responses: [RedisApiData]) -> Future<Void> in
                            
                            return connection.redis.multi { (conn: RedisApi) in
                                conn.sadd(key: "processes", members: [self.statKey])
                                _ = conn.exists(keys: [self.statKey]).do { (exists: Int) in
                                    if exists == 0 {
                                        // In Sidekiq, this code is a lifecycle event that kicks some things
                                        // off if this is the first time this is run.  I don't think we need
                                        // this in this implementation but keeping the code here for reference
                                    }
                                }
                                conn.hmset(key: self.statKey, fieldValuePairs: [
                                    ("info", self.toRedis),
                                    ("busy", String(self.runningJobsCount)),
                                    ("beat", String(Date().toEpoch)),
                                    ("quiet", self.shouldQuiet ? "true":"false")
                                    ])
                                conn.expire(key: self.statKey, seconds: 60)
                                _ = conn.rpop(key: "\(self.statKey)-signals").do { (msg: String?) in
                                    // This is a mechanism for the WEB UI to send TERM messages to the launcher
                                    if let msg = msg {
                                        if msg == "TSTP" {
                                            self.logger.info("'quiet' command received")
                                            self.shouldQuiet = true
                                        }
                                        else if msg == "TERM" {
                                            self.logger.info("'stop' command received")
                                            self.stop()
                                        }
                                        // TODO: TTIN - dump_threads
                                    }
                                }
                                }.flatten(on: self.container).mapToVoid()
                        }.catch { error in
                            self.logger.error("heartbeat error: \(error)")
                            
                            // Add the failed and processed back.  We don't want to lose those
                            // because of a network issue
                            self.processedCount.increment(by: processed)
                            self.failedCount.increment(by: failed)
                    }
                    
                }}
    }
    
    /// Stop Heartbeat
    ///
    func clearHeartbeat() {
        self.logger.debug("clearing heartbeat")
        
        // Stop the heartbeat task
        self.hearbeatTask?.cancel()
        self.hearbeatTask = nil
        
        // Clear the heartbeat params
        _ = RedisPooledConnection.openWithAutoClose(on: self.container, as: .backgrounderRedis) {
            (connection: RedisConnection) -> Future<Void> in
            
            return connection.redis.multi { (conn: RedisApi) in
                
                // Delete the heartbeat since we are shutting down.  Note that we
                // do not remove the task so it will add itself back if we didn't
                // shut down
                conn.srem(key: "processes", members: [self.statKey])
                conn.del(keys: [self.statWorkersKey])
                
                }.flatten(on: self.container).mapToVoid()
        }
    }
}
