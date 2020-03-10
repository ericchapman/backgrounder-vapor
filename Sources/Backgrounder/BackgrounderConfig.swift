import Vapor

public struct BackgrounderConfig: ServiceType {
    
    /// The default URL to use when none is provided
    public static let defaultRedisUrl = "redis://127.0.0.1:6379/0"
    
    public struct QueueConfig {
        var queueList = [String]()
        var weighted = false
        
        /// Constructor that will default to the queue named "default"
        ///
        public init() {}
        
        /// Constructor that creates a priority based queue where the
        /// first queue has the highest priority
        ///
        /// - parameters:
        ///   - priority: Array of strings ordered with the first value
        ///     having the highest priority
        ///
        public init(priority: [String]) {
            self.queueList = priority
        }
        
        /// Constructor that creates a weighted based queue where each
        /// value has a weight associated which will increase the
        /// probability of it being hte first value
        ///
        /// - parameters:
        ///   - weighted: Dictionary of "queue -> weight" pairs
        ///
        public init(weighted: [String:Int]) {
            self.weighted = true
            
            for (queue, weight) in weighted {
                for _ in 1...weight {
                    self.queueList.append(queue)
                }
            }
            
        }
        
        /// Method to generate the list of queues based on the scheme
        /// being used
        ///
        /// If priority was used, the list is returned
        /// If weighted was used, the list is regenerated on every request
        /// If the queues were provided, "default" is used
        ///
        /// - returns: List of queues in priority order
        ///
        public func generate() -> [String] {
            if self.queueList.count == 0 {
                return ["default"]
            }
            else {
                if self.weighted {
                    return self.queueList.shuffled().unique()
                }
                else {
                    return self.queueList
                }
            }
        }
        
        /// Returns the list of the queries in the original order
        ///
        /// - returns: List of the queues in set order
        ///
        public func list() -> [String] {
            return self.queueList.unique()
        }
        
    }
    
    /// See `ServiceType`.
    public static func makeService(for worker: Container) throws -> BackgrounderConfig {
        return .default()
    }
    
    /// The URL for the Redis connection
    public var redisUrl: String
    
    /// The tag for the process
    public var tag: String
    
    /// Number of seconds to keep stats
    public var statTTL: Int
    
    /// The number of processes to instantiate
    public var concurrency: Int
    
    /// The maximum number of jobs to run per process
    public var maxJobsPerWorker: Int
    
    /// The maximum time to retry a failing job
    public var maxRetries: Int
    
    /// The number of seconds to wait before retrying a job
    public var retryInterval: Int
    
    /// The number of seconds to wait before retrying a connection
    public var retryConnectionInterval: Int

    /// The labels for the worker
    public var labels: [String]
    
    /// The queues in the system, ordered by highest priority first
    public var queues: QueueConfig

    /// The number of seconds to block waiting for a new value from Redis
    public var blockingTimeout: Int
    
    /// The number of seconds to wait before performing maintenance activies
    public var maintenanceInterval: Int
    
    /// The number of seconds to wait before sending the health check
    public var healthCheckInterval: Int
    
    /// The number of seconds to wait after stopping before killing the job
    public var killTimeout: Int
    
    /// The number of seconds to wait before stopping a job
    public var jobTimeout: Int?
    
    /// Specifies if this instance should perform maintenance tasks
    public var shouldPerformMaintenance: Bool
    
    /// Specifies if logging should use detailed method
    public var detailedLogging: Bool
    
    /// The logging level
    public var logLevel: LogLevel
    
    /// Generates the default configuration
    public static func `default`(
        redisUrl: String = BackgrounderConfig.defaultRedisUrl,
        tag: String = "app",
        statTTL: Int = 5*365*24*60*60, // 5 years
        concurrency: Int = ProcessInfo.processInfo.activeProcessorCount,
        maxJobsPerWorker: Int = 5,
        maxRetries: Int = 5,
        retryInterval: Int = 5,
        retryConnectionInterval: Int = 2,
        labels: [String] = [],
        queues: QueueConfig = QueueConfig(),
        blockingTimeout: Int = 2,
        maintenanceInterval: Int = 5,
        healthCheckInterval: Int = 5,
        killTimeout: Int = 10,
        jobTimeout: Int? = nil,
        shouldPerformMaintenance: Bool = true,
        detailedLogging: Bool = true,
        logLevel: LogLevel = .info
        ) -> BackgrounderConfig {
        return BackgrounderConfig(
            redisUrl: redisUrl,
            tag: tag,
            statTTL: statTTL,
            concurrency: concurrency,
            maxJobsPerWorker: maxJobsPerWorker,
            maxRetries: maxRetries,
            retryInterval: retryInterval,
            retryConnectionInterval: retryConnectionInterval,
            labels: labels,
            queues: queues,
            blockingTimeout: blockingTimeout,
            maintenanceInterval: maintenanceInterval,
            healthCheckInterval: healthCheckInterval,
            killTimeout: killTimeout,
            jobTimeout: jobTimeout,
            shouldPerformMaintenance: shouldPerformMaintenance,
            detailedLogging: detailedLogging,
            logLevel: logLevel
        )
    }
    
}
