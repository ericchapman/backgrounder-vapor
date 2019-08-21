import Vapor

/// Protocol for Handlers
///
///    Handler.performAsync(args: [:], on: worker)
///
/// - note: The Handlers must be of type "class" so that they can be
///         dynamically loaded by string name
///
public protocol BackgrounderHandler: class {
    
    /// Required constructor
    ///
    init(on worker: Container, logger: Logger)
    
    /// Returns a handle to the logger
    var logger: Logger { get }
    
    /// Returns a handle to the worker
    var worker: Container { get }
    
    /// Returns a handle to the eventLoop
    var eventLoop: EventLoop { get }
    
    /// Returns the name of the class
    static var handlerClassName: String { get }
    
    /// Allows the default queue to be defaulted for the handler
    static var queue: String? { get }
    
    /// Allows the default retry to be specified for the handler
    static var retry: Bool? { get }
    
    /// Method that will execute the job
    ///
    /// - parameters:
    ///   - args: The arguments for the job
    ///   - worker: The worker that the job is executing on
    /// - returns: A future Void signifying when the job is complete
    ///
    func perform(args: [Any]) throws -> Future<Void>
    
    /// Method to dispatch a job
    ///
    /// - parameters:
    ///   - args: The arguments to pass to the call
    ///   - worker: The worker that the job is executing on
    /// - returns: The created job
    ///
    static func performAsync(args: [Any], on worker: Container) -> Future<BackgrounderJob>
    
    /// Method to dispatch a job at a certain time
    ///
    /// - parameters:
    ///   - time: The time to perform the task
    ///   - args: The arguments to pass to the call
    ///   - worker: The worker that the job is executing on
    /// - returns: The created job
    ///
    static func performAt(_ time: Date, args: [Any], on worker: Container) -> Future<BackgrounderJob>
    
    /// Method to dispatch a job in a certain time
    ///
    /// - parameters:
    ///   - seconds: The number of seconds to perform the task in
    ///   - args: The arguments to pass to the call
    ///   - worker: The worker that the job is executing on
    /// - returns: The created job
    ///
    static func performIn(_ seconds: TimeInterval, args: [Any], on worker: Container) -> Future<BackgrounderJob>
    
    /// Allows custom options to be set such as queue or job id
    ///
    /// - parameters:
    ///   - queue: The queue
    ///   - id: Used to override the job ID
    ///   - retry: true if the job should retry on error
    /// - returns: A handler proxy
    ///
    static func set(queue: String?, id: String?, retry: Bool?) -> BackgrounderHandlerProxy
}

/// Default implementation for the protocols
extension BackgrounderHandler {
    
    public var eventLoop: EventLoop { return self.worker.eventLoop }
    
    public static var queue: String? { return nil }
    
    public static var retry: Bool? { return nil }
    
    public static var handlerClassName: String {
        return String(reflecting: self).toRuby
    }
    
    public static func performAsync(args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        return self.performAt(Date(), args: args, on: worker)
    }
    
    public static func performIn(_ seconds: TimeInterval, args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        return self.performAt(Date().addingTimeInterval(seconds), args: args, on: worker)
    }
    
    public static func performAt(_ time: Date, args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        // Creat the job
        let job = BackgrounderJob(
            className: self.handlerClassName,
            args: args,
            queue: self.queue,
            runAt: time,
            retry: self.retry)
        
        // Push the job to Redis
        return job.submit(on: worker).map(to: BackgrounderJob.self) { wasSubmitted in
            return job
        }
    }
    
    public static func set(queue: String?=nil, id: String?=nil, retry: Bool?=nil) -> BackgrounderHandlerProxy {
        let proxy = BackgrounderHandlerProxy(klass: Self.self)
        proxy.queue = queue ?? self.queue
        proxy.id = id
        proxy.retry = retry ?? self.retry
        return proxy
    }
}

/// Base class for backgrounder handlers
///
open class BackgrounderHandlerBase: BackgrounderHandler {
    public let logger: Logger
    public let worker: Container
    
    open class var queue: String? { return nil }
    
    open class var retry: Bool? { return nil }
    
    required public init(on worker: Container, logger: Logger=BackgrounderLogger(level: .error)) {
        self.logger = logger
        self.worker = worker
    }
    
    open func perform(args: [Any]) throws -> Future<Void> {
        self.logger.info("Do Something")
        return .done(on: worker)
    }
}

/// This class allows options to be overriden
///
///    Handler.set(queue: "high").performAsync(args: [:], on: worker)
///
public class BackgrounderHandlerProxy {
    public var at: Date?
    public var queue: String?
    public var retry: Bool?
    public var id: String?
    public var klass: BackgrounderHandler.Type
    
    public init(klass: BackgrounderHandler.Type) { self.klass = klass }
    
    public func performAsync(args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        return self.performAt(Date(), args: args, on: worker)
    }
    
    public func performIn(_ seconds: TimeInterval, args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        return self.performAt(Date().addingTimeInterval(seconds), args: args, on: worker)
    }
    
    public func performAt(_ time: Date, args: [Any], on worker: Container) -> Future<BackgrounderJob> {
        // Create the Job
        let job = BackgrounderJob(
            className: self.klass.handlerClassName,
            args: args,
            id: self.id,
            queue: self.queue,
            runAt: time,
            retry: self.retry)
        
        // Push the job to Redis
        return job.submit(on: worker).map(to: BackgrounderJob.self) { wasSubmitted in
            return job
        }
    }
}
