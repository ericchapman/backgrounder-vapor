import Vapor

extension Application {
    /// Stores a reference to the `Application`'s currently running backgrounder.
    public var runningBackgrounder: RunningBackgrounder? {
        get {
            guard let cache = try? self.make(RunningBackgrounderCache.self) else {
                return nil
            }
            return cache.storage
        }
        set {
            guard let cache = try? self.make(RunningBackgrounderCache.self) else {
                return
            }
            cache.storage = newValue
        }
    }
}

/// A context for the currently running `Runner` protocol. When a `Runner` successfully boots,
/// it sets one of these on the `runningBackgrounder` property of the `Application`.
///
/// This struct can be used to close the server.
///
///     try app.runningRunner?.close().wait()
///
/// It can also be used to wait until something else closes the server.
///
///     try app.runningRunner?.onClose().wait()
///
public struct RunningBackgrounder {
    /// A future that will be completed when the server closes.
    public let onStop: Future<Void>
    
    /// Stops the currently running backgrounder, if one is running.
    public let stop: () -> Future<Void>
}

/// MARK: Internal

/// Reference-type wrapper around a `RunningBackgrounder`.
internal final class RunningBackgrounderCache: ServiceType {
    
    /// See `ServiceType`.
    static func makeService(for container: Container) throws -> Self {
        return .init()
    }
    
    /// The stored `RunningBackgrounder`.
    var storage: RunningBackgrounder?
    
    /// Creates a new `RunningBackgrounderCache`.
    private init() { }
}

