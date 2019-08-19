import Vapor
import RedisApi

/// The Redis helper provides a base class for other helpers
///
class BackgrounderHelper {
    
    /// The redis connection for this client
    let connection: RedisConnection
    
    /// The worker for this client
    var worker: Container {
        return self.connection.worker
    }

    /// Returns the event loop to use for operations
    var eventLoop: EventLoop {
        return self.worker.eventLoop
    }
    
    /// Returns the redis helper API
    var redis: RedisApi {
        return self.connection.redis
    }

    /// Constructor
    ///
    /// - parameters:
    ///   - connection: The RedisConnection object
    ///
    required init(redis connection: RedisConnection) {
        self.connection = connection
    }
    
    /// Retrieves the info about the redis DB
    ///
    func info() -> Future<String?> {
        return self.redis.info()
    }
}
