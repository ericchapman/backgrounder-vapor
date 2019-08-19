import Redis
import RedisApi

public class RedisConnection {

    /// The client to access the helpers
    let api: RedisWrapper
    
    /// The identifier for the Redis DB
    let identifier: RedisIdentifier
    
    /// The worker for this client
    public var worker: Container {
        return self.api.worker
    }
    
    /// Returns the handle to the redis API
    public var redis: RedisApi {
        return self.api
    }
    
    /// The redis connection for this client
    public var client: RedisClient {
        return self.api.client
    }
    
    /// Constructor
    public init(on worker: Container, client: RedisClient, as identifier: RedisIdentifier) {
        self.identifier = identifier
        self.api = RedisWrapper(on: worker, client: client)
    }
    
    /// Creates and destroys a redis connection
    ///
    /// - parameters:
    ///   - on: The container to open the connection on
    ///   - identifier: The identifier for the DB
    ///
    public class func openWithAutoClose<T>(
        on worker: Container,
        as identifier: RedisIdentifier,
        closure: @escaping (RedisConnection) throws -> Future<T>) -> Future<T> {
        return self.open(on: worker, as: identifier).flatMap(to: T.self) { connection in
            return try closure(connection).map(to: T.self, { (result: T) -> T in
                connection.close()
                return result
            })
        }
    }
    
    /// Creates a redis connection
    ///
    public class func open(
        on worker: Container,
        as identifier: RedisIdentifier) -> Future<RedisConnection> {
        return worker.newConnection(to: identifier).map(to: RedisConnection.self) { client in
            return RedisConnection(on: worker, client: client, as: identifier)
        }
    }
    
    /// Closes the connection
    ///
    public func close() {
        self.client.close()
    }
}

public class RedisPooledConnection: RedisConnection {

    /// Creates a pooled redis connection
    ///
    override public class func open(
        on worker: Container,
        as identifier: RedisIdentifier) -> Future<RedisConnection> {
        return worker.requestPooledConnection(to: identifier).map(to: RedisConnection.self) { client in
            return RedisPooledConnection(on: worker, client: client, as: identifier)
        }
    }
    
    /// Releases the pooled connection
    ///
    override public func close() {
        do {
            try self.worker.releasePooledConnection(self.client, to: self.identifier)
        } catch {
            print("\(error)")
        }
    }
}

public typealias RedisIdentifier = DatabaseIdentifier<RedisDatabase>

extension DatabaseIdentifier {
    static var backgrounderRedis: RedisIdentifier {
        return .init("backgrounder-redis")
    }
}
