@testable import Backgrounder
import Vapor
import RedisApi
import XCTest

extension Application {
    static func testable() throws -> Application {
        let config = Config.default()
        var services = Services.default()
        let env = Environment.testing
        
        // Setup the redis connections
        var databases = DatabasesConfig()
        try BackgrounderProvider.setupRedis(as: .backgrounderRedis, services: &services, databases: &databases)
        try BackgrounderProvider.setupRedis(as: .redis, services: &services, databases: &databases)
        services.register(databases)
        
        // Created the application
        return try Application(config: config, environment: env, services: services)
    }
}

class RedisTestCase: XCTestCase {
    var app: Application!
    var connection: RedisConnection!
    
    override func setUp() {
        super.setUp()
        self.app = try! Application.testable()
        self.connection = try! RedisConnection.open(on: self.app, as: .backgrounderRedis).wait()
        
        // Flush Redis
        self.flush()
    }
    
    override func tearDown() {
        super.tearDown()
        
        self.connection.close()
    }
    
    var redis: RedisApi {
        return self.connection.redis
    }
    
    func flush() {
        _ = try! self.redis.flushdb().wait()
    }
}

public class TestHandler: BackgrounderHandlerBase {
    static let key = "test:key"
    
    public override func perform(args: [Any]) throws -> EventLoopFuture<Void> {
        return RedisPooledConnection.openWithAutoClose(on: self.worker, as: .redis) { (connection: RedisConnection) -> EventLoopFuture<Void> in
            return connection.redis.multi { (conn: RedisApi) in
                conn.incrby(key: TestHandler.key, increment: 1)
                _ = conn.clientId().do { (id: Int) in
                    self.logger.warning("redis client id: \(id)")
                }
            }.flatten(on: self.worker).mapToVoid()
        }
    }
}

public class TestDefaultHandler: BackgrounderHandlerBase {
    static let key = "test:default:key"

    public override static var queue: String? { return "high" }
    
    public override static var retry: Bool? { return true }
    
    public override func perform(args: [Any]) throws -> EventLoopFuture<Void> {
        return RedisPooledConnection.openWithAutoClose(on: self.worker, as: .redis) { (connection: RedisConnection) -> EventLoopFuture<Void> in
            return connection.redis.incrby(key: TestDefaultHandler.key, increment: 1).map(to: Void.self) { count in
                // Do Nothing
            }
        }
    }
}

public class TestErrorHandler: BackgrounderHandlerBase {
    static let key = "test:error:key"
    
    enum HandlerError: Error {
        case generalError
    }
    
    public override func perform(args: [Any]) throws -> EventLoopFuture<Void> {
        if let throwNow = args.first as? Bool, throwNow == true {
            throw HandlerError.generalError
        }
        
        return RedisPooledConnection.openWithAutoClose(on: self.worker, as: .redis) { (connection: RedisConnection) -> EventLoopFuture<Void> in
            return connection.redis.incrby(key: TestErrorHandler.key, increment: 1).map(to: Void.self) { count in
                throw HandlerError.generalError
            }
        }
    }
}

public class TestBusyHandler: BackgrounderHandlerBase {
    
    public override func perform(args: [Any]) throws -> EventLoopFuture<Void> {
        let delay = args[0] as? Int ?? 5  // Default to 5 seconds
        
        return self.eventLoop.execute(in: TimeAmount.seconds(delay)) {
            // Do nothing
        }
    }
}
