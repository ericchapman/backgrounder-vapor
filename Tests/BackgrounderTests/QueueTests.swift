@testable import Backgrounder
import Redis
import Vapor
import XCTest

final class QueueTests: RedisTestCase {

    func testMethods() throws {
        // Dispatch test handler
        _ = try TestHandler.set(queue: "queue1").performAsync(args: [true], on: self.app).wait()
        _ = try TestHandler.set(queue: "queue2").performAsync(args: [true], on: self.app).wait()
        _ = try TestHandler.set(queue: "queue2").performAsync(args: [true], on: self.app).wait()
        
        // Check the sizes of the queues
        let queues = try BackgrounderQueue.all(redis: self.connection).wait()
        XCTAssertEqual(queues.count, 2)
        XCTAssertEqual(try queues[0].size.wait(), 1)
        XCTAssertEqual(try queues[1].size.wait(), 2)
        
        sleep(1)
        
        XCTAssertGreaterThanOrEqual(try queues[0].latency.wait() ?? 0, 1)
        XCTAssertGreaterThanOrEqual(try queues[1].latency.wait() ?? 0, 1)
        
        // Remove the queues
        try queues.forEach { try $0.clear().wait() }
        
        XCTAssertEqual(try queues[0].size.wait(), 0)
        XCTAssertEqual(try queues[1].size.wait(), 0)
        XCTAssertEqual(try BackgrounderQueue.all(redis: self.connection).wait().count, 0)
    }
    
    static var allTests = [
        ("testMethods", testMethods),
        ]
}

