@testable import Backgrounder
import Redis
import Vapor
import XCTest

final class HandlerTests: RedisTestCase {

    func testPerformAsync() throws {
        // Dispatch test handler
        let job = try TestHandler.performAsync(args: [true], on: self.app).wait()
        
        // Check Redis
        let pushedJobData = try self.redis.lpop(key: "default".toRedisQueueName).wait()!
        let pushedJob = try BackgrounderJob.fromRedis(pushedJobData)
        
        XCTAssertEqual(pushedJob.id, job.id)
        XCTAssertEqual(pushedJob.className, "BackgrounderTests.TestHandler")
        XCTAssertEqual(pushedJob.queue, "default")
        XCTAssertEqual(pushedJob.args.first as? Bool, true)
        XCTAssertNil(pushedJob.runAt)
        
        XCTAssertEqual(try self.redis.smembers(key: "queues").wait().count, 1)
    }
    
    func testPerformIn() throws {
        let queue = BackgrounderScheduleQueue(redis: self.connection)
        
        XCTAssertEqual(try queue.size.wait(), 0)
        
        // Dispatch test handler
        _ = try TestHandler.performIn(200, args: [10], on: self.app).wait()
        XCTAssertEqual(try queue.size.wait(), 1)
    }
    
    func testPerformAt() throws {
        let queue = BackgrounderScheduleQueue(redis: self.connection)
        
        XCTAssertEqual(try queue.size.wait(), 0)
        
        // Dispatch test handler
        _ = try TestHandler.performAt(Date().addingTimeInterval(200), args: [10], on: self.app).wait()
        XCTAssertEqual(try queue.size.wait(), 1)
    }
    
    func testSetPerformAsync() throws {
        // Dispatch test handler
        let job = try TestHandler.set(queue: "high", id: "1234").performAsync(args: [10], on: self.app).wait()
        XCTAssertEqual(job.id, "1234")
        
        // Check Redis
        let pushedJobData = try self.redis.lpop(key: "high".toRedisQueueName).wait()!
        let pushedJob = try BackgrounderJob.fromRedis(pushedJobData)
        
        XCTAssertEqual(pushedJob.id, job.id)
        XCTAssertEqual(pushedJob.className, "BackgrounderTests.TestHandler")
        XCTAssertEqual(pushedJob.queue, "high")
        XCTAssertEqual(pushedJob.args.first as? Int, 10)
        XCTAssertNil(pushedJob.runAt)
    }
    
    func testSetPerformIn() throws {
        let queue = BackgrounderScheduleQueue(redis: self.connection)
        
        XCTAssertEqual(try queue.size.wait(), 0)
        
        // Dispatch test handler
        _ = try TestHandler.set(queue: "high").performIn(200, args: [10], on: self.app).wait()
        XCTAssertEqual(try queue.size.wait(), 1)
    }
    
    func testSetPerformAt() throws {
        let queue = BackgrounderScheduleQueue(redis: self.connection)
        
        XCTAssertEqual(try queue.size.wait(), 0)
        
        // Dispatch test handler
        _ = try TestHandler.set(queue: "high").performAt(Date().addingTimeInterval(200), args: [10], on: self.app).wait()
        XCTAssertEqual(try queue.size.wait(), 1)
    }
    
    func testDefault() throws {
        let job1 = try TestHandler.performAsync(args: [], on: self.app).wait()
        XCTAssertEqual(job1.queue, "default")
        XCTAssertEqual(job1.retry, false)
        
        let job2 = try TestDefaultHandler.performAsync(args: [], on: self.app).wait()
        XCTAssertEqual(job2.queue, "high")
        XCTAssertEqual(job2.retry, true)
        
        let job3 = try TestDefaultHandler.set(queue: "default", retry: false).performAsync(args: [], on: self.app).wait()
        XCTAssertEqual(job3.queue, "default")
        XCTAssertEqual(job3.retry, false)
    }

    static var allTests = [
        ("testPerformAsync", testPerformAsync),
        ("testPerformIn", testPerformIn),
        ("testPerformAt", testPerformAt),
        ("testSetPerformAsync", testSetPerformAsync),
        ("testSetPerformIn", testSetPerformIn),
        ("testSetPerformAt", testSetPerformAt),
        ("testDefault", testDefault),
        ]
}

