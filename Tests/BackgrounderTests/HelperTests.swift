@testable import Backgrounder
import Redis
import Vapor
import XCTest

final class HelperTests: RedisTestCase {

    func testSubmitJob() throws {
        let submitter = BackgrounderSubmitter(redis: self.connection)
        
        // Submit initial job
        let job1 = BackgrounderJob(className: "App.TestHandler", args: [])
        XCTAssertEqual(try submitter.submit(job: job1).wait(), 1)
        
        // Submit the job
        let job2 = BackgrounderJob(className: "App.TestHandler", args: [])
        XCTAssertEqual(try submitter.submit(job: job2).wait(), 2)
        
        // Check Redis
        let pushedJobData = try self.redis.lpop(key: "default".toRedisQueueName).wait()
        let pushedJob = try BackgrounderJob.fromRedis(pushedJobData!)
        
        XCTAssertEqual(pushedJob.id, job2.id)
        XCTAssertEqual(pushedJob.className, "App.TestHandler")
        XCTAssertEqual(pushedJob.queue, "default")
        
        // Delete the other job
        XCTAssertEqual(try job1.delete(on: self.app).wait(), true)
    }

    func testSubmitMultipleJobs() throws {
        let submitter = BackgrounderSubmitter(redis: self.connection)
        
        // Submit initial job
        XCTAssertEqual(try submitter.submit(job: BackgrounderJob(className: "App.TestHandler", args: [])).wait(), 1)
        
        // Submit other jobs
        let job1 = BackgrounderJob(className: "App.TestHandler", args: [])
        let job2 = BackgrounderJob(className: "App.TestHandler", args: [], queue: "high")
        XCTAssertEqual(try submitter.submit(jobs: [job1, job2]).wait(), 3)
        
        // Check Job 1 in Redis
        let pushedJob1Data = try self.redis.lpop(key: "default".toRedisQueueName).wait()
        let pushedJob1 = try BackgrounderJob.fromRedis(pushedJob1Data!)
        
        XCTAssertEqual(pushedJob1.id, job1.id)
        XCTAssertEqual(pushedJob1.className, "App.TestHandler")
        XCTAssertEqual(pushedJob1.queue, "default")
        
        // Check Job 2 in Redis
        let pushedJob2Data = try self.redis.lpop(key: "high".toRedisQueueName).wait()
        let pushedJob2 = try BackgrounderJob.fromRedis(pushedJob2Data!)
        
        XCTAssertEqual(pushedJob2.id, job2.id)
        XCTAssertEqual(pushedJob2.className, "App.TestHandler")
        XCTAssertEqual(pushedJob2.queue, "high")
    }
    
    func testScheduleJobs() throws {
        let scheduler = BackgrounderScheduler(redis: self.connection)
        
        // Schedule jobs
        let job1 = BackgrounderJob(className: "App.TestHandler", args: [], runAt: Date().addingTimeInterval(600))
        let job2 = BackgrounderJob(className: "App.TestHandler", args: [], queue: "high", runAt: Date().addingTimeInterval(600))
        XCTAssertEqual(try scheduler.schedule(jobs: [job1, job2], queue: "temp").wait(), 2)
        
        // Check Job 2 in Redis
        XCTAssertEqual(try self.redis.zcount(key: "temp", min: .min, max: .max).wait(), 2)
    }
    
    func testEnqueueJobs() throws {
        // Schedule jobs
        let job1 = BackgrounderJob(className: "App.TestHandler", args: [], runAt: Date())
        let job2 = BackgrounderJob(className: "App.TestHandler", args: [], queue: "high", runAt: Date())
        let job3 = BackgrounderJob(className: "App.TestHandler", args: [], queue: "high", runAt: Date().addingTimeInterval(500)) // Won't enqueue
        XCTAssertEqual(try BackgrounderScheduler(redis: self.connection).schedule(jobs: [job1, job2, job3], queue: "temp").wait(), 3)
        
        // Check queues
        XCTAssertEqual(try self.redis.llen(key: "default".toRedisQueueName).wait(), 0)
        XCTAssertEqual(try self.redis.llen(key: "high".toRedisQueueName).wait(), 0)
        
        // Enqueue Jobs
        XCTAssertEqual(try BackgrounderEnqueuer(redis: self.connection).enqueue(queue: "temp").wait(), 2)
        
        // Check queues
        XCTAssertEqual(try self.redis.llen(key: "default".toRedisQueueName).wait(), 1)
        XCTAssertEqual(try self.redis.llen(key: "high".toRedisQueueName).wait(), 1)
    }
    
    func testDispatcherJobs() throws {
        // Submit initial job
        let job = BackgrounderJob(className: "App.TestHandler", args: [], queue: "high")
        XCTAssertEqual(try BackgrounderSubmitter(redis: self.connection).submit(job: job).wait(), 1)
        
        let dispatcher = BackgrounderDispatcher(redis: self.connection)
        let response = try dispatcher.dequeue(queues: ["high"], timeout: 1).wait()
        
        XCTAssertEqual(response?.0, "high")
        XCTAssertEqual(response?.1.id, job.id)
        
        // Returns nil when tried again
        XCTAssertNil(try dispatcher.dequeue(queues: ["high"], timeout: 1).wait())
    }
    
    static var allTests = [
        ("testSubmitJob", testSubmitJob),
        ("testSubmitMultipleJobs", testSubmitMultipleJobs),
        ("testScheduleJobs", testScheduleJobs),
        ("testEnqueueJobs", testEnqueueJobs),
        ("testDispatcherJobs", testDispatcherJobs),
        ]
}


