@testable import Backgrounder
import Redis
import Vapor
import XCTest

final class WorkerTests: RedisTestCase {

    var process: BackgrounderLauncher?
    var config: BackgrounderConfig!
    
    let interval: UInt32 = 2
    
    override func setUp() {
        super.setUp()

        self.config = BackgrounderConfig.default()
        config.maintenanceInterval = Int(self.interval)
        config.healthCheckInterval = Int(self.interval)
        config.killTimeout = 2
        config.logLevel = .info
        
        // Create the worker
        self.process = BackgrounderLauncher(config: config, on: self.app)
        
        // Start the launcher
        _ = try! self.process?.start()
    }
    
    override func tearDown() {
        super.tearDown()
        
        // Close the launcher and wait for it to complete
        try! self.process?.stop().wait()
    }
    
    func waitForKeyToStabalize(key: String) throws {
        var isDone = false
        var lastValue = try self.redis.get(key: key).wait()
        repeat {
            sleep(4)
            let nextValue = try self.redis.get(key: key).wait()
            
            if nextValue == lastValue {
                isDone = true
            }
            
            lastValue = nextValue
        } while !isDone
    }
    
    func submitJobs(count: Int, failing: Bool=false, busy: Bool=false) throws {
        // Create the submitter
        let submitter = BackgrounderSubmitter(redis: self.connection)

        // Create the jobs
        var jobs = [BackgrounderJob]()
        for _ in (0..<count) {
            if failing {
                jobs.append(BackgrounderJob(className: TestErrorHandler.handlerClassName, args: [], retry: true))
            }
            else if busy {
                jobs.append(BackgrounderJob(className: TestBusyHandler.handlerClassName, args: [2], retry: true))
            }
            else {
                jobs.append(BackgrounderJob(className: TestHandler.handlerClassName, args: [], retry: true))
            }
        }
        _ = try submitter.submit(jobs: jobs).wait()
    }
    
    func testWorkerJobDispatch() throws {
        let jobTotal = 5000
        
        // Submit a bunch of jobs
        try self.submitJobs(count: jobTotal)

        // Wait for the value to stop changing
        try waitForKeyToStabalize(key: TestHandler.key)

        // Check to see that the job dispatched
        XCTAssertEqual(try self.redis.get(key: TestHandler.key).wait(), String(jobTotal))
    }
    
    func testProcessErrorHandler() throws {
        // Create the submitter
        let submitter = BackgrounderSubmitter(redis: self.connection)
        
        // Throw Immediate.  Should handle and value is nil
        _ = try submitter.submit(job: BackgrounderJob(className: TestErrorHandler.handlerClassName, args: [true])).wait()
        sleep(1)
        XCTAssertNil(try self.redis.get(key: TestErrorHandler.key).wait())
        
        // Throw Later
        _ = try submitter.submit(job: BackgrounderJob(className: TestErrorHandler.handlerClassName, args: [])).wait()
        sleep(1)
        XCTAssertEqual(try self.redis.get(key: TestErrorHandler.key).wait(), "1")
        
        // Check the error queue
        sleep(3)
        XCTAssertEqual(try self.redis.zcount(key: BackgrounderQueue.deadQueue, min: .min, max: .max).wait(), 2)
    }
    
    func testProcessScheduleLogic() throws {
        // Create some jobs
        let job = try TestHandler.performIn(5, args: [], on: self.app).wait()
        _ = try TestHandler.performIn(5, args: [], on: self.app).wait()
        _ = try TestHandler.performIn(5, args: [], on: self.app).wait()
        _ = try TestHandler.performIn(5, args: [], on: self.app).wait()
        
        // Wait a few seconds and make sure they did NOT run yett
        sleep(4)
        XCTAssertNil(try self.redis.get(key: TestHandler.key).wait())
        
        // cancel the first job
        XCTAssertEqual(try job.deleteScheduled(on: self.app).wait(), true)
   
        // Wait a few more seconds and make sure that only 3 ran since the first was cancelled
        sleep(4)
        XCTAssertEqual(try self.redis.get(key: TestHandler.key).wait(), "3")
    }
    
    func testProcessRetryLogic() throws {
        // Create some jobs
        let job1 = BackgrounderJob(className: "BackgrounderTests.TestErrorHandler", args: [], retry: true)
        let job2 = BackgrounderJob(className: "BackgrounderTests.TestErrorHandler", args: [], retry: true)

        // Expire job 1
        while job1.shouldRetry(max: self.config.maxRetries) == true {}
        
        // Submit the jobs
        XCTAssertEqual(try BackgrounderSubmitter(redis: self.connection).submit(jobs: [job1, job2]).wait(), 2)
        
        // Wait for the job to finish and check the queues
        sleep(3)
        XCTAssertEqual(try self.redis.zcount(key: BackgrounderQueue.deadQueue, min: .min, max: .max).wait(), 1)
        XCTAssertEqual(try self.redis.zcount(key: BackgrounderQueue.retryQueue, min: .min, max: .max).wait(), 1)
        
        // Flush Redis
        self.flush()
        
        // Cheat testing retry re-execute logic by placing a job in the retry queue and waiting for it to run
        let job3 = BackgrounderJob(className: "BackgrounderTests.TestHandler", args: [], runAt: Date().addingTimeInterval(2), retry: true)
        
        // Create a scheduler and push to the retry queue
        XCTAssertEqual(try BackgrounderScheduler(redis: self.connection).schedule(
            job: job3,
            queue: BackgrounderQueue.retryQueue).wait(), 1)
        XCTAssertEqual(try self.redis.zcount(key: BackgrounderQueue.retryQueue, min: .min, max: .max).wait(), 1)

        sleep(6)
        XCTAssertEqual(try self.redis.get(key: TestHandler.key).wait(), "1")
    }
    
    func testProcessHeartbeat() throws {
        let processId = self.process!.identity
        
        // Check Initial State
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 0)
        XCTAssertEqual(try self.redis.smembers(key: "\(processId):workers").wait().count, 0)
        XCTAssertNil(try self.redis.get(key: "stat:processed").wait())
        XCTAssertNil(try self.redis.get(key: "stat:failed").wait())
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait().keys.count, 0)
        
        // Spawn some jobs.  They wont be reported until the iteration after the next one
        try self.submitJobs(count: 8)
        try self.submitJobs(count: 2, busy: true)
        try self.submitJobs(count: 2, failing: true)
        
        sleep(self.interval-1)

        // Check first update
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 1)
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait()[0], self.process!.identity)
        XCTAssertEqual(try self.redis.hgetall(key: "\(processId):workers").wait().keys.count, 0)
        XCTAssertEqual(try self.redis.get(key: "stat:processed").wait(), "0")
        XCTAssertEqual(try self.redis.get(key: "stat:failed").wait(), "0")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["busy"], "0")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["quiet"], "false")
        
        sleep(self.interval)
        
        // Has the jobs recorded
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 1)
        XCTAssertEqual(try self.redis.get(key: "stat:processed").wait(), "10")
        XCTAssertEqual(try self.redis.get(key: "stat:failed").wait(), "2")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["busy"], "2")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["quiet"], "false")
        
        // We should have 2 tasks jobs busy now
        let busyJobs = try self.redis.hgetall(key: "\(processId):workers").wait()
        XCTAssertEqual(busyJobs.keys.count, 2)
        for (key, value) in busyJobs {
            // Check that payload matches job ID in TID
            let jobId = String(key.split(separator: ":")[1])
            let jobInfo = value.fromJson() ?? [String: Any]()
            let jobPayload = jobInfo["payload"] as? [String:Any] ?? [:]
            XCTAssertEqual(jobPayload["jid"] as? String, jobId)
        }
        
        // Test quiet
        self.process!.quiet()
        sleep(self.interval)
        
        // Jobs still recorded, busy clears, quiet set
        XCTAssertEqual(try self.redis.get(key: "stat:processed").wait(), "12")
        XCTAssertEqual(try self.redis.get(key: "stat:failed").wait(), "2")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["busy"], "0")
        XCTAssertEqual(try self.redis.hgetall(key: processId).wait()["quiet"], "true")
        
        // Test stop
        try self.process?.stop().wait()
        self.process = nil
        sleep(self.interval)
        
        // Check Initial State
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 0)
        XCTAssertEqual(try self.redis.smembers(key: "\(processId):workers").wait().count, 0)
        XCTAssertEqual(try self.redis.get(key: "stat:processed").wait(), "12")
        XCTAssertEqual(try self.redis.get(key: "stat:failed").wait(), "2")
    }
    
    func testKillTimeout() throws {
        // Ensure the job queue is empty
        XCTAssertEqual(try self.redis.llen(key: "default").wait(), 0)
        
        // Create long running job
        let job = BackgrounderJob(className: TestBusyHandler.handlerClassName, args: [10], retry: true)
        
        // Submit the job
        XCTAssertEqual(try BackgrounderSubmitter(redis: self.connection).submit(job: job).wait(), 1)
        
        // Sleep 2 seconds and ensure the job is no longer in the queue (it is running
        sleep(1)
        XCTAssertEqual(try self.redis.llen(key: "default").wait(), 0)
        
        // Explicitely stop the launcher
        try! self.process?.stop().wait()
        
        // Wait 3 seconds and make sure the job is pushed back to the queue
        sleep(UInt32(self.config.killTimeout)+1)
        XCTAssertEqual(try self.redis.llen(key: "default").wait(), 1)
        
        // Nil out the proces sto make sure stop isn't called twice
        self.process = nil
    }
    
    func testStopCommands() throws {
        let processId = self.process!.identity
        
        // Spawn some jobs to let the health check run
        try self.submitJobs(count: 8)

        sleep(self.interval+1)
        
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 1)
        XCTAssertEqual(self.process?.state, .running)
        
        // Send the quiet command
        _ = try self.redis.lpush(key: "\(processId)-signals", values: ["TSTP"]).wait()
        
        sleep(self.interval+1)
        
        // Ensure the process is quiet
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 1)
        XCTAssertEqual(self.process?.shouldQuiet, true)
        XCTAssertEqual(self.process?.state, .running)
        
        // Send the stop command
        _ = try self.redis.lpush(key: "\(processId)-signals", values: ["TERM"]).wait()
        
        sleep(self.interval+1)
        
        // Ensure the process is stopped and removed
        XCTAssertEqual(try self.redis.smembers(key: "processes").wait().count, 0)
        XCTAssertEqual(self.process?.state, .idle)
    }
    
    static var allTests = [
        ("testWorkerJobDispatch", testWorkerJobDispatch),
        ("testProcessErrorHandler", testProcessErrorHandler),
        ("testProcessScheduleLogic", testProcessScheduleLogic),
        ("testProcessRetryLogic", testProcessRetryLogic),
        ("testProcessHeartbeat", testProcessHeartbeat),
        ("testKillTimeout", testKillTimeout),
        ("testStopCommands", testStopCommands),
        ]
}
