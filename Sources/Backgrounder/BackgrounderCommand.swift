import Vapor
import Redis

class BenchmarkHandler: BackgrounderHandlerBase {
    override func perform(args: [Any]) throws -> EventLoopFuture<Void> {
        return RedisPooledConnection.openWithAutoClose(on: worker, as: .redis, closure: {
            (connection: RedisConnection) -> EventLoopFuture<Void> in

            // Perform some commands
            return connection.redis.multi { conn in
                let tempId = UUID().TID
                conn.set(key: tempId, value: "10")
                conn.incrby(key: tempId, increment: 5)
                _ = conn.get(key: tempId).do({ (value: String?) in
                    if value != "15" {
                        self.logger.error("expected 15, gpt \(value ?? "nil")")
                    }
                })
                conn.del(keys: [tempId])
                }.flatten(on: self.worker).mapToVoid()
        })
    }
}

public struct BackgrounderCommand: Command, ServiceType {
    /// See `ServiceType`.
    public static func makeService(for container: Container) throws -> BackgrounderCommand {
        return try BackgrounderCommand(backgrounder: container.make())
    }
    
    /// See `Command`.
    public var arguments: [CommandArgument] {
        return []
    }
    
    /// See `Command`.
    public var options: [CommandOption] {
        return [
            .value(name: "benchmark", short: "b", help: ["Runs the benchmark handler N times"]),
        ]
    }
    
    /// See `Command`.
    public let help: [String] = ["Invokes the Backgrounder process"]
    
    /// The server to boot.
    private let backgrounder: Backgrounder
    
    /// Create a new `ServeCommand`.
    public init(backgrounder: Backgrounder) {
        self.backgrounder = backgrounder
    }
    
    /// See `Command`.
    public func run(using context: CommandContext) throws -> Future<Void> {
        if let benchmark = context.options["benchmark"],
            let times = Int(benchmark) {
            
            context.console.print("Benchmark invoked with \(times) jobs")
            
            // Create the jobs
            var jobs = [BackgrounderJob]()
            for _ in (0..<times) {
                jobs.append(BackgrounderJob(className: BenchmarkHandler.handlerClassName, args: []))
            }
            
            // Dispatch them
            let promise = context.container.eventLoop.newPromise(Void.self)
            
            DispatchQueue.global(qos: .background).async {
                do {
                    // Open a Redis connection
                    let connection = try RedisConnection.open(on: context.container, as: .backgrounderRedis).wait()
                    
                    // Submit the jobs to Redis
                    _ = try BackgrounderQueue.push(redis: connection, jobs: jobs).wait()
                    
                    context.console.print("Jobs submitted")
                    
                    func wait(ms: TimeInterval) {
                        let end = Date().addingTimeInterval(ms/1000)
                        while Date() < end {}
                    }
                    
                    let queue = BackgrounderQueue(name: "default", redis: connection)
                    
                    let startTime = Date()
                    var endTime = startTime
                    var lastTime = startTime
                    var lastJobs = times
                    var currentJobs = times
                    
                    repeat {
                        wait(ms: 50)
                        currentJobs = try queue.size.wait()
                        let now = Date()
                        if (now.toEpoch-lastTime.toEpoch) > 1 {
                            context.console.print("Processing \(lastJobs-currentJobs) jobs/s")
                            lastTime = now
                            lastJobs = currentJobs
                        }
                        
                        if currentJobs > 0 {
                            endTime = Date()
                        }
                    } while currentJobs > 0
                    
                    // Close the Redis connection
                    connection.close()

                    // Report the results
                    let elapsedTime = endTime.toEpoch-startTime.toEpoch
                    let jobsPerSecond = Int(round(TimeInterval(times)/elapsedTime))
                    context.console.print("\(times) jobs processed in \(String(format: "%0.2f", elapsedTime)) s.  Average of \(jobsPerSecond) jobs/s")
                    
                    promise.succeed()
                } catch {
                    context.console.print("error: \(error)")
                    promise.fail(error: error)
                }
            }
            
            return promise.futureResult
        }
        else {
            return try self.backgrounder.start()
        }
    }
}
