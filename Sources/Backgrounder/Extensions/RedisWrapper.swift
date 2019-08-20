import Vapor
import Redis
import RedisApi
import DatabaseKit

/// This is a wrapper class to abstrct the Redis library from the rest
/// of the code
public class RedisWrapper {
    
    /// The redis connection for this client
    let client: RedisClient
    
    /// The worker for this client
    let worker: Container

    /// Constructor
    ///
    init(on worker: Container, client: RedisClient) {
        self.worker = worker
        self.client = client
    }
}

///------------------------------------- Wrap Redis Client --------------------------------

/// Implement 'RedisApiData' protocol
///
extension RedisData: RedisApiData {
    public var redisToArray: [RedisApiData]? { return self.array }
    public var redisToString: String? { return self.string }
    public var redisToInt: Int? { return self.int }
    public var redisToDouble: Double? {
        if let string = self.string {
            return Double(string)
        }
        return nil
    }
}

/// Implement 'RedisApi' protocol
///
extension RedisWrapper: RedisApi {
    public var eventLoop: EventLoop {
        return self.worker.eventLoop
    }
    
    public func send(command: String, args: [String]) -> Future<RedisApiData> {
        let data = ([command] + args).map { RedisData(bulk: $0) }
        return self.client.send(RedisData.array(data)).map(to: RedisApiData.self, {
            (data: RedisData) -> RedisApiData in
            return data as RedisApiData
        })
    }
    
    public func send(pipeline commands: [[String]]) -> Future<[RedisApiData]> {
        let promise = self.eventLoop.newPromise([RedisApiData].self)
        
        // Dispatch the requests
        var futures = [Future<RedisData>]()
        for command in commands {
            let data = command.map { RedisData(bulk: $0) }
            futures.append(self.client.send(RedisData.array(data)))
        }
        
        // Flatten them and return the array of responses
        _ = futures.flatten(on: self.worker).do { (response: [RedisData]) in
            promise.succeed(result: response)
            }.catch { error in
                promise.fail(error: error)
        }
        
        return promise.futureResult
    }
}

