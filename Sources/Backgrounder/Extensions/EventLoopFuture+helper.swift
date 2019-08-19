import Service

extension EventLoopFuture {
    /// Chains Futures Together where the last result will be used with the next
    /// one.  This is useful when you are trying to combine the results of values
    /// in a loop where each loop iteration return a future
    ///
    /// Lets say you have the synchronous function "doSomething()" that returns
    /// an integer and you want to sum the results. Synchronously you would do
    /// the following
    ///
    ///     var total = 0
    ///     for value in values {
    ///        total += doSomething(value)
    ///     }
    ///     return total
    ///
    /// If "doSomething()" instead returns a Future, you can do the following with
    /// this method
    ///
    ///     var futureResult = eventLoop.future(0)
    ///     for value in values {
    ///        futureResult = futureResult.chain(operation: {
    ///            return doSomething(value)
    ///        }, combine: { v1, v2 in
    ///            return v1 + v2
    ///        }
    ///     }
    ///     return futureResult
    ///
    /// - parameters:
    ///   - operation: The operation to chain
    ///   - combine: Method to combine the previouse value with this one
    ///              and return the result
    /// - returns: A future containing the new result
    ///
    public func chain(operation: @escaping () throws -> Future<T>, combine: @escaping (T,T) -> T) -> Future<T> {
        return self.flatMap(to: T.self) { v1 in
            return try operation().map(to: T.self) { v2 in
                return combine(v1, v2)
            }}
    }
    
    /// Maps any result to a void future
    ///
    public func mapToVoid() -> Future<Void> {
        return self.map(to: Void.self) { (responses: T) -> Void in }
    }
}
