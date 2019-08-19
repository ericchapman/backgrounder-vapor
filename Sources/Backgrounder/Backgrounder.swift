import Vapor

/// Backgrounders are capable of fetching jobs from a database and executing them
public protocol Backgrounder {
    /// Starts the `Backgrounder`.
    ///
    /// Upon starting, the `Backgrounder` must set the application's `runningBackgrounder` property.
    ///
    /// - parameters:
    /// - returns: A future notification that will complete when the `Backgrounder` has started successfully.
    ///
    func start() throws -> Future<Void>
    
    /// Stops the 'Backgrounder'
    ///
    /// - parameters:
    /// - returns: A future notification that will complete when the `Backgrounder` has stpped successfully.
    ///
    func stop() -> Future<Void>
    
    /// Quiets the 'Backgrounder'.  this should be called before shutdown
    /// to quiesce the backgrounder to make sure the loss of data is minimal
    ///
    func quiet()
}
