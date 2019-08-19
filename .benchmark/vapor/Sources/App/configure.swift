import Redis
import Backgrounder
import Vapor

/// Called before your application initializes.
public func configure(_ config: inout Config, _ env: inout Environment, _ services: inout Services) throws {
    
    // Create containers
    var databases = DatabasesConfig()
  
    // =========================================================
    // Redis
    // =========================================================
    
    // Setup the backgrounder
    try BackgrounderProvider.setup(
        config: BackgrounderConfig.default(concurrency: 5, logLevel: .warning),
        services: &services,
        databases: &databases)
    
    // Setup the other Redis store
    try BackgrounderProvider.setupRedis(
        as: .redis,
        services: &services,
        databases: &databases)
    
    // Set the max pools to 2
    let poolConfig = DatabaseConnectionPoolConfig(maxConnections: 2)
    services.register(poolConfig)

    // =========================================================
    // Register Services
    // =========================================================
    services.register(databases)
}
