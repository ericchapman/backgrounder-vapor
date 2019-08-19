import Vapor
import Redis

public class BackgrounderProvider {
 
    /// This method instantiates the backgrounder instance
    ///
    /// - note: Use this method when you need to define other commands and
    ///   databases
    ///
    /// - parameters:
    ///   - config - The configuration for the backgrounder
    ///   - services - The srvices object
    ///   - databases - Database config object
    ///   - commands - Commands config object
    ///
    public static func setup(
        config: BackgrounderConfig,
        services: inout Services,
        databases: inout DatabasesConfig,
        commands: inout CommandConfig) throws {
        
        // Setup Redis
        try self.setupRedis(
            redisUrl: config.redisUrl,
            as: .backgrounderRedis,
            services: &services,
            databases: &databases)
        
        // Setup Backgrounder
        services.register(config)
        services.register(BackgrounderLauncher.self)
        services.register(BackgrounderCommand.self)
        
        // Add the command
        commands.use(BackgrounderCommand.self, as: "backgrounder")
    }
    
    /// This method instantiates the backgrounder instance
    ///
    /// - note: Use this method when you need to define other commands
    ///
    /// - parameters:
    ///   - config - The configuration for the backgrounder
    ///   - services - The srvices object
    ///   - commands - Commands config object
    ///
    public static func setup(
        config: BackgrounderConfig,
        services: inout Services,
        commands: inout CommandConfig) throws {
        
        var databases = DatabasesConfig()
   
        try self.setup(
            config: config,
            services: &services,
            databases: &databases,
            commands: &commands)
        
        services.register(databases)
    }
    
    /// This method instantiates the backgrounder instance
    ///
    /// - note: Use this method when you need to define other databases
    ///
    /// - parameters:
    ///   - config - The configuration for the backgrounder
    ///   - services - The srvices object
    ///   - databases - Database config object
    ///
    public static func setup(
        config: BackgrounderConfig,
        services: inout Services,
        databases: inout DatabasesConfig) throws {
        
        var commands = CommandConfig.default()
        
        try self.setup(
            config: config,
            services: &services,
            databases: &databases,
            commands: &commands)
        
        services.register(commands)
    }
    
    /// This method instantiates the backgrounder instance
    ///
    ///
    /// - parameters:
    ///   - config - The configuration for the backgrounder
    ///   - services - The srvices object
    ///
    public static func setup(
        config: BackgrounderConfig,
        services: inout Services) throws {
        
        var databases = DatabasesConfig()
        var commands = CommandConfig.default()
        
        try self.setup(
            config: config,
            services: &services,
            databases: &databases,
            commands: &commands)
        
        services.register(databases)
        services.register(commands)
    }
    
    /// This method create the Redis connection
    ///
    /// - note: This can be used to setup other Redis connections as well
    ///
    public static func setupRedis(
        redisUrl: String = BackgrounderConfig.defaultRedisUrl,
        as identifier: RedisIdentifier,
        services: inout Services,
        databases: inout DatabasesConfig) throws {
        
        // Add the database kit
        try services.register(DatabaseKitProvider())
        
        // Add the database
        databases.add(database: try RedisDatabase(url: URL(string: redisUrl)!), as: identifier)
        
        // Add the pooled connection
        services.register(KeyedCache.self) { container -> RedisCache in
            let pool = try container.connectionPool(to: identifier)
            return .init(pool: pool)
        }
    }
    
    /// This method create the Redis connection
    ///
    /// - note: This can be used to setup other Redis connections as well
    ///
    public static func setupRedis(
        redisUrl: String = BackgrounderConfig.defaultRedisUrl,
        as identifier: DatabaseIdentifier<RedisDatabase>,
        services: inout Services) throws {
        
        var databases = DatabasesConfig()
        
        // Setup Redis
        try self.setupRedis(
            redisUrl: redisUrl,
            as: identifier,
            services: &services,
            databases: &databases)
        
        services.register(databases)
    }
    
}
