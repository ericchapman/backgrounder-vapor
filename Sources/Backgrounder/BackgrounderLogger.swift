import Vapor

public class BackgrounderLogger: Logger {

    /// The log level
    let level: LogLevel
    
    /// The prefix for the log
    let prefix: String
    
    /// If the logging is detailed or not
    let detailed: Bool

    /// Constructor
    public init(level: LogLevel, prefix: String?=nil, detailed: Bool=true) {
        self.level = level
        self.prefix = prefix != nil ? "\(prefix!) " : ""
        self.detailed = detailed
    }
    
    /// Implement the logger
    public func log(_ string: String, at level: LogLevel, file: String, function: String, line: UInt, column: UInt) {
        if level.intValue >= self.level.intValue {
            if detailed {
                print("\(Date().iso8601) \(self.prefix)\(level.description): \(string)")
            }
            else {
                print("\(self.prefix)\(level.description): \(string)")
            }
        }
    }
}

extension LogLevel {
    /// See `CustomStringConvertible`
    public var intValue: Int {
        switch self {
        case .custom(_): return 1
        case .verbose: return 2
        case .debug: return 3
        case .info: return 4
        case .warning: return 5
        case .error: return 6
        case .fatal: return 7
        }
    }
}
