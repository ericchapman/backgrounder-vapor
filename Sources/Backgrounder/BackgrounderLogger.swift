import Vapor

public class BackgrounderLogger: Logger {

    /// The log level
    let level: LogLevel
    
    /// The prefix for the log
    let prefix: String?

    /// Constructor
    public init(level: LogLevel, prefix: String?=nil) {
        self.level = level
        self.prefix = prefix
    }
    
    /// Implement the logger
    public func log(_ string: String, at level: LogLevel, file: String, function: String, line: UInt, column: UInt) {
        if level.intValue >= self.level.intValue {
            print("\(Date().iso8601) [\(level.description)]: Backgrounder\(self.prefix != nil ? " \(self.prefix!)" : "") - \(string)")
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
