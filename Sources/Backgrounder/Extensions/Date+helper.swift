import Foundation

extension Date {
    
    /// Returns the date in seconds from Epoch
    public var toEpoch: TimeInterval {
        return self.timeIntervalSince1970
    }
    
    /// Returns the date in YYYY-MM-dd
    var toStatDate: String {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd"
        return dateFormatter.string(from: self)
    }
    
    /// Returns a random time using the passed in interval
    ///
    /// - parameters:
    ///   - interval: Number of seconds to pick a random time between
    /// - returns: A new date that is a random time from now
    ///
    public static func random(in interval: Int) -> Date {
        return Date().addingTimeInterval(TimeInterval(Int.random(in: 1..<interval)))
    }
}

extension Int {
    
    /// Returns a time interval that represents 'self' seconds
    public var seconds: TimeInterval {
        return TimeInterval(self)
    }
    
    /// Returns a time interval that represents 'self' minutes
    public var minutes: TimeInterval {
        return TimeInterval(60*self)
    }
    
    /// Returns a time interval that represents 'self' hours
    public var hours: TimeInterval {
        return TimeInterval(60*self.minutes)
    }
    
    /// Returns a time interval that represents 'self' days
    public var days: TimeInterval {
        return TimeInterval(24*self.hours)
    }
    
    /// Returns a time interval that represents 'self' weeks
    public var weeks: TimeInterval {
        return TimeInterval(7*self.days)
    }
    
    /// Returns a time interval that represents 'self' months
    public var months: TimeInterval {
        return TimeInterval(30*self.days)
    }
    
    /// Returns a time interval that represents 'self' years
    public var years: TimeInterval {
        return TimeInterval(365*self.days)
    }
}

extension TimeInterval {
    
    /// Returns the date using seconds from epoch
    public var fromEpoch: Date {
        return Date(timeIntervalSince1970: self)
    }
    
    /// Returns a date that is 'now' - 'time interval'
    public var ago: Date {
        return Date(timeIntervalSinceNow: -self)
    }
    
    /// Returns a date that is 'now' + 'time interval'
    public var fromNow: Date {
        return Date(timeIntervalSinceNow: self)
    }
}
