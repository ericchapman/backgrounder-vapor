import Foundation

extension Date {
    /// Returns the date in seconds from Epoch
    var toEpoch: TimeInterval {
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
    static func random(in interval: Int) -> Date {
        return Date().addingTimeInterval(TimeInterval(Int.random(in: 1..<interval)))
    }
}

extension TimeInterval {
    /// Returns the date using seconds from epoch
    var fromEpoch: Date {
        return Date(timeIntervalSince1970: self)
    }
}
