import Foundation

/// Simple struct for storing real time counters
///
struct BackgrounderCounter {
    /// The value of the counter
    var value = 0
    
    /// Incremants the value of the counter
    ///
    mutating func increment(by count: Int=1) {
        self.value += count
    }
    
    /// Resets the counter and returns the last value
    ///
    mutating func reset() -> Int {
        let copy = self.value
        self.value = 0
        return copy
    }
}
