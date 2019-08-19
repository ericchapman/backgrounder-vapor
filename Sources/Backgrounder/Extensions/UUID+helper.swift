
import Foundation

extension UUID {
    /// Returns a 9 character uuid
    var TID: String {
        return String(self.uuidString.replacingOccurrences(of: "-", with: "").suffix(9)).lowercased()
    }
    
    /// Returns a 24 character uuid
    var JID: String {
        return String(self.uuidString.replacingOccurrences(of: "-", with: "").suffix(24)).lowercased()
    }
}
