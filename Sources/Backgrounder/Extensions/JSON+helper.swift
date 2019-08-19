//
//  Any+helper.swift
//  App
//
//  Created by Eric Chapman on 7/25/19.
//

import Foundation

extension Array where Element == Any {
    
    /// Serializes the object
    ///
    /// - returns: A json string with the serialized array
    ///
    func toJson() -> String {
        let jsonData = try! JSONSerialization.data(withJSONObject: self, options: [])
        return String(data: jsonData, encoding: .utf8) ?? "[]"
    }
}

extension Dictionary where Key == String, Value == Any {
    
    /// Serializes the object
    ///
    /// - returns: A json string with the serialized object
    ///
    func toJson() -> String {
        let jsonData = try! JSONSerialization.data(withJSONObject: self, options: [])
        return String(data: jsonData, encoding: .utf8) ?? "{}"
    }
    
}

extension Data {
    
    /// Deserializes the object
    ///
    /// - returns: The deserialized object
    ///
    func fromJson<T>() -> T? {
        return try! JSONSerialization.jsonObject(with: self, options: []) as? T
    }

}

extension String {
    
    /// Deserializes the object
    ///
    /// - returns: The deserialized object
    ///
    func fromJson<T>() -> T? {
        return self.data(using: .utf8)?.fromJson()
    }
    
}
