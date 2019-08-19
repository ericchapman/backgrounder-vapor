//
//  Array+helper.swift
//  Async
//
//  Created by Eric Chapman on 7/30/19.
//

import Foundation

extension Array where Element == String {
    
    /// Returns the array with duplicate values filtered out
    ///
    /// - returns: Array of unique string items
    ///
    func unique() -> [String] {
        var result = [String]()
        for queue in self {
            if !result.contains(queue) {
                result.append(queue)
            }
        }
        return result
    }
}
