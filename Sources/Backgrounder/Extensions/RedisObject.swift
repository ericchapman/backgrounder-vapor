//
//  RedisObject.swift
//  Backgrounder
//
//  Created by Eric Chapman on 7/27/19.
//

import Foundation

public protocol RedisObject: class {
    associatedtype T:RedisObject
    
    /// Serialed version of the job
    var redisData: String? { set get }
    
    /// Returns the object as a Redis object
    var toRedis: String { get }

    /// Deserializes from Redis
    static func fromRedis(_ string: String) throws -> T
    
    /// Converte the job to a hash that can be serialized
    func toHash() -> [String:Any]
    
    /// Creates the job from a hash
    static func fromHash(hash: [String:Any]) -> T?
}

extension RedisObject {
    
    public static func fromRedis(_ string: String) throws -> T {
        // Desrialize
        let hash = string.fromJson() ?? [String: Any]()
        
        // Create the job
        let object = Self.fromHash(hash: hash)!
        object.redisData = string
        
        return object
    }
    
    public var toRedis: String {
        if self.redisData == nil {
            self.redisData = self.toHash().toJson()
        }
        return self.redisData!
    }
}
