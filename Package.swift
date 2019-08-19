// swift-tools-version:4.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Backgrounder",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(name: "Backgrounder", targets: ["Backgrounder"]),
    ],
    dependencies: [
        // 💧 A server-side Swift web framework.
        .package(url: "https://github.com/vapor/vapor.git", from: "3.0.0"),
        
        // ⚡️ Non-blocking, event-driven Redis client.
        .package(url: "https://github.com/vapor/redis.git", from: "3.4.0"),
        
        // Redis Api
        .package(url: "https://github.com/ericchapman/vapor-redis-api.git", from: "0.3.0")
    ],
    targets: [
        // Backgrounder targets
        .target(name: "Backgrounder", dependencies: ["RedisApi", "Redis", "Vapor"]),
        .testTarget(name: "BackgrounderTests", dependencies: ["Backgrounder"]),
    ]
)
