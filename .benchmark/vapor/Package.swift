// swift-tools-version:5.0
import PackageDescription

let package = Package(
    name: "BackgrounderBenchmark",
    products: [
        .library(name: "BackgrounderBenchmark", targets: ["App"]),
    ],
    dependencies: [
        // Backgrounder
        .package(path: "../../../Backgrounder"),
    ],
    targets: [
        .target(name: "App", dependencies: ["Backgrounder"]),
        .target(name: "Run", dependencies: ["App"]),
        .testTarget(name: "AppTests", dependencies: ["App"])
    ]
)

