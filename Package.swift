// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import PackageDescription

let package = Package(
    name: "ArrowSwift",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        .library(
            name: "Arrow",
            targets: ["Arrow"])
    ],
    dependencies: [
        .package(url: "https://github.com/google/flatbuffers.git", branch: "v25.2.10"),
        .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.26.1"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.30.0"),
        .package(
            url: "https://github.com/apple/swift-atomics.git",
            .upToNextMajor(from: "1.2.0") // or `.upToNextMinor
        )
    ],
    targets: [
        .target(
            name: "ArrowC",
            path: "Arrow/Sources/ArrowC",
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]

        ),
        .target(
            name: "Arrow",
            dependencies: ["ArrowC",
                           .product(name: "FlatBuffers", package: "flatbuffers"),
                           .product(name: "Atomics", package: "swift-atomics")
            ],
            path: "Arrow/Sources/Arrow",
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .target(
            name: "ArrowFlight",
            dependencies: [
                "Arrow",
                .product(name: "GRPC", package: "grpc-swift"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf")
            ],
            path: "ArrowFlight/Sources/ArrowFlight",
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .testTarget(
            name: "ArrowTests",
            dependencies: ["Arrow", "ArrowC"],
            path: "Arrow/Tests",
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .testTarget(
            name: "ArrowFlightTests",
            dependencies: ["ArrowFlight"],
            path: "ArrowFlight/Tests",
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        )

    ]
)
