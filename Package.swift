// swift-tools-version: 6.1
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
    name: "Arrow",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        .library(
            name: "Arrow",
            targets: ["Arrow"]),
        .library(
            name: "ArrowFlight",
            targets: ["ArrowFlight"])
    ],
    dependencies: [
        .package(url: "https://github.com/google/flatbuffers.git", exact: "25.12.19"),
        .package(url: "https://github.com/grpc/grpc-swift-2.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.35.1"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.3.0")
    ],
    targets: [
        .target(
            name: "ArrowC",
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
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .target(
            name: "ArrowFlight",
            dependencies: [
                "Arrow",
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf")
            ],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .testTarget(
            name: "ArrowTests",
            dependencies: ["Arrow", "ArrowC"],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .testTarget(
            name: "ArrowFlightTests",
            dependencies: [
                "ArrowFlight",
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport")
            ],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        )

    ],
    // TODO: The package is not yet Swift 6 language mode clean; migrate gradually.
    swiftLanguageModes: [.v5]
)
