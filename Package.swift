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
    name: "Arrow",
    platforms: [
        .macOS(.v10_15),
    ],
    products: [
        .library(
            name: "Arrow",
            targets: ["Arrow"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/google/flatbuffers.git", from: "25.2.10"),
        .package(url: "https://github.com/grpc/grpc-swift.git", from: "1.25.0"),
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.29.0"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.3.0"),
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
            dependencies: [
                .target(name: "ArrowC"),
                .product(name: "FlatBuffers", package: "flatbuffers"),
                .product(name: "Atomics", package: "swift-atomics"),
            ],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .target(
            name: "ArrowFlight",
            dependencies: [
                .target(name: "Arrow"),
                .product(name: "GRPC", package: "grpc-swift"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
            ],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .target(
            name: "go-swift",
            dependencies: [
                .target(name: "Arrow"),
            ]
        ),
        .testTarget(
            name: "ArrowTests",
            dependencies: ["Arrow", "ArrowC"],
            resources: [
                .process("Resources/testdata_double.arrow"),
                .process("Resources/testdata_bool.arrow"),
                .process("Resources/testdata_struct.arrow"),
            ],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
        .testTarget(
            name: "ArrowFlightTests",
            dependencies: ["ArrowFlight"],
            swiftSettings: [
                // build: .unsafeFlags(["-warnings-as-errors"])
            ]
        ),
    ]
)
