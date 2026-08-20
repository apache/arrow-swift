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

import Foundation
import XCTest

/// Locates files in the `apache/arrow-testing` corpus.
///
/// The corpus is resolved from, in order:
/// 1. The `ARROW_TEST_DATA` environment variable.
/// 2. The `testing/data` submodule directory.
///
/// When neither is available the corpus is treated as absent and callers skip,
/// so that `swift test` succeeds on a checkout where the submodule has not
/// been initialised, and when tests run against an extracted source archive.
enum ArrowTestData {
    /// Root of the corpus, or `nil` when it cannot be located.
    static var root: URL? {
        let manager = FileManager.default

        if let path = ProcessInfo.processInfo.environment["ARROW_TEST_DATA"], !path.isEmpty {
            let url = URL(fileURLWithPath: path, isDirectory: true)
            return manager.fileExists(atPath: url.path) ? url : nil
        }

        let fallback = repositoryRoot
            .appendingPathComponent("testing", isDirectory: true)
            .appendingPathComponent("data", isDirectory: true)
        return manager.fileExists(atPath: fallback.path) ? fallback : nil
    }

    /// Absolute URL of a file inside the corpus, for example
    /// `arrow-ipc-stream/integration/cpp-21.0.0/generated_primitive.stream`.
    ///
    /// Throws `XCTSkip` when the corpus is unavailable. Fails the calling test
    /// when the corpus is present but does not contain the requested file,
    /// which indicates a stale submodule rather than a missing corpus.
    static func url(
        _ relativePath: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> URL {
        guard let root else {
            throw XCTSkip(
                """
                arrow-testing corpus not found. Run \
                'git submodule update --init', or set ARROW_TEST_DATA to the \
                data directory of an apache/arrow-testing checkout.
                """)
        }

        let url = root.appendingPathComponent(relativePath)
        if !FileManager.default.fileExists(atPath: url.path) {
            XCTFail(
                "Missing file in arrow-testing corpus: \(relativePath)",
                file: file,
                line: line)
        }
        return url
    }

    /// This file is at `<repository>/Tests/ArrowTests/ArrowTestData.swift`, so
    /// the repository root is three levels up. `#filePath` is used rather than
    /// `#file` because the latter can be shortened to a bare file name
    /// depending on compiler settings.
    private static var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

final class ArrowTestDataTests: XCTestCase {
    /// Verifies path resolution only. Reading and validating corpus contents
    /// is deliberately out of scope here.
    func testResolvesCorpusPath() throws {
        let url = try ArrowTestData.url(
            "arrow-ipc-stream/integration/cpp-21.0.0/generated_primitive.stream")
        XCTAssertTrue(FileManager.default.fileExists(atPath: url.path))
    }
}
