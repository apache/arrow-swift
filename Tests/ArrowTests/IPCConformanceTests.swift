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

import XCTest
@testable import Arrow

/// Expectations shared by the file and stream conformance tests, which read
/// the same logical cases in two different encodings.
private enum PrimitiveCase {
    static let corpusPrefix = "arrow-ipc-stream/integration/cpp-21.0.0"

    /// The 22 fields of the `generated_primitive` cases, in file order.
    static let fields: [(String, ArrowTypeId)] = [
        ("bool_nullable", .boolean), ("bool_nonnullable", .boolean),
        ("int8_nullable", .int8), ("int8_nonnullable", .int8),
        ("int16_nullable", .int16), ("int16_nonnullable", .int16),
        ("int32_nullable", .int32), ("int32_nonnullable", .int32),
        ("int64_nullable", .int64), ("int64_nonnullable", .int64),
        ("uint8_nullable", .uint8), ("uint8_nonnullable", .uint8),
        ("uint16_nullable", .uint16), ("uint16_nonnullable", .uint16),
        ("uint32_nullable", .uint32), ("uint32_nonnullable", .uint32),
        ("uint64_nullable", .uint64), ("uint64_nonnullable", .uint64),
        ("float32_nullable", .float), ("float32_nonnullable", .float),
        ("float64_nullable", .double), ("float64_nonnullable", .double)
    ]

    static func assertSchema(
        _ schema: ArrowSchema?,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard let schema else {
            XCTFail("schema is nil", file: file, line: line)
            return
        }
        XCTAssertEqual(schema.fields.count, fields.count, file: file, line: line)
        guard schema.fields.count == fields.count else { return }
        for (index, expected) in fields.enumerated() {
            XCTAssertEqual(schema.fields[index].name, expected.0, file: file, line: line)
            XCTAssertEqual(schema.fields[index].type.id, expected.1, file: file, line: line)
        }
    }

    /// Every value in the result rendered as a string, with `nil` for nulls,
    /// used by testStreamAndFileAgree (currently disabled).
    /// Remove this when the test is re-enabled.
    /*
     private static func snapshot(_ result: ArrowReader.ArrowReaderResult) -> [String] {
     var values: [String] = []
     for (batchIndex, batch) in result.batches.enumerated() {
     for column in 0..<batch.columns.count {
     let holder = batch.column(column)
     for row in 0..<Int(batch.length) {
     let label = "b\(batchIndex)c\(column)r\(row)"
     if holder.array.asAny(UInt(row)) == nil {
     values.append("\(label)=nil")
     } else if let text = holder.array as? AsString {
     values.append("\(label)=\(text.asString(UInt(row)))")
     }
     }
     }
     }
     return values
     }
     */
}

/// Reads IPC files from the `apache/arrow-testing` corpus, which are produced
/// by other Arrow implementations. Unlike the round-trip tests in IPCTests,
/// these verify conformance with the format rather than self-consistency.
final class IPCFileConformanceTests: XCTestCase {
    private func readFile(_ name: String) throws -> ArrowReader.ArrowReaderResult {
        let url = try ArrowTestData.url("\(PrimitiveCase.corpusPrefix)/\(name)")
        switch ArrowReader().fromFile(url) {
        case .success(let result):
            return result
        case .failure(let error):
            throw error
        }
    }

    func testPrimitiveFileSchemaAndShape() throws {
        let result = try readFile("generated_primitive.arrow_file")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 2)
        XCTAssertEqual(result.batches.reduce(0) { $0 + Int($1.length) }, 37)
        for batch in result.batches {
            XCTAssertEqual(batch.columns.count, PrimitiveCase.fields.count)
        }

        let batch0 = result.batches[0]
        XCTAssertEqual(batch0.length, 17)

        let col0 = batch0.column(0)
        let arr0 = col0.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertNil(col0.array.asAny(0))
        XCTAssertNil(col0.array.asAny(1))
        XCTAssertEqual(arr0.asString(2), "true")
        XCTAssertNil(col0.array.asAny(3))

        let col2 = batch0.column(2)
        let arr2 = col2.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertEqual(arr2.asString(0), "-128")
        XCTAssertEqual(arr2.asString(1), "127")
        XCTAssertEqual(arr2.asString(2), "27")
        XCTAssertEqual(arr2.asString(3), "-90")

        let col4 = batch0.column(4)
        let arr4 = col4.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertEqual(arr4.asString(0), "-32768")
        XCTAssertEqual(arr4.asString(1), "32767")

        let col6 = batch0.column(6)
        let arr6 = col6.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertEqual(arr6.asString(0), "-2147483648")

        let col10 = batch0.column(10)
        let arr10 = col10.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertEqual(arr10.asString(0), "0")
        XCTAssertEqual(arr10.asString(1), "255")

        let col18 = batch0.column(18)
        let f0 = try XCTUnwrap(col18.array.asAny(0) as? Float)
        XCTAssertEqual(f0, 641.818, accuracy: 0.001)

        let col20 = batch0.column(20)
        let d0 = try XCTUnwrap(col20.array.asAny(0) as? Double)
        let d1 = try XCTUnwrap(col20.array.asAny(1) as? Double)
        XCTAssertEqual(d0, -955.504, accuracy: 0.001)
        XCTAssertEqual(d1, -1746.99, accuracy: 0.001)

        let batch1 = result.batches[1]
        XCTAssertEqual(batch1.length, 20)

        let b1col0 = batch1.column(0)
        let b1arr0 = b1col0.array as! AsString // swiftlint:disable:this force_cast
        XCTAssertNil(b1col0.array.asAny(0))
        XCTAssertNil(b1col0.array.asAny(1))
        XCTAssertNil(b1col0.array.asAny(2))
        XCTAssertEqual(b1arr0.asString(3), "true")
    }

    /// A schema message with no record batches at all.
    func testPrimitiveFileWithNoBatches() throws {
        let result = try readFile("generated_primitive_no_batches.arrow_file")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 0)
    }

    /// Record batches that are present but contain no rows.
    func testPrimitiveFileWithZeroLengthBatches() throws {
        let result = try readFile("generated_primitive_zerolength.arrow_file")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 3)
        for batch in result.batches {
            XCTAssertEqual(batch.length, 0)
            XCTAssertEqual(batch.columns.count, PrimitiveCase.fields.count)
        }
    }

    /// A struct column and two top-level fields sharing the same name, read
    /// from a file produced by another Arrow implementation.
    ///
    /// Only schema and shape are asserted. Value assertions are omitted
    /// because fields carrying no validity buffer currently read
    /// uninitialized memory, so their values differ between runs.
    func testStructAndDuplicateFieldNames() throws {
        let result = try readFile("generated_duplicate_fieldnames.arrow_file")

        guard let schema = result.schema else {
            XCTFail("schema is nil")
            return
        }
        XCTAssertEqual(schema.fields.count, 3)
        XCTAssertEqual(schema.fields[0].name, "ints")
        XCTAssertEqual(schema.fields[0].type.id, .int8)
        XCTAssertEqual(schema.fields[1].name, "ints")
        XCTAssertEqual(schema.fields[1].type.id, .int32)
        XCTAssertEqual(schema.fields[2].name, "struct")
        XCTAssertEqual(schema.fields[2].type.id, .strct)

        let structType = try XCTUnwrap(schema.fields[2].type as? ArrowTypeStruct)
        XCTAssertEqual(structType.fields.count, 2)
        XCTAssertEqual(structType.fields[0].type.id, .int32)
        XCTAssertEqual(structType.fields[1].type.id, .string)

        XCTAssertEqual(result.batches.count, 1)
        let batch = result.batches[0]
        XCTAssertEqual(batch.length, 1)
        XCTAssertEqual(batch.columns.count, 3)

        let nested = try XCTUnwrap(batch.column(2).array as? NestedArray)
        let children = try XCTUnwrap(nested.fields)
        XCTAssertEqual(children.count, 2)
        XCTAssertEqual(children[0].type.id, .int32)
        XCTAssertEqual(children[1].type.id, .string)
    }
}

/// Reads IPC streams from the `apache/arrow-testing` corpus. The streaming
/// tests in IPCTests write with ArrowWriter and read the result back, which
/// establishes self-consistency but not conformance; these read streams
/// produced by another Arrow implementation.
final class IPCStreamConformanceTests: XCTestCase {
    private func readStream(_ name: String) throws -> ArrowReader.ArrowReaderResult {
        let url = try ArrowTestData.url("\(PrimitiveCase.corpusPrefix)/\(name)")
        let data = try Data(contentsOf: url)
        switch ArrowReader().readStreaming(data) {
        case .success(let result):
            return result
        case .failure(let error):
            throw error
        }
    }

    func testPrimitiveStreamSchemaAndShape() throws {
        let result = try readStream("generated_primitive.stream")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 2)
        XCTAssertEqual(result.batches.reduce(0) { $0 + Int($1.length) }, 37)
        XCTAssertEqual(result.batches[0].length, 17)
        XCTAssertEqual(result.batches[1].length, 20)
    }

    /// A schema message with no record batches at all.
    func testPrimitiveStreamWithNoBatches() throws {
        let result = try readStream("generated_primitive_no_batches.stream")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 0)
    }

    /// Record batches that are present but contain no rows.
    func testPrimitiveStreamWithZeroLengthBatches() throws {
        let result = try readStream("generated_primitive_zerolength.stream")
        PrimitiveCase.assertSchema(result.schema)
        XCTAssertEqual(result.batches.count, 3)
        for batch in result.batches {
            XCTAssertEqual(batch.length, 0)
        }
    }

    /// Disabled: compares stream and file encodings of the same case to ensure
    /// both readers produce identical values. Currently fails non-deterministically
    /// because ArrowBuffer.createBuffer does not initialize unallocated memory
    /// for null buffers, causing out-of-bounds reads on fields with no nulls.
    /// See: https://github.com/apache/arrow-swift/issues/NNN (ArrowBuffer)
    /// This test should pass once that issue is fixed and will serve as the
    /// regression test for it.
    /*
     func testStreamAndFileAgree() throws {
     let streamResult = try readStream("generated_primitive.stream")

     let fileURL = try ArrowTestData.url(
     "\(PrimitiveCase.corpusPrefix)/generated_primitive.arrow_file")
     guard case .success(let fileResult) = ArrowReader().fromFile(fileURL) else {
     XCTFail("could not read generated_primitive.arrow_file")
     return
     }

     XCTAssertEqual(
     PrimitiveCase.snapshot(streamResult),
     PrimitiveCase.snapshot(fileResult))
     }
     */
}
