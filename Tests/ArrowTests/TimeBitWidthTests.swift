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
import FlatBuffers
@testable import Arrow

final class TimeBitWidthTests: XCTestCase {
    private func timeBitWidth(_ arrowType: ArrowType) throws -> Int32 {
        let schema = ArrowSchema.Builder()
            .addField("col", type: arrowType, isNullable: false)
            .finish()
        let writer = ArrowWriter()
        let data: Data
        switch writer.toMessage(schema) {
        case .success(let result): data = result
        case .failure(let error): throw error
        }
        var buffer = ByteBuffer(data: data)
        let message: org_apache_arrow_flatbuf_Message = getRoot(byteBuffer: &buffer)
        let fbSchema: org_apache_arrow_flatbuf_Schema = message.header(type: org_apache_arrow_flatbuf_Schema.self)!
        let field = fbSchema.fields(at: 0)!
        let timeType: org_apache_arrow_flatbuf_Time = field.type(type: org_apache_arrow_flatbuf_Time.self)!
        return timeType.bitWidth
    }

    func testTimeBitWidthSerialization() throws {
        XCTAssertEqual(try timeBitWidth(ArrowTypeTime64(.nanoseconds)), 64)
        XCTAssertEqual(try timeBitWidth(ArrowTypeTime64(.microseconds)), 64)
        XCTAssertEqual(try timeBitWidth(ArrowTypeTime32(.milliseconds)), 32)
        XCTAssertEqual(try timeBitWidth(ArrowTypeTime32(.seconds)), 32)
    }
}
