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

import Arrow
import Foundation

extension ArrowJSONIntegrationTest {

    /// Encode a column (array + field) to the Arrow testing JSON format.
    func encodeColumn(
        holder: ArrowArrayHolder,
        field: ArrowField
    ) throws -> ArrowJSON.Column {

        let arrowData = holder.data

        let length = Int(holder.length)
        let indices = (UInt(0)..<UInt(length))

        // Validity is always present in the Arrow JSON files.
        let validity: [Int] = indices.map { arrowData.isNull($0) ? 0 : 1 }

        var offsets: [Int64]?
        var dataValue: [DataValue]? = []
        var children: [ArrowJSON.Column]?

        switch field.type.id {
        case .boolean:
            guard let arr = holder.array as? BoolArray else {
                throw ArrowError.invalid("Expected BoolArray")
            }
            dataValue = indices.map { arr[$0].map(DataValue.bool) ?? .null }

        case .int8:
            dataValue = try extractFixedData(holder.array, indices: indices, as: Int8.self)
        case .int16:
            dataValue = try extractFixedData(holder.array, indices: indices, as: Int16.self)
        case .int32:
            dataValue = try extractFixedData(holder.array, indices: indices, as: Int32.self)
        case .int64:
            dataValue = try extractFixedData(
                holder.array, indices: indices, as: Int64.self, as64BitString: true)
        case .uint8:
            dataValue = try extractFixedData(holder.array, indices: indices, as: UInt8.self)
        case .uint16:
            dataValue = try extractFixedData(holder.array, indices: indices, as: UInt16.self)
        case .uint32:
            dataValue = try extractFixedData(holder.array, indices: indices, as: UInt32.self)
        case .uint64:
            dataValue = try extractFixedData(
                holder.array, indices: indices, as: UInt64.self, as64BitString: true)
        case .float:
            dataValue = try extractFloatData(holder.array, indices: indices, as: Float.self)
        case .double:
            dataValue = try extractFloatData(holder.array, indices: indices, as: Double.self)
        case .date32:
            guard let arr = holder.array as? Date32Array else {
                throw ArrowError.invalid("Expected Date32Array")
            }
            dataValue = indices.map { idx in
                guard let date = arr[idx] else { return .null }
                return .int(Int((date.timeIntervalSince1970 / 86_400).rounded()))
            }
        case .date64:
            guard let arr = holder.array as? Date64Array else {
                throw ArrowError.invalid("Expected Date64Array")
            }
            dataValue = indices.map { idx in
                guard let date = arr[idx] else { return .null }
                let millis = Int64((date.timeIntervalSince1970 * 1000).rounded())
                return .string("\(millis)")
            }
        case .time32:
            dataValue = try extractFixedData(holder.array, indices: indices, as: Int32.self)
        case .time64:
            dataValue = try extractFixedData(
                holder.array, indices: indices, as: Int64.self, as64BitString: true)
        case .timestamp:
            dataValue = try extractFixedData(
                holder.array, indices: indices, as: Int64.self, as64BitString: true)
        case .string:
            guard let arr = holder.array as? StringArray else {
                throw ArrowError.invalid("Expected StringArray")
            }
            offsets = try readInt32Offsets(arrowData, count: length)
            dataValue = indices.map { arr[$0].map(DataValue.string) ?? .null }

        case .binary:
            guard let arr = holder.array as? BinaryArray else {
                throw ArrowError.invalid("Expected BinaryArray")
            }
            offsets = try readInt32Offsets(arrowData, count: length)
            dataValue = indices.map { idx in
                guard let val = arr[idx] else { return .null }
                return .string(val.map { String(format: "%02X", $0) }.joined())
            }

        case .strct:
            throw ArrowError.notImplemented
        case .list:
            throw ArrowError.notImplemented
        default:
            throw ArrowError.notImplemented
        }

        return .init(
            name: field.name,
            count: length,
            validity: validity,
            offset: offsets,
            data: dataValue,
            children: children
        )
    }

    // MARK: - Helpers

    func extractFixedData<T: FixedWidthInteger>(
        _ array: AnyArray,
        indices: Range<UInt>,
        as type: T.Type,
        as64BitString: Bool = false
    ) throws -> [DataValue] {
        guard let arr = array as? ArrowArray<T> else {
            throw ArrowError.invalid("Expected ArrowArray<\(T.self)>")
        }
        return indices.map { idx in
            guard let val = arr[idx] else { return .null }
            return as64BitString ? .string("\(val)") : .int(Int(val))
        }
    }

    func extractFloatData<T: BinaryFloatingPoint & Codable>(
        _ array: AnyArray,
        indices: Range<UInt>,
        as type: T.Type
    ) throws -> [DataValue] {
        guard let arr = array as? ArrowArray<T> else {
            throw ArrowError.invalid("Expected ArrowArray<\(T.self)>")
        }
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        return try indices.map { idx in
            guard let value = arr[idx] else { return .null }
            let jsonNumber = try decoder.decode(T.self, from: encoder.encode(value))
            return .string("\(jsonNumber)")
        }
    }

    func readInt32Offsets(_ arrowData: ArrowData, count: Int) throws -> [Int64] {
        guard arrowData.buffers.count > 1 else {
            throw ArrowError.invalid("Missing offsets buffer")
        }
        let offsetsBuffer = arrowData.buffers[1]
        return (0...count).map { idx in
            Int64(
                offsetsBuffer.rawPointer.advanced(by: idx * MemoryLayout<Int32>.stride)
                    .load(as: Int32.self))
        }
    }
}
