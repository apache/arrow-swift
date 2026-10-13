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
import Arrow

extension ArrowJSONIntegrationTest {

    /// Reads an Arrow IPC file and encodes it into the gold JSON model.
    func decodeArrowJSON(from arrowURL: URL) throws -> ArrowJSON {
        let result: ArrowReader.ArrowReaderResult
        switch ArrowReader().fromFile(arrowURL) {
        case .success(let res): result = res
        case .failure(let error): throw error
        }

        guard let schema = result.schema else {
            throw ArrowError.invalid("Arrow file is missing a schema")
        }

        return ArrowJSON(
            schema: try encodeSchema(schema),
            batches: try encode(batches: result.batches, schema: schema),
            dictionaries: nil
        )
    }

    func buildHolder(column: ArrowJSON.Column, type: ArrowType) throws -> ArrowArrayHolder {
        let builder = try ArrowArrayBuilders.loadBuilder(arrowType: type)
        let data = column.data ?? []
        let validity = column.validity ?? Array(repeating: 1, count: column.count)

        for idx in 0..<column.count {
            if validity[idx] == 0 {
                builder.appendAny(nil)
            } else {
                builder.appendAny(try nativeValue(data[idx], typeId: type.id))
            }
        }
        return try builder.toHolder()
    }

    /// Reconstructs an `ArrowType` from the JSON type object.
    func decodeArrowType(_ field: ArrowJSON.Field) throws -> ArrowType {
        switch field.type.name {
        case "bool":
            return ArrowType(ArrowType.ArrowBool)

        case "int":
            guard let bitWidth = field.type.bitWidth, let isSigned = field.type.isSigned else {
                throw ArrowError.invalid("int type missing bitWidth/isSigned")
            }
            switch (bitWidth, isSigned) {
            case (8, true): return ArrowType(ArrowType.ArrowInt8)
            case (16, true): return ArrowType(ArrowType.ArrowInt16)
            case (32, true): return ArrowType(ArrowType.ArrowInt32)
            case (64, true): return ArrowType(ArrowType.ArrowInt64)
            case (8, false): return ArrowType(ArrowType.ArrowUInt8)
            case (16, false): return ArrowType(ArrowType.ArrowUInt16)
            case (32, false): return ArrowType(ArrowType.ArrowUInt32)
            case (64, false): return ArrowType(ArrowType.ArrowUInt64)
            default: throw ArrowError.invalid("Unsupported int width \(bitWidth)")
            }

        case "floatingpoint":
            switch field.type.precision {
            case "SINGLE": return ArrowType(ArrowType.ArrowFloat)
            case "DOUBLE": return ArrowType(ArrowType.ArrowDouble)
            default: throw ArrowError.notImplemented  // HALF: no float16 support upstream yet
            }

        case "utf8": return ArrowType(ArrowType.ArrowString)
        case "binary": return ArrowType(ArrowType.ArrowBinary)

        case "date":
            switch field.type.unit {
            case "DAY": return ArrowType(ArrowType.ArrowDate32)
            case "MILLISECOND": return ArrowType(ArrowType.ArrowDate64)
            default: throw ArrowError.invalid("Unknown date unit")
            }

        case "time":
            guard let bitWidth = field.type.bitWidth, let unit = field.type.unit else {
                throw ArrowError.invalid("time type missing bitWidth/unit")
            }
            return bitWidth == 32
                ? ArrowTypeTime32(unit == "SECOND" ? .seconds : .milliseconds)
                : ArrowTypeTime64(unit == "MICROSECOND" ? .microseconds : .nanoseconds)

        case "timestamp":
            guard let unit = field.type.unit else {
                throw ArrowError.invalid("timestamp type missing unit")
            }
            let tsUnit: ArrowTimestampUnit
            switch unit {
            case "SECOND": tsUnit = .seconds
            case "MILLISECOND": tsUnit = .milliseconds
            case "MICROSECOND": tsUnit = .microseconds
            case "NANOSECOND": tsUnit = .nanoseconds
            default: throw ArrowError.invalid("Unknown timestamp unit \(unit)")
            }
            return ArrowTypeTimestamp(tsUnit, timezone: field.type.timezone)

        default:
            // struct and list deferred.
            throw ArrowError.notImplemented
        }
    }

    func nativeValue(_ value: DataValue, typeId: ArrowTypeId) throws -> Any? {
        func intVal() throws -> Int {
            guard case .int(let intVal) = value else {
                throw ArrowError.invalid("Expected int, got \(value)")
            }
            return intVal
        }
        func strVal() throws -> String {
            guard case .string(let strVal) = value else {
                throw ArrowError.invalid("Expected string, got \(value)")
            }
            return strVal
        }
        func exact<T: FixedWidthInteger>(_ type: T.Type) throws -> T {
            let intVal = try intVal()
            guard let val = T(exactly: intVal) else {
                throw ArrowError.invalid("\(intVal) doesn't fit in \(T.self)")
            }
            return val
        }

        switch typeId {
        case .boolean:
            guard case .bool(let val) = value else {
                throw ArrowError.invalid("Expected bool, got \(value)")
            }
            return val
        case .int8: return try exact(Int8.self)
        case .int16: return try exact(Int16.self)
        case .int32: return try exact(Int32.self)
        case .int64: return Int64(try strVal())
        case .uint8: return try exact(UInt8.self)
        case .uint16: return try exact(UInt16.self)
        case .uint32: return try exact(UInt32.self)
        case .uint64: return UInt64(try strVal())
        // Arrow JSON floats decode to .string, see DataValue
        case .float: return Float(try strVal())
        case .double: return Double(try strVal())
        case .string:
            guard case .string(let str) = value else {
                throw ArrowError.invalid("Expected string, got \(value)")
            }
            return str
        case .binary:
            guard case .string(let hex) = value else { throw ArrowError.invalid("Expected hex string") }
            var data = Data(capacity: hex.count / 2)
            var idx = hex.startIndex
            while idx < hex.endIndex {
                let next = hex.index(idx, offsetBy: 2)
                if let byte = UInt8(hex[idx..<next], radix: 16) { data.append(byte) }
                idx = next
            }
            return data
        case .date32:
            return Date(timeIntervalSince1970: Double(try intVal()) * 86_400)
        case .date64:
            return Date(timeIntervalSince1970: Double(Int64(try strVal()) ?? 0) / 1000)
        case .time32:
            return Int32(try intVal())
        case .time64, .timestamp:
            return Int64(try strVal()) ?? 0
        default:
            throw ArrowError.notImplemented
        }
    }

    func encodeSchema(_ schema: ArrowSchema) throws -> ArrowJSON.Schema {
        .init(fields: try schema.fields.map(encodeField), metadata: nil)
    }

    func encodeField(_ field: ArrowField) throws -> ArrowJSON.Field {
        let (type, children) = try encodeFieldType(field.type)
        return .init(
            name: field.name,
            type: type,
            nullable: field.isNullable,
            children: children ?? []
        )
    }

    /// Returns the JSON `type` object plus, for nested types, the child fields
    /// (structs get one child per member; lists get a single "item" child).
    func encodeFieldType(
        _ type: ArrowType
    ) throws -> (ArrowJSON.FieldType, [ArrowJSON.Field]?) {
        func fieldType(
            name: String,
            byteWidth: Int? = nil,
            bitWidth: Int? = nil,
            isSigned: Bool? = nil,
            precision: String? = nil,
            scale: Int? = nil,
            unit: String? = nil,
            timezone: String? = nil,
            listSize: Int? = nil
        ) -> ArrowJSON.FieldType {
            .init(
                name: name,
                byteWidth: byteWidth,
                bitWidth: bitWidth,
                isSigned: isSigned,
                precision: precision,
                scale: scale,
                unit: unit,
                timezone: timezone,
                listSize: listSize
            )
        }

        switch type.id {
        case .boolean:
            return (fieldType(name: "bool"), nil)

        case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
            let (bitWidth, isSigned) = intWidthAndSign(type.id)
            return (fieldType(name: "int", bitWidth: bitWidth, isSigned: isSigned), nil)

        case .float:
            return (fieldType(name: "floatingpoint", precision: "SINGLE"), nil)
        case .double:
            return (fieldType(name: "floatingpoint", precision: "DOUBLE"), nil)
        case .string:
            return (fieldType(name: "utf8"), nil)
        case .binary:
            return (fieldType(name: "binary"), nil)
        case .date32:
            return (fieldType(name: "date", unit: "DAY"), nil)
        case .date64:
            return (fieldType(name: "date", unit: "MILLISECOND"), nil)
        case .time32:
            guard let time32 = type as? ArrowTypeTime32 else {
                throw ArrowError.invalid("Expected ArrowTypeTime32")
            }
            let unit = time32.unit == .seconds ? "SECOND" : "MILLISECOND"
            return (fieldType(name: "time", bitWidth: 32, unit: unit), nil)

        case .time64:
            guard let time64 = type as? ArrowTypeTime64 else {
                throw ArrowError.invalid("Expected ArrowTypeTime64")
            }
            let unit = time64.unit == .microseconds ? "MICROSECOND" : "NANOSECOND"
            return (fieldType(name: "time", bitWidth: 64, unit: unit), nil)

        case .timestamp:
            guard let timeStamp = type as? ArrowTypeTimestamp else {
                throw ArrowError.invalid("Expected ArrowTypeTimestamp")
            }
            let unit: String
            switch timeStamp.unit {
            case .seconds: unit = "SECOND"
            case .milliseconds: unit = "MILLISECOND"
            case .microseconds: unit = "MICROSECOND"
            case .nanoseconds: unit = "NANOSECOND"
            }
            return (fieldType(name: "timestamp", unit: unit, timezone: timeStamp.timezone), nil)

        case .strct:
            guard let structType = type as? ArrowTypeStruct else {
                throw ArrowError.invalid("Expected ArrowTypeStruct")
            }
            return (fieldType(name: "struct"), try structType.fields.map(encodeField))

        case .list:
            guard let listType = type as? ArrowTypeList else {
                throw ArrowError.invalid("Expected ArrowTypeList")
            }
            return (fieldType(name: "list"), [try encodeField(listType.elementField)])

        default:
            throw ArrowError.notImplemented
        }
    }

    private func intWidthAndSign(_ id: ArrowTypeId) -> (Int, Bool) {
        switch id {
        case .int8: return (8, true)
        case .int16: return (16, true)
        case .int32: return (32, true)
        case .int64: return (64, true)
        case .uint8: return (8, false)
        case .uint16: return (16, false)
        case .uint32: return (32, false)
        case .uint64: return (64, false)
        default: fatalError("Not an integer type")
        }
    }

    func encode(
        batches: [RecordBatch],
        schema: ArrowSchema
    ) throws -> [ArrowJSON.Batch] {
        try batches.map { recordBatch in
            let columns = try schema.fields.enumerated().map { index, field in
                try encodeColumn(holder: recordBatch.column(index), field: field)
            }
            return .init(count: Int(recordBatch.length), columns: columns)
        }
    }

}
