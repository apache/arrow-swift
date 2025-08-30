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

public class ArrowDecoder: Decoder {
    var rbIndex: UInt = 0
    var singleRBCol: Int = 0
    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]
    public let rb: RecordBatch
    public let nameToCol: [String: ArrowArrayHolder]
    public let columns: [ArrowArrayHolder]
    public init(_ decoder: ArrowDecoder) {
        userInfo = decoder.userInfo
        codingPath = decoder.codingPath
        rb = decoder.rb
        columns = decoder.columns
        nameToCol = decoder.nameToCol
        rbIndex = decoder.rbIndex
    }

    public init(_ rb: RecordBatch) {
        self.rb = rb
        var colMapping = [String: ArrowArrayHolder]()
        var columns = [ArrowArrayHolder]()
        for index in 0 ..< self.rb.schema.fields.count {
            let field = self.rb.schema.fields[index]
            columns.append(self.rb.column(index))
            colMapping[field.name] = self.rb.column(index)
        }

        self.columns = columns
        nameToCol = colMapping
    }

    public func decode<T: Decodable, U: Decodable>(_: [T: U].Type) throws -> [T: U] {
        var output = [T: U]()
        if rb.columnCount != 2 {
            throw ArrowError.invalid("RecordBatch column count of 2 is required to decode to map")
        }

        for index in 0 ..< rb.length {
            rbIndex = index
            singleRBCol = 0
            let key = try T(from: self)
            singleRBCol = 1
            let value = try U(from: self)
            output[key] = value
        }

        singleRBCol = 0
        return output
    }

    public func decode<T: Decodable>(_ type: T.Type) throws -> [T] {
        var output = [T]()
        for index in 0 ..< rb.length {
            rbIndex = index
            try output.append(type.init(from: self))
        }

        return output
    }

    public func container<Key>(keyedBy _: Key.Type
    ) -> KeyedDecodingContainer<Key> where Key: CodingKey {
        let container = ArrowKeyedDecoding<Key>(self, codingPath: codingPath)
        return KeyedDecodingContainer(container)
    }

    public func unkeyedContainer() -> UnkeyedDecodingContainer {
        return ArrowUnkeyedDecoding(self, codingPath: codingPath)
    }

    public func singleValueContainer() -> SingleValueDecodingContainer {
        return ArrowSingleValueDecoding(self, codingPath: codingPath)
    }

    func getCol(_ name: String) throws -> AnyArray {
        guard let col = nameToCol[name] else {
            throw ArrowError.invalid("Column for key \"\(name)\" not found")
        }

        return col.array
    }

    func getCol(_ index: Int) throws -> AnyArray {
        if index >= columns.count {
            throw ArrowError.outOfBounds(index: Int64(index))
        }

        return columns[index].array
    }

    func doDecode<T>(_ key: CodingKey) throws -> T? {
        let array: AnyArray = try getCol(key.stringValue)
        return array.asAny(rbIndex) as? T
    }

    func doDecode<T>(_ col: Int) throws -> T? {
        let array: AnyArray = try getCol(col)
        return array.asAny(rbIndex) as? T
    }

    func isNull(_ key: CodingKey) throws -> Bool {
        let array: AnyArray = try getCol(key.stringValue)
        return array.asAny(rbIndex) == nil
    }

    func isNull(_ col: Int) throws -> Bool {
        let array: AnyArray = try getCol(col)
        return array.asAny(rbIndex) == nil
    }
}

private struct ArrowUnkeyedDecoding: UnkeyedDecodingContainer {
    var codingPath: [CodingKey]
    var count: Int? = 0
    var isAtEnd: Bool = false
    var currentIndex: Int = 0
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
        count = self.decoder.columns.count
    }

    mutating func increment() {
        currentIndex += 1
        isAtEnd = currentIndex >= count!
    }

    mutating func decodeNil() throws -> Bool {
        defer { increment() }
        return try decoder.isNull(currentIndex)
    }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if type == Int8?.self || type == Int16?.self ||
            type == Int32?.self || type == Int64?.self ||
            type == UInt8?.self || type == UInt16?.self ||
            type == UInt32?.self || type == UInt64?.self ||
            type == String?.self || type == Double?.self ||
            type == Float?.self || type == Date?.self ||
            type == Bool?.self || type == Bool.self ||
            type == Int8.self || type == Int16.self ||
            type == Int32.self || type == Int64.self ||
            type == UInt8.self || type == UInt16.self ||
            type == UInt32.self || type == UInt64.self ||
            type == String.self || type == Double.self ||
            type == Float.self || type == Date.self {
            defer { increment() }
            return try decoder.doDecode(currentIndex)!
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }

    func nestedContainer<NestedKey>(
        keyedBy _: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }
}

private struct ArrowKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath = [CodingKey]()
    var allKeys = [Key]()
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
    }

    func contains(_ key: Key) -> Bool {
        return decoder.nameToCol.keys.contains(key.stringValue)
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        try decoder.isNull(key)
    }

    func decode(_: Bool.Type, forKey key: Key) throws -> Bool {
        return try decoder.doDecode(key)!
    }

    func decode(_: String.Type, forKey key: Key) throws -> String {
        return try decoder.doDecode(key)!
    }

    func decode(_: Double.Type, forKey key: Key) throws -> Double {
        return try decoder.doDecode(key)!
    }

    func decode(_: Float.Type, forKey key: Key) throws -> Float {
        return try decoder.doDecode(key)!
    }

    func decode(_: Int.Type, forKey _: Key) throws -> Int {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    func decode(_: Int8.Type, forKey key: Key) throws -> Int8 {
        return try decoder.doDecode(key)!
    }

    func decode(_: Int16.Type, forKey key: Key) throws -> Int16 {
        return try decoder.doDecode(key)!
    }

    func decode(_: Int32.Type, forKey key: Key) throws -> Int32 {
        return try decoder.doDecode(key)!
    }

    func decode(_: Int64.Type, forKey key: Key) throws -> Int64 {
        return try decoder.doDecode(key)!
    }

    func decode(_: UInt.Type, forKey _: Key) throws -> UInt {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    func decode(_: UInt8.Type, forKey key: Key) throws -> UInt8 {
        return try decoder.doDecode(key)!
    }

    func decode(_: UInt16.Type, forKey key: Key) throws -> UInt16 {
        return try decoder.doDecode(key)!
    }

    func decode(_: UInt32.Type, forKey key: Key) throws -> UInt32 {
        return try decoder.doDecode(key)!
    }

    func decode(_: UInt64.Type, forKey key: Key) throws -> UInt64 {
        return try decoder.doDecode(key)!
    }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
            return try decoder.doDecode(key)!
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }

    func nestedContainer<NestedKey>(
        keyedBy _: NestedKey.Type,
        forKey _: Key
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func nestedUnkeyedContainer(forKey _: Key) throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }

    func superDecoder(forKey _: Key) throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }
}

private struct ArrowSingleValueDecoding: SingleValueDecodingContainer {
    var codingPath = [CodingKey]()
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
    }

    func decodeNil() -> Bool {
        do {
            return try decoder.isNull(decoder.singleRBCol)
        } catch {
            return false
        }
    }

    func decode(_: Bool.Type) throws -> Bool {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: String.Type) throws -> String {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Double.Type) throws -> Double {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Float.Type) throws -> Float {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Int.Type) throws -> Int {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    func decode(_: Int8.Type) throws -> Int8 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Int16.Type) throws -> Int16 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Int32.Type) throws -> Int32 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: Int64.Type) throws -> Int64 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: UInt.Type) throws -> UInt {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    func decode(_: UInt8.Type) throws -> UInt8 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: UInt16.Type) throws -> UInt16 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: UInt32.Type) throws -> UInt32 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode(_: UInt64.Type) throws -> UInt64 {
        return try decoder.doDecode(decoder.singleRBCol)!
    }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
            return try decoder.doDecode(decoder.singleRBCol)!
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }
}
