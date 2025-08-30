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

public protocol ArrowBufferBuilder {
    associatedtype ItemType
    var capacity: UInt { get }
    var length: UInt { get }
    var nullCount: UInt { get }
    var offset: UInt { get }
    init() throws
    func append(_ newValue: ItemType?)
    func isNull(_ index: UInt) -> Bool
    func resize(_ length: UInt)
    func finish() -> [ArrowBuffer]
}

public class BaseBufferBuilder {
    var nulls: ArrowBuffer
    public var offset: UInt = 0
    public var capacity: UInt { return nulls.capacity }
    public var length: UInt = 0
    public var nullCount: UInt = 0

    init(_ nulls: ArrowBuffer) {
        self.nulls = nulls
    }

    public func isNull(_ index: UInt) -> Bool {
        return nulls.length == 0 || BitUtility.isSet(index + offset, buffer: nulls)
    }

    func resizeLength(_ data: ArrowBuffer, len: UInt = 0) -> UInt {
        if len == 0 || len < data.length * 2 {
            if data.length == 0 || data.length * 2 < ArrowBuffer.minLength {
                return ArrowBuffer.minLength
            }
            return UInt(data.length * 2)
        }

        return UInt(len * 2)
    }
}

public class ValuesBufferBuilder<T>: BaseBufferBuilder {
    var values: ArrowBuffer
    var stride: Int
    override public var capacity: UInt { return values.capacity }

    init(values: ArrowBuffer, nulls: ArrowBuffer, stride: Int = MemoryLayout<T>.stride) {
        self.stride = stride
        self.values = values
        super.init(nulls)
    }
}

public class FixedBufferBuilder<T>: ValuesBufferBuilder<T>, ArrowBufferBuilder {
    public typealias ItemType = T
    private let defaultVal: ItemType
    public required init() throws {
        defaultVal = try FixedBufferBuilder<T>.defaultValueForType()
        let values = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<T>.stride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        super.init(values: values, nulls: nulls)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(length)
        let byteIndex = self.stride * Int(index)
        length += 1
        if length > values.length {
            resize(length)
        }

        if let val = newValue {
            BitUtility.setBit(index + offset, buffer: nulls)
            values.rawPointer.advanced(by: byteIndex).storeBytes(of: val, as: T.self)
        } else {
            nullCount += 1
            BitUtility.clearBit(index + offset, buffer: nulls)
            values.rawPointer.advanced(by: byteIndex).storeBytes(of: defaultVal, as: T.self)
        }
    }

    public func resize(_ length: UInt) {
        if length > values.length {
            let resizeLength = resizeLength(self.values)
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<T>.size))
            var nulls = ArrowBuffer.createBuffer(resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            self.values = values
            self.nulls = nulls
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<T>.size))
        var nulls = ArrowBuffer.createBuffer(length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        return [nulls, values]
    }

    fileprivate static func defaultValueForType() throws -> T {
        let type = T.self
        if type == Int8.self {
            return Int8(0) as! T // swiftlint:disable:this force_cast
        } else if type == Int16.self {
            return Int16(0) as! T // swiftlint:disable:this force_cast
        } else if type == Int32.self {
            return Int32(0) as! T // swiftlint:disable:this force_cast
        } else if type == Int64.self {
            return Int64(0) as! T // swiftlint:disable:this force_cast
        } else if type == UInt8.self {
            return UInt8(0) as! T // swiftlint:disable:this force_cast
        } else if type == UInt16.self {
            return UInt16(0) as! T // swiftlint:disable:this force_cast
        } else if type == UInt32.self {
            return UInt32(0) as! T // swiftlint:disable:this force_cast
        } else if type == UInt64.self {
            return UInt64(0) as! T // swiftlint:disable:this force_cast
        } else if type == Float.self {
            return Float(0) as! T // swiftlint:disable:this force_cast
        } else if type == Double.self {
            return Double(0) as! T // swiftlint:disable:this force_cast
        }

        throw ArrowError.unknownType("Unable to determine default value")
    }
}

public class BoolBufferBuilder: ValuesBufferBuilder<Bool>, ArrowBufferBuilder {
    public typealias ItemType = Bool
    public required init() throws {
        let values = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        super.init(values: values, nulls: nulls)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(length)
        length += 1
        if (length / 8) > values.length {
            resize(length)
        }

        if newValue != nil {
            BitUtility.setBit(index + offset, buffer: nulls)
            if newValue == true {
                BitUtility.setBit(index + offset, buffer: values)
            } else {
                BitUtility.clearBit(index + offset, buffer: values)
            }

        } else {
            nullCount += 1
            BitUtility.clearBit(index + offset, buffer: nulls)
            BitUtility.clearBit(index + offset, buffer: values)
        }
    }

    public func resize(_ length: UInt) {
        if (length / 8) > values.length {
            let resizeLength = resizeLength(self.values)
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            var nulls = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            self.values = values
            self.nulls = nulls
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<UInt8>.size))
        var nulls = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<UInt8>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        return [nulls, values]
    }
}

public class VariableBufferBuilder<T>: ValuesBufferBuilder<T>, ArrowBufferBuilder {
    public typealias ItemType = T
    var offsets: ArrowBuffer
    let binaryStride = MemoryLayout<UInt8>.stride
    public required init() throws {
        let values = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
        offsets = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<Int32>.stride))
        super.init(values: values, nulls: nulls, stride: binaryStride)
    }

    public func append(_ newValue: ItemType?) {
        let index = UInt(length)
        length += 1
        let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
        if length >= offsets.length {
            resize(UInt(offsets.length + 1))
        }
        var binData: Data
        var isNull = false
        if let val = newValue {
            binData = getBytesFor(val)!
        } else {
            var nullVal = 0
            isNull = true
            binData = Data(bytes: &nullVal, count: MemoryLayout<UInt32>.size)
        }

        var currentIndex: Int32 = 0
        var currentOffset = Int32(binData.count)
        if index > 0 {
            currentIndex = offsets.rawPointer.advanced(by: offsetIndex).load(as: Int32.self)
            currentOffset += currentIndex
            if currentOffset > values.length {
                value_resize(UInt(currentOffset))
            }
        }

        if isNull {
            nullCount += 1
            BitUtility.clearBit(index + offset, buffer: nulls)
        } else {
            BitUtility.setBit(index + offset, buffer: nulls)
        }

        binData.withUnsafeBytes { bufferPointer in
            let rawPointer = bufferPointer.baseAddress!
            self.values.rawPointer.advanced(by: Int(currentIndex))
                .copyMemory(from: rawPointer, byteCount: binData.count)
        }

        offsets.rawPointer.advanced(by: offsetIndex + MemoryLayout<Int32>.stride)
            .storeBytes(of: currentOffset, as: Int32.self)
    }

    public func value_resize(_ length: UInt) {
        if length > values.length {
            let resizeLength = resizeLength(self.values, len: length)
            var values = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.values, to: &values, len: self.values.capacity)
            self.values = values
        }
    }

    public func resize(_ length: UInt) {
        if length > offsets.length {
            let resizeLength = resizeLength(self.offsets, len: length)
            var nulls = ArrowBuffer.createBuffer(resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
            var offsets = ArrowBuffer.createBuffer(resizeLength, size: UInt(MemoryLayout<Int32>.size))
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: self.offsets.capacity)
            self.nulls = nulls
            self.offsets = offsets
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var values = ArrowBuffer.createBuffer(self.values.length, size: UInt(MemoryLayout<UInt8>.size))
        var nulls = ArrowBuffer.createBuffer(length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
        var offsets = ArrowBuffer.createBuffer(length, size: UInt(MemoryLayout<Int32>.size))
        ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: offsets.capacity)
        return [nulls, offsets, values]
    }
}

public class AbstractWrapperBufferBuilder<T, U>: ArrowBufferBuilder {
    public typealias ItemType = T
    public var capacity: UInt { return bufferBuilder.capacity }
    public var length: UInt { return bufferBuilder.length }
    public var nullCount: UInt { return bufferBuilder.nullCount }
    public var offset: UInt { return bufferBuilder.offset }
    let bufferBuilder: FixedBufferBuilder<U>
    public required init() throws {
        bufferBuilder = try FixedBufferBuilder()
    }

    public func append(_: ItemType?) {
        fatalError("Method is not implemented")
    }

    public func isNull(_ index: UInt) -> Bool {
        return bufferBuilder.isNull(index)
    }

    public func resize(_ length: UInt) {
        bufferBuilder.resize(length)
    }

    public func finish() -> [ArrowBuffer] {
        return bufferBuilder.finish()
    }
}

public class Date32BufferBuilder: AbstractWrapperBufferBuilder<Date, Int32> {
    override public func append(_ newValue: ItemType?) {
        if let val = newValue {
            let daysSinceEpoch = Int32(val.timeIntervalSince1970 / 86400)
            bufferBuilder.append(daysSinceEpoch)
        } else {
            bufferBuilder.append(nil)
        }
    }
}

public class Date64BufferBuilder: AbstractWrapperBufferBuilder<Date, Int64> {
    override public func append(_ newValue: ItemType?) {
        if let val = newValue {
            let daysSinceEpoch = Int64(val.timeIntervalSince1970 * 1000)
            bufferBuilder.append(daysSinceEpoch)
        } else {
            bufferBuilder.append(nil)
        }
    }
}

public final class StructBufferBuilder: BaseBufferBuilder, ArrowBufferBuilder {
    public typealias ItemType = [Any?]
    var info: ArrowNestedType?
    public init() throws {
        let nulls = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<UInt8>.stride))
        super.init(nulls)
    }

    public func initializeTypeInfo(_ fields: [ArrowField]) {
        info = ArrowNestedType(ArrowType.ArrowStruct, fields: fields)
    }

    public func append(_ newValue: [Any?]?) {
        let index = UInt(length)
        length += 1
        if length > nulls.length {
            resize(length)
        }

        if newValue != nil {
            BitUtility.setBit(index + offset, buffer: nulls)
        } else {
            nullCount += 1
            BitUtility.clearBit(index + offset, buffer: nulls)
        }
    }

    public func resize(_ length: UInt) {
        if length > nulls.length {
            let resizeLength = resizeLength(self.nulls)
            var nulls = ArrowBuffer.createBuffer(resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
            ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
            self.nulls = nulls
        }
    }

    public func finish() -> [ArrowBuffer] {
        let length = self.length
        var nulls = ArrowBuffer.createBuffer(length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
        ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
        return [nulls]
    }
}
