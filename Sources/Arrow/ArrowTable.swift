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

public class ArrowColumn {
    public let field: ArrowField
    fileprivate let dataHolder: ChunkedArrayHolder
    public var type: ArrowType { return dataHolder.type }
    public var length: UInt { return dataHolder.length }
    public var nullCount: UInt { return dataHolder.nullCount }

    public func data<T>() -> ChunkedArray<T> {
        return (dataHolder.holder as! ChunkedArray<T>) // swiftlint:disable:this force_cast
    }

    public var name: String { return field.name }
    public init(_ field: ArrowField, chunked: ChunkedArrayHolder) {
        self.field = field
        dataHolder = chunked
    }
}

public class ArrowTable {
    public let schema: ArrowSchema
    public var columnCount: UInt { return UInt(columns.count) }
    public let rowCount: UInt
    public let columns: [ArrowColumn]
    init(_ schema: ArrowSchema, columns: [ArrowColumn]) {
        self.schema = schema
        self.columns = columns
        rowCount = columns[0].length
    }

    public static func from(recordBatches: [RecordBatch]) -> Result<ArrowTable, ArrowError> {
        if recordBatches.isEmpty {
            return .failure(.arrayHasNoElements)
        }

        var holders = [[ArrowArrayHolder]]()
        let schema = recordBatches[0].schema
        for recordBatch in recordBatches {
            for index in 0 ..< schema.fields.count {
                if holders.count <= index {
                    holders.append([ArrowArrayHolder]())
                }
                holders[index].append(recordBatch.columns[index])
            }
        }

        let builder = ArrowTable.Builder()
        for index in 0 ..< schema.fields.count {
            switch makeArrowColumn(schema.fields[index], holders: holders[index]) {
            case let .success(column):
                builder.addColumn(column)
            case let .failure(error):
                return .failure(error)
            }
        }

        return .success(builder.finish())
    }

    private static func makeArrowColumn(
        _ field: ArrowField,
        holders: [ArrowArrayHolder]
    ) -> Result<ArrowColumn, ArrowError> {
        do {
            return try .success(holders[0].getArrowColumn(field, holders))
        } catch {
            return .failure(.runtimeError("\(error)"))
        }
    }

    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowColumn]()

        public init() {}

        @discardableResult
        public func addColumn<T>(_ fieldName: String, arrowArray: ArrowArray<T>) throws -> Builder {
            return try addColumn(fieldName, chunked: ChunkedArray([arrowArray]))
        }

        @discardableResult
        public func addColumn<T>(_ fieldName: String, chunked: ChunkedArray<T>) -> Builder {
            let field = ArrowField(fieldName, type: chunked.type, isNullable: chunked.nullCount != 0)
            schemaBuilder.addField(field)
            columns.append(ArrowColumn(field, chunked: ChunkedArrayHolder(chunked)))
            return self
        }

        @discardableResult
        public func addColumn<T>(_ field: ArrowField, arrowArray: ArrowArray<T>) throws -> Builder {
            schemaBuilder.addField(field)
            let holder = try ChunkedArrayHolder(ChunkedArray([arrowArray]))
            columns.append(ArrowColumn(field, chunked: holder))
            return self
        }

        @discardableResult
        public func addColumn<T>(_ field: ArrowField, chunked: ChunkedArray<T>) -> Builder {
            schemaBuilder.addField(field)
            columns.append(ArrowColumn(field, chunked: ChunkedArrayHolder(chunked)))
            return self
        }

        @discardableResult
        public func addColumn(_ column: ArrowColumn) -> Builder {
            schemaBuilder.addField(column.field)
            columns.append(column)
            return self
        }

        public func finish() -> ArrowTable {
            return ArrowTable(schemaBuilder.finish(), columns: columns)
        }
    }
}

public class RecordBatch {
    public let schema: ArrowSchema
    public var columnCount: UInt { return UInt(columns.count) }
    public let columns: [ArrowArrayHolder]
    public let length: UInt
    public init(_ schema: ArrowSchema, columns: [ArrowArrayHolder]) {
        self.schema = schema
        self.columns = columns
        length = columns[0].length
    }

    public class Builder {
        let schemaBuilder = ArrowSchema.Builder()
        var columns = [ArrowArrayHolder]()

        public init() {}

        @discardableResult
        public func addColumn(_ fieldName: String, arrowArray: ArrowArrayHolder) -> Builder {
            let field = ArrowField(fieldName, type: arrowArray.type, isNullable: arrowArray.nullCount != 0)
            schemaBuilder.addField(field)
            columns.append(arrowArray)
            return self
        }

        @discardableResult
        public func addColumn(_ field: ArrowField, arrowArray: ArrowArrayHolder) -> Builder {
            schemaBuilder.addField(field)
            columns.append(arrowArray)
            return self
        }

        public func finish() -> Result<RecordBatch, ArrowError> {
            if columns.count > 0 {
                let columnLength = columns[0].length
                for column in columns {
                    if column.length != columnLength { // swiftlint:disable:this for_where
                        return .failure(.runtimeError("Columns have different sizes"))
                    }
                }
            }
            return .success(RecordBatch(schemaBuilder.finish(), columns: columns))
        }
    }

    public func data<T>(for columnIndex: Int) -> ArrowArray<T> {
        let arrayHolder = column(columnIndex)
        return (arrayHolder.array as! ArrowArray<T>) // swiftlint:disable:this force_cast
    }

    public func anyData(for columnIndex: Int) -> AnyArray {
        let arrayHolder = column(columnIndex)
        return arrayHolder.array
    }

    public func column(_ index: Int) -> ArrowArrayHolder {
        return columns[index]
    }

    public func column(_ name: String) -> ArrowArrayHolder? {
        if let index = schema.fieldIndex(name) {
            return columns[index]
        }

        return nil
    }
}
