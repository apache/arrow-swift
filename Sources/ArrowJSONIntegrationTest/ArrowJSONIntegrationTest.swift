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

import ArgumentParser
import Arrow
import Foundation

enum IntegrationMode: String, ExpressibleByArgument, CaseIterable {
    case arrowToJson = "ARROW_TO_JSON"
    case jsonToArrow = "JSON_TO_ARROW"
    case validate = "VALIDATE"
}

@main
struct ArrowJSONIntegrationTest: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "arrow-json-integration-test",
        abstract: "Converts between and validates Arrow and JSON integration test files."
    )

    @Option(name: .long, help: "Path to the Arrow file.")
    var arrow: String

    @Option(name: .long, help: "Path to the JSON file.")
    var json: String

    @Option(name: .long, help: "Mode: ARROW_TO_JSON, JSON_TO_ARROW, or VALIDATE.")
    var mode: IntegrationMode

    func validate() throws {
        // Runs automatically before `run()` — good place for input checks.
        if mode != .jsonToArrow {
            guard FileManager.default.fileExists(atPath: arrow) else {
                throw ValidationError("Arrow file not found at path: \(arrow)")
            }
        }
        if mode != .arrowToJson {
            guard FileManager.default.fileExists(atPath: json) else {
                throw ValidationError("JSON file not found at path: \(json)")
            }
        }
    }

    func run() throws {
        let arrowURL = URL(fileURLWithPath: arrow)
        let jsonURL = URL(fileURLWithPath: json)

        switch mode {
        case .arrowToJson:
            try convertArrowToJSON(arrowURL: arrowURL, jsonURL: jsonURL)
        case .jsonToArrow:
            try convertJSONToArrow(jsonURL: jsonURL, arrowURL: arrowURL)
        case .validate:
            try validateFiles(arrowURL: arrowURL, jsonURL: jsonURL)
        }
    }

    private func convertJSONToArrow(jsonURL: URL, arrowURL: URL) throws {
        let arrowJSON = try JSONDecoder().decode(ArrowJSON.self, from: try Data(contentsOf: jsonURL))
        let jsonFields = arrowJSON.schema.fields  // [ArrowJSON.Field]
        let arrowFields = try jsonFields.map(decodeField)  // [ArrowField]

        var batches: [RecordBatch] = []
        for batch in arrowJSON.batches {
            let builder = RecordBatch.Builder()
            for (jsonField, arrowField) in zip(jsonFields, arrowFields) {
                guard let column = batch.columns.first(where: { $0.name == jsonField.name }) else {
                    throw ArrowError.invalid("Missing column \(jsonField.name)")
                }
                _ = builder.addColumn(
                    arrowField.name,
                    arrowArray: try buildHolder(column: column, type: arrowField.type))
            }
            switch builder.finish() {
            case .success(let recordBatch): batches.append(recordBatch)
            case .failure(let error): throw error
            }
        }

        let schemaBuilder = ArrowSchema.Builder()
        for arrowField in arrowFields {
            schemaBuilder.addField(arrowField)
        }
        let schema = schemaBuilder.finish()

        switch ArrowWriter().writeFile(ArrowWriter.Info(.recordbatch, schema: schema, batches: batches)) {
        case .success(let data): try data.write(to: arrowURL)
        case .failure(let error): throw error
        }
    }

    /// Converts an Arrow JSON field into the real arrow-swift `ArrowField`.
    func decodeField(_ field: ArrowJSON.Field) throws -> ArrowField {
        ArrowField(field.name, type: try decodeArrowType(field), isNullable: field.nullable)
    }

    private func validateFiles(arrowURL: URL, jsonURL: URL) throws {
        let expected = try JSONDecoder().decode(
            ArrowJSON.self, from: try Data(contentsOf: jsonURL))

        let actual = try decodeArrowJSON(from: arrowURL)

        guard expected.normalized() == actual.normalized() else {
            throw ValidationError("Arrow file does not match JSON file")
        }
    }

    private func convertArrowToJSON(arrowURL: URL, jsonURL: URL) throws {
        let gold = try decodeArrowJSON(from: arrowURL)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted]
        try encoder.encode(gold).write(to: jsonURL)
    }
}
