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

import struct Foundation.Data
import GRPCCore
import Arrow

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
public class FlightClient<Transport: ClientTransport> {
    let client: Arrow_Flight_Protocol_FlightService.Client<Transport>

    public init(wrapping client: GRPCCore.GRPCClient<Transport>) {
        self.client = Arrow_Flight_Protocol_FlightService.Client(wrapping: client)
    }

    public init(transport: Transport) {
        self.client = Arrow_Flight_Protocol_FlightService.Client(wrapping: GRPCClient(transport: transport))
    }

    private func readMessages(
        _ response: StreamingClientResponse<Arrow_Flight_Protocol_FlightData>
    ) async throws -> ArrowReader.ArrowReaderResult {
        let reader = ArrowReader()
        let arrowResult = ArrowReader.makeArrowReaderResult()
        for try await data in response.messages {
            switch reader.fromMessage(
                data.dataHeader,
                dataBody: data.dataBody,
                result: arrowResult) {
            case .success:
                continue
            case .failure(let error):
                throw error
            }
        }

        return arrowResult
    }

    private func writeBatches(
        to writer: RPCWriter<Arrow_Flight_Protocol_FlightData>,
        descriptor: FlightDescriptor,
        recordBatches: [RecordBatch]
    ) async throws {
        let arrowWriter = ArrowWriter()
        switch arrowWriter.toMessage(recordBatches[0].schema) {
        case .success(let schemaData):
            try await writer.write(
                FlightData(
                    schemaData,
                    dataBody: Data(),
                    flightDescriptor: descriptor).toProtocol())
            for recordBatch in recordBatches {
                switch arrowWriter.toMessage(recordBatch) {
                case .success(let data):
                    try await writer.write(
                        FlightData(
                            data[0],
                            dataBody: data[1],
                            flightDescriptor: descriptor).toProtocol())
                case .failure(let error):
                    throw error
                }
            }
        case .failure(let error):
            throw error
        }
    }

    public func listActions(_ closure: @Sendable @escaping (FlightActionType) -> Void) async throws {
        try await client.listActions(Arrow_Flight_Protocol_Empty()) { response in
            for try await data in response.messages {
                closure(FlightActionType(data))
            }
        }
    }

    public func listFlights(
        _ criteria: FlightCriteria,
        closure: @Sendable @escaping (FlightInfo) throws -> Void) async throws {
        try await client.listFlights(criteria.toProtocol()) { response in
            for try await data in response.messages {
                try closure(FlightInfo(data))
            }
        }
    }

    public func doAction(
        _ action: FlightAction,
        closure: @Sendable @escaping (FlightResult) throws -> Void) async throws {
        try await client.doAction(action.toProtocol()) { response in
            for try await data in response.messages {
                try closure(FlightResult(data))
            }
        }
    }

    public func getSchema(_ descriptor: FlightDescriptor) async throws -> FlightSchemaResult {
        let schemaResult = try await client.getSchema(descriptor.toProtocol())
        return FlightSchemaResult(schemaResult)
    }

    public func doGet(
        _ ticket: FlightTicket,
        readerResultClosure: @Sendable @escaping (ArrowReader.ArrowReaderResult) throws -> Void) async throws {
        try await client.doGet(ticket.toProtocol()) { response in
            try readerResultClosure(try await self.readMessages(response))
        }
    }

    public func doGet(
        _ ticket: FlightTicket,
        flightDataClosure: @Sendable @escaping (FlightData) throws -> Void) async throws {
        try await client.doGet(ticket.toProtocol()) { response in
            for try await data in response.messages {
                try flightDataClosure(FlightData(data))
            }
        }
    }

    public func doPut(
        _ descriptor: FlightDescriptor,
        recordBatches: [RecordBatch],
        closure: @Sendable @escaping (FlightPutResult) throws -> Void) async throws {
        if recordBatches.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        var closureCalled = false
        try await client.doPut { requestWriter in
            try await self.writeBatches(
                to: requestWriter,
                descriptor: descriptor,
                recordBatches: recordBatches)
        } onResponse: { response in
            for try await putResult in response.messages {
                try closure(FlightPutResult(putResult))
                closureCalled = true
            }
        }

        if !closureCalled {
            try closure(FlightPutResult())
        }
    }

    public func doPut(
        _ flightData: FlightData,
        closure: @Sendable @escaping (FlightPutResult) throws -> Void) async throws {
        var closureCalled = false
        try await client.doPut { requestWriter in
            try await requestWriter.write(flightData.toProtocol())
        } onResponse: { response in
            for try await putResult in response.messages {
                try closure(FlightPutResult(putResult))
                closureCalled = true
            }
        }

        if !closureCalled {
            try closure(FlightPutResult())
        }
    }

    public func doExchange(
        _ descriptor: FlightDescriptor,
        recordBatches: [RecordBatch],
        closure: @Sendable @escaping (ArrowReader.ArrowReaderResult) throws -> Void) async throws {
        if recordBatches.isEmpty {
            throw ArrowFlightError.emptyCollection
        }

        try await client.doExchange { requestWriter in
            try await self.writeBatches(
                to: requestWriter,
                descriptor: descriptor,
                recordBatches: recordBatches)
        } onResponse: { response in
            try closure(try await self.readMessages(response))
        }
    }

    public func doExchange(
        flightData: FlightData,
        closure: @Sendable @escaping (FlightData) throws -> Void) async throws {
        try await client.doExchange { requestWriter in
            try await requestWriter.write(flightData.toProtocol())
        } onResponse: { response in
            for try await responseMessage in response.messages {
                try closure(FlightData(responseMessage))
            }
        }
    }
}
