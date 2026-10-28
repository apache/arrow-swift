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
import GRPCCore

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
public class RecordBatchStreamReader: AsyncSequence, AsyncIteratorProtocol {
    public typealias AsyncIterator = RecordBatchStreamReader
    public typealias Element = (Arrow.RecordBatch?, FlightDescriptor?)
    let reader = ArrowReader()
    var batches = [RecordBatch]()
    var descriptor: FlightDescriptor?
    var batchIndex = 0
    var streamIterator: RPCAsyncSequence<Arrow_Flight_Protocol_FlightData, any Swift.Error>.AsyncIterator
    let stream: RPCAsyncSequence<Arrow_Flight_Protocol_FlightData, any Swift.Error>
    init(_ stream: RPCAsyncSequence<Arrow_Flight_Protocol_FlightData, any Swift.Error>) {
        self.stream = stream
        self.streamIterator = self.stream.makeAsyncIterator()
    }

    public func next() async throws -> (Arrow.RecordBatch?, FlightDescriptor?)? {
        guard !Task.isCancelled else {
            return nil
        }

        if batchIndex < batches.count {
            let batch = batches[batchIndex]
            batchIndex += 1
            return (batch, descriptor)
        }

        let result = ArrowReader.makeArrowReaderResult()
        while true {
            guard let flightData = try await self.streamIterator.next() else {
                return nil
            }

            let dataBody = flightData.dataBody
            let dataHeader = flightData.dataHeader
            descriptor = FlightDescriptor(flightData.flightDescriptor)
            switch reader.fromMessage(
                dataHeader,
                dataBody: dataBody,
                result: result) {
            case .success(()):
                if result.batches.count > 0 {
                    batches = result.batches
                    batchIndex = 1
                    return (batches[0], descriptor)
                }
            case .failure(let error):
                throw error
            }
        }
    }

    public func makeAsyncIterator() -> RecordBatchStreamReader {
        self
    }
}
