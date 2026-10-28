#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Regenerates Sources/ArrowFlight/Flight.pb.swift and
# Sources/ArrowFlight/Flight.grpc.swift from format/Flight.proto.
#
# The .proto files are maintained in the apache/arrow repository.
# Either check out apache/arrow next to this repository, or set
# ARROW_FORMAT_DIR to point at its format/ directory.
#
# Requires protoc, protoc-gen-swift and protoc-gen-grpc-swift-2 on PATH.

set -eu

FORMAT_DIR="${ARROW_FORMAT_DIR:-../arrow/format}"

pushd Sources/ArrowFlight/
protoc -I "${FORMAT_DIR}" --swift_out=. "${FORMAT_DIR}/Flight.proto"
protoc -I "${FORMAT_DIR}" --grpc-swift-2_out=. "${FORMAT_DIR}/Flight.proto"
popd
