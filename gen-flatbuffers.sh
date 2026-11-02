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

set -eu

# The .fbs format files are maintained in the apache/arrow repository.
# These sources are generated from format/*.fbs at this revision. Recording
# it keeps regeneration reproducible: bump it deliberately, regenerate, and
# commit the result, rather than picking up whatever is in the working copy.
ARROW_REVISION=7151dcd6faa6a507d99b33ecc797f3d6a74f45ae

# Either check out apache/arrow at ${ARROW_REVISION} next to this repository,
# or set ARROW_FORMAT_DIR to point at its format/ directory.
FORMAT_DIR="${ARROW_FORMAT_DIR:-../arrow/format}"
if [ ! -d "${FORMAT_DIR}" ]; then
  echo "error: ${FORMAT_DIR} not found" >&2
  echo "error: check out apache/arrow at ${ARROW_REVISION} and set ARROW_FORMAT_DIR" >&2
  exit 1
fi
# Resolve before changing directory below, because it may be relative.
FORMAT_DIR=$(cd "${FORMAT_DIR}" && pwd)

# The generated sources must be produced by the flatc that matches the
# FlatBuffers runtime in Package.swift. A mismatch produces sources that do
# not compile against the runtime.
EXPECTED_FLATC_VERSION=$(
  sed -n 's|.*google/flatbuffers\.git", exact: "\([^"]*\)".*|\1|p' Package.swift
)
ACTUAL_FLATC_VERSION=$(flatc --version | sed 's/^flatc version //')
if [ -n "${EXPECTED_FLATC_VERSION}" ] &&
   [ "${ACTUAL_FLATC_VERSION}" != "${EXPECTED_FLATC_VERSION}" ]; then
  echo "error: flatc ${ACTUAL_FLATC_VERSION} does not match the FlatBuffers" >&2
  echo "error: runtime ${EXPECTED_FLATC_VERSION} in Package.swift" >&2
  exit 1
fi

echo "Generating with flatc ${ACTUAL_FLATC_VERSION}" \
     "from apache/arrow ${ARROW_REVISION}"

pushd Sources/Arrow/
flatc --swift "${FORMAT_DIR}/Message.fbs"
flatc --swift "${FORMAT_DIR}/Schema.fbs"
flatc --swift "${FORMAT_DIR}/SparseTensor.fbs"
flatc --swift "${FORMAT_DIR}/Tensor.fbs"
flatc --swift "${FORMAT_DIR}/File.fbs"
cat <<HEADER > header.swift
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
HEADER
for generated_swift in *_generated.swift; do
  mv ${generated_swift} ${generated_swift}.orig
  cat header.swift ${generated_swift}.orig > ${generated_swift}
  rm ${generated_swift}.orig
done
rm header.swift
popd