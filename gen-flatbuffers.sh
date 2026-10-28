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
# Either check out apache/arrow next to this repository, or set
# ARROW_FORMAT_DIR to point at its format/ directory.
FORMAT_DIR="${ARROW_FORMAT_DIR:-../arrow/format}"

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