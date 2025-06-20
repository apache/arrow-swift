#!/usr/bin/env bash
#
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

github_actions_group_begin() {
  echo "::group::$1"
  set -x
}

github_actions_group_end() {
  set +x
  echo "::endgroup::"
}

github_actions_group_begin "Prepare"
source_dir="${1}"
build_dir="${2}"

rm -rf "${build_dir}"
mkdir -p "${build_dir}"
cp -a "${source_dir}" "${build_dir}/source"
rm -rf "${build_dir}/source/.build"
if [ -d /cache ]; then
  mkdir -p /cache/swift-build
  ln -s /cache/swift-build "${build_dir}/source/.build"
fi
github_actions_group_end

github_actions_group_begin "Generate data"
data_gen_dir="${build_dir}/source/data-generator/swift-datagen"
if [ -d /cache ]; then
  export GOCACHE="/cache/go-build"
  export GOMODCACHE="/cache/go-mod"
fi
export GOPATH="${build_dir}"
pushd "${data_gen_dir}"
go get -d ./...
go run .
cp *.arrow ../../Arrow
popd
github_actions_group_end

github_actions_group_begin "Use -warnings-as-errors"
for package in . Arrow ArrowFlight; do
  pushd "${build_dir}/source/${package}"
  sed 's/\/\/ build://g' Package.swift > Package.swift.build
  mv Package.swift.build Package.swift
  popd
done
github_actions_group_end

github_actions_group_begin "Build"
pushd "${build_dir}/source"
swift build
popd
github_actions_group_end
