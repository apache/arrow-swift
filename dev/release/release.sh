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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 21.0.0 0"
  exit 1
fi

version="$1"
rc="$2"

: "${RELEASE_DEFAULT:=1}"
: "${RELEASE_TAG:=${RELEASE_DEFAULT}}"
: "${RELEASE_UPLOAD:=${RELEASE_DEFAULT}}"
: "${RELEASE_CLEAN:=${RELEASE_DEFAULT}}"

if [ ! -f "${SOURCE_DIR}/.env" ]; then
  echo "You must create ${SOURCE_DIR}/.env"
  echo "You can use ${SOURCE_DIR}/.env.example as template"
  exit 1
fi
. "${SOURCE_DIR}/.env"

cd "${SOURCE_TOP_DIR}"

git_origin_url="$(git remote get-url origin)"
repository="${git_origin_url#*github.com?}"
repository="${repository%.git}"
case "${git_origin_url}" in
git@github.com:apache/arrow-swift.git | https://github.com/apache/arrow-swift.git)
  : # OK
  ;;
*)
  echo "This script must be ran with working copy of apache/arrow-swift."
  echo "The origin's URL: ${git_origin_url}"
  exit 1
  ;;
esac

tag="v${version}"
rc_tag="${tag}-rc${rc}"
if [ "${RELEASE_TAG}" -gt 0 ]; then
  echo "Tagging for release: ${tag}"
  git tag -a -m "${version}" "${tag}" "${rc_tag}^{}"
  git push origin "${tag}"
fi

release_id="apache-arrow-swift-${version}"
source_archive="apache-arrow-swift-${version}.tar.gz"
dist_url="https://dist.apache.org/repos/dist/release/arrow"
dist_base_dir="dev/release/dist"
dist_dir="${dist_base_dir}/${release_id}"
if [ "${RELEASE_UPLOAD}" -gt 0 ]; then
  echo "Checking out ${dist_url}"
  rm -rf "${dist_base_dir}"
  svn co --depth=empty "${dist_url}" "${dist_base_dir}"
  gh release download "${rc_tag}" \
    --dir "${dist_dir}" \
    --pattern "${source_archive}*" \
    --repo "${repository}" \
    --skip-existing

  echo "Uploading to release/"
  pushd "${dist_base_dir}"
  svn add "${release_id}"
  svn ci -m "Apache Arrow Swift ${version}"
  popd
  rm -rf "${dist_base_dir}"
fi

if [ "${RELEASE_CLEAN}" -gt 0 ]; then
  echo "Keep only the latest versions"
  old_releases=$(
    svn ls https://dist.apache.org/repos/dist/release/arrow/ |
      grep -E '^apache-arrow-swift-' |
      sort --version-sort --reverse |
      tail -n +2
  )
  for old_release_version in ${old_releases}; do
    echo "Remove old release ${old_release_version}"
    svn \
      delete \
      -m "Remove old Apache Arrow Swift release: ${old_release_version}" \
      "https://dist.apache.org/repos/dist/release/arrow/${old_release_version}"
  done
fi

echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/arrow/${release_id}"
echo "  https://swiftpackageindex.com/apache/arrow"
echo
echo "Add this release to ASF's report database:"
echo "  https://reporter.apache.org/addrelease.html?arrow"
