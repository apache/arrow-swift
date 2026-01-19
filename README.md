<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow Swift

[Apache Arrow](https://arrow.apache.org/) is a universal columnar format and multi-language toolbox for fast data interchange and in-memory analytics. It contains a set of technologies that enable data systems to efficiently store, process, and move data.

## Installation

### Swift Package Manager

Add Arrow as a dependency in your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/apache/arrow-swift.git", from: "21.0.0")
]
```

Then add `Arrow` to your target's dependencies:

```swift
.target(
    name: "YourTarget",
    dependencies: ["Arrow"]
)
```

### Xcode

1. Go to **File > Add Package Dependencies...**
2. Enter the repository URL: `https://github.com/apache/arrow-swift.git`
3. Select your desired version rules and click **Add Package**

## Getting Started

TODO: We should refer auto generated documentation on Swift Package Index after we register this package to Swift Package Index.

## Getting involved

Even if you do not plan to contribute to Apache Arrow itself or Apache Arrow integrations in other projects, we'd be happy to have you involved:

* [Join the mailing list](https://arrow.apache.org/community/#mailing-lists): Share your ideas and use cases for the project
* Follow our activity on [GitHub Issues](https://github.com/apache/arrow-swift/issues)
* Contribute code

We prefer to receive contributions in the form of GitHub pull requests. Please send pull requests against the [github.com/apache/arrow-swift](https://github.com/apache/arrow-swift) repository.

If you are looking for some ideas on what to contribute, check out the [GitHub
Issues](https://github.com/apache/arrow-swift/issues) for the Apache Arrow Swift project. Comment on the issue, [GitHub Discussions](https://github.com/apache/arrow-swift/discussions) and/or contact [dev@arrow.apache.org](https://arrow.apache.org/community/#mailing-lists) with your questions and ideas.

If you'd like to report a bug but don't have time to fix it, you can still post it on GitHub issues.

## License

[Apache 2.0](https://github.com/apache/arrow-swift/blob/main/LICENSE.txt)
