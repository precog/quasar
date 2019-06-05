# Quasar [![Travis](https://travis-ci.com/slamdata/quasar.svg?branch=master)](https://travis-ci.com/slamdata/quasar) [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

Quasar is a purely-functional compiler and optimizing planner for queries expressed in terms of the [Multidimensional Relational Algebra (MRA)](https://slamdata.com/how-it-works/). Quasar has support for arbitrary backends, both heavyweight (full evaluation engines) and lightweight (simple reads with optional pushdown of structural operations and columnar predicates), including full classpath isolation for lightweight backends.

It's important to note that Quasar is not, in and of itself, a runnable application. It is a library which is used by the broader SlamData product, much of which is closed-source. Contributions are very much welcome, as is feedback, questions, and general conversation. Join the Discord!

## Building and Testing

Quasar builds with SBT:

```bash
$ ./sbt
> test:compile
> test
```

If running on Windows, you may use the SBT batch file instead of the shell script.

## Code Organization

Probably the most interesting part of the codebase is the optimizing query planner, which is implemented in the **qsu** submodule, based on data structures defined in **qscript**. I recommend starting by looking at the `LPtoQS` class, which defines a kleisli composition that clearly lays out all of the phases of the compiler. The core data structure used by the compiler is `QSUGraph`, which is a purely functional representation of a directed acyclic graph, which in turn represents data flow in a query.

The formulation of the query plan itself is a fixed-point data structure dictated by several pattern functors composed via coproducts. The primary such pattern functor is `QScriptCore`. You would generally deconstruct and interpret this query plan using general folds provided by [matryoshka](https://github.com/slamdata/matryoshka).

Query operations which are pushed down to the underlying data source are represented by `ScalarStage`, and carried via `InterpretedRead`. Data sources are always free to only implement a subset of the pushdown functionality.

The codebase makes extremely heavy use of [Scalaz](https://github.com/scalaz/scalaz) and [Cats](https://github.com/typelevel/cats) throughout (using [shims](https://github.com/djspiewak/shims) to solve the impedance between them), and many high-level operations (such as datasets) are represented as [fs2](https://fs2.io) streams.

## Local Datasource

A `Datasource` implementation providing access to the filesystems local to the JVM.

Configuration for the local datasource has the following JSON format

```json
{
  "rootDir": String,
  "format": {
    "type": "json",
    "variant": "array-wrapped" | "line-delimited",
    [precise: Boolean]
  },
  ["readChunkSizeBytes": Number,]
  ["compressionScheme": "gzip"]
}
```
* `rootDir` an absolute path to a local directory at which to root the datasource, all paths handled by the datasource will be interpreted relative to this physical directory.
* `format` the format of _all_ resources in the datasource, currently JSON is supported in both array-wrapped and line-delimited variants.
* `readChunkSizeBytes` (optional) an integer indicating the chunk size to use when reading local files, the default is `1048576` (1MB). Different values may yield higher throughput depending on the filesystem.
* `compressionScheme` (optional) whether to expect resources to be compressed, currently `gzip` is the only supported compression scheme. Omitting this option indicates uncompressed resources.

## Legal

Copyright &copy; 2014 - 2019 SlamData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
