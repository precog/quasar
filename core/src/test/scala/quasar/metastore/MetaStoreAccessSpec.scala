/*
 * Copyright 2014–2017 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.metastore

import quasar.fs.FileSystemType
import quasar.fs.mount.{ConnectionUri, MountConfig}

import doobie.contrib.specs2.analysisspec.AnalysisSpec
import doobie.util.transactor.Transactor
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineMV
import org.specs2.mutable._
import pathy.Path, Path._
import scalaz.concurrent.Task

abstract class MetaStoreAccessSpec extends Specification with AnalysisSpec {
  val schema = Schema.schema

  def rawTransactor: Transactor[Task]

  def refineNonNeg = refineMV[NonNegative]

  "static query checks" >> {

    // NB: these tests do not execute the queries or validate results, but only
    // type-check them against the schema available via the transactor.

    check(Queries.fsMounts)
    check(Queries.mountsHavingPrefix(rootDir))
    check(Queries.lookupMountType(rootDir))
    check(Queries.lookupMountConfig(rootDir))
    check(Queries.insertMount(rootDir, MountConfig.fileSystemConfig(FileSystemType(""), ConnectionUri(""))))
    check(Queries.deleteMount(rootDir))
  }
}

class EphemeralH2AccessSpec extends MetaStoreAccessSpec with H2MetaStoreFixture
