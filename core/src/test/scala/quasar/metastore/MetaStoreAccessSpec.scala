/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._
import quasar.Variables
import quasar.fs.FileSystemType
import quasar.fs.mount.{ConnectionUri, MountConfig, MountType}
import quasar.fs.mount.cache.ViewCache
import quasar.sql._

import java.time.Instant

import doobie.util.transactor.Transactor
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.refineMV
import org.specs2.mutable._
import pathy.Path
import Path._
import doobie.specs2.analysisspec.TaskChecker

import scalaz.concurrent.Task

abstract class MetaStoreAccessSpec extends Specification with TaskChecker with MetaStoreFixture {
  val schema = Schema.schema

  def rawTransactor: Transactor[Task]

  def refineNonNeg = refineMV[NonNegative]

  "static query checks" >> {

    val f = rootDir </> file("α")
    val instant = Instant.ofEpochSecond(0)
    val viewCache = ViewCache(
      MountConfig.ViewConfig(sqlB"α", Variables.empty), None, None, 0, None, None,
      0, instant, ViewCache.Status.Pending, None, f, None)

    val pathedMountConfig = PathedMountConfig(
      rootDir </> file("mimir"),
      MountType.fileSystemMount(FileSystemType("local")),
      ConnectionUri("/tmp/local"))

    // NB: these tests do not execute the queries or validate results, but only
    // type-check them against the schema available via the transactor.

    check(Queries.fsMounts)
    check(Queries.mounts)
    check(Queries.insertPathedMountConfig(pathedMountConfig))
    check(Queries.mountsHavingPrefix(rootDir))
    check(Queries.lookupMountType(rootDir))
    check(Queries.lookupMountConfig(rootDir))
    check(Queries.insertMount(rootDir, MountConfig.fileSystemConfig(FileSystemType(""), ConnectionUri(""))))
    check(Queries.deleteMount(rootDir))
    check(Queries.viewCachePaths)
    check(Queries.lookupViewCache(f))
    check(Queries.insertViewCache(f, viewCache))
    check(Queries.updateViewCache(f, viewCache))
    check(Queries.updateViewCacheErrorMsg(f, "err"))
    check(Queries.deleteViewCache(f))
    check(Queries.staleCachedViews(instant))
    check(Queries.cacheRefreshAssigneStart(f, "α", instant, f))
    check(Queries.updatePerSuccesfulCacheRefresh(f, instant, 0, instant))
  }

  "fsMounts" should {
    "not attempt to load views and modules" >> {
      interpretIO(for {
        _        <- MetaStoreAccess.insertMount(rootDir </> file("view"), MountConfig.viewConfig0(sqlB"1"))
        _        <- MetaStoreAccess.insertMount(rootDir </> dir("module"), MountConfig.moduleConfig(sqlM"IMPORT `/f/`"))
        fsMounts <- MetaStoreAccess.fsMounts
      } yield {
        fsMounts must_=== Map.empty
      }).unsafePerformSync
    }
  }
}

class EphemeralH2AccessSpec extends MetaStoreAccessSpec with H2MetaStoreFixture
