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

package quasar.main.api

import quasar.contrib.scalaz._
import quasar.fs.mount._
import quasar.fp.ski._
import quasar.main._
import quasar.metastore.MetaStoreFixture
import quasar.sql._

import scala.StringContext

import pathy.Path._
import scalaz._, Scalaz._

class ApiSpec extends quasar.Qspec {
  "Api" >> {
    "change metastore at runtime" >> {
      val sampleMountPath = rootDir </> file("foo")
      val sampleMount = MountConfig.viewConfig0(sqlB"select * from zips")
      val mount = Mounting.Ops[CoreEff]
      (for {
        firstMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
        quasarFS <- Quasar.initWithDbConfig(firstMetaConf, _ => ().point[MainTask]).run.map(a => a.leftMap(e => new scala.Exception(e.shows))).unattempt
        run0 = quasarFS.taskInter
        result <- (for {
          _             <- mount.mountOrReplace(sampleMountPath, sampleMount, false).foldMap(run0)
          mountIsThere  <- mount.lookupConfig(sampleMountPath).run.run.map(_.isDefined).foldMap(run0)
          otherMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
          _             <- MetaStoreLocation.Ops[CoreEff].set(otherMetaConf, initialize = true).foldMap(run0)
          noLongerThere <- mount.lookupConfig(sampleMountPath).run.run.map(_.empty).foldMap(run0)
          _             <- MetaStoreLocation.Ops[CoreEff].set(firstMetaConf, initialize = true).foldMap(run0)
          thereAgain    <- mount.lookupConfig(sampleMountPath).run.run.map(_.isDefined).foldMap(run0)
        } yield {
          mountIsThere must_=== true
          noLongerThere must_=== true
          thereAgain must_=== true
        }).onFinish(κ(quasarFS.shutdown))
      } yield result).unsafePerformSync
    }
  }
}