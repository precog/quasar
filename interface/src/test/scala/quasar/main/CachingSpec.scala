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

package quasar.main

import slamdata.Predef._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.{Failure, Timing, Write}
import quasar.fp._, free._
import quasar.fs._, InMemory._
import quasar.fs.mount._, Mounting.PathTypeMismatch
import quasar.fs.mount.module.Module
import quasar.fs.mount.cache.{VCache, ViewCache}, VCache.{VCacheKVS, VCacheExpW}
import quasar.metastore.H2MetaStoreFixture
import quasar.metastore.MetaStoreFixture.createNewTestMetaStoreConfig
import quasar.sql._
import quasar.Variables

import java.time.Instant
import scala.concurrent.duration._

import doobie.imports.ConnectionIO
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

final class CachingSpec extends quasar.Qspec with H2MetaStoreFixture {
  import Caching.Eff

  val schema: quasar.db.Schema[Int] = quasar.metastore.Schema.schema

  type MountingFileSystem[A] = Coproduct[Mounting, FileSystem, A]

  val mount = λ[Mounting ~> Task](_ => Task.fail(new RuntimeException("unimplemented")))

  def timingInterp(i: Instant) = λ[Timing ~> Task] {
    case Timing.Timestamp => Task.now(i)
    case Timing.Nanos     => Task.now(0)
  }

  def vcacheInterp(fs: FileSystem ~> Task, cw: VCacheExpW ~> Task): VCacheKVS ~> Task =
    foldMapNT(
      (fs compose injectNT[ManageFile, FileSystem]) :+:
      Failure.showRuntimeError[Task, FileSystemError] :+:
      transactor.trans :+:
      cw
    ) compose
      VCache.interp[(ManageFile :\: FileSystemFailure :\: ConnectionIO :/: VCacheExpW)#M]

  def eff(i: Instant): Task[Eff ~> Task] =
    (
      runFs(InMemState.empty)      ⊛
      createNewTestMetaStoreConfig ⊛
      TaskRef(Tags.Min(Option.empty[VCache.Expiration]))
    )((fs, metaConf, r) =>
      NaturalTransformation.refl[Task]                                          :+:
      transactor.trans                                                          :+:
      MetaStoreLocation.impl.constant(metaConf)                                 :+:
      (foldMapNT(mount :+: fs) compose Module.impl.default[MountingFileSystem]) :+:
      mount                                                                     :+:
      Empty.analyze[Task]                                                       :+:
      (fs compose Inject[QueryFile, FileSystem])                                :+:
      (fs compose Inject[ReadFile, FileSystem])                                 :+:
      (fs compose Inject[WriteFile, FileSystem])                                :+:
      (fs compose Inject[ManageFile, FileSystem])                               :+:
      vcacheInterp(fs, Write.fromTaskRef(r))                                    :+:
      timingInterp(i)                                                           :+:
      Failure.showRuntimeError[Task, Module.Error]                              :+:
      Failure.showRuntimeError[Task, PathTypeMismatch]                          :+:
      Failure.showRuntimeError[Task, MountingError]                             :+:
      Failure.showRuntimeError[Task, FileSystemError])

  "Caching" should {
    "refresh" >> {
      val f = rootDir </> file("f")
      val g = rootDir </> file("g")

      val expr = sqlB"""select {"α": 42}"""

      val i = Instant.parse("1970-01-01T01:00:00Z")

      val viewCache = ViewCache(
        MountConfig.ViewConfig(expr, Variables.empty), None, None, 0, None, None,
        600L, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, g, None)

      def eval: Task[Free[Eff, ?] ~> Task] = eff(i) ∘ (foldMapNT(_))

      val vcache = VCacheKVS.Ops[Eff]

      val r: (Option[ViewCache], FileSystemError \/ Vector[Data]) =
        (eval >>= { e =>
          val r: Process[Task, Unit] = Caching.refresh(e, "the freshness")

          val p: Free[Eff, (Option[ViewCache], FileSystemError \/ Vector[Data])] =
            for {
              _  <- vcache.put(f, viewCache)
              _  <- lift(r.take(1).run).into[Eff]
              vc <- vcache.get(f).run
              gg <- ReadFile.Ops[Eff].scanAll(g).runLog.run
            } yield (vc, gg)

          e(p)
        }).unsafePerformSync

      r must_= ((
        viewCache.copy(
          lastUpdate = i.some,
          executionMillis = 0L.some,
          assigneeStart = i.some,
          refreshAfter = ViewCache.expireAt(i, viewCache.maxAgeSeconds.seconds) | i,
          status = ViewCache.Status.Successful).some,
        Vector(Data.Obj(ListMap("α" -> Data.Int(42)))).right[FileSystemError]
      ))
    }
  }
}
