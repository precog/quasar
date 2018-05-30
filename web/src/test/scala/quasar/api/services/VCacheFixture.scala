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

package quasar.api.services

import slamdata.Predef._
import quasar.api._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.metastore.MetaStoreFixture
import quasar.fs.mount._
import quasar.fs.mount.cache.{VCache, ViewCache}
import quasar.fs.mount.cache.VCache.{VCacheKVS, VCacheExpR, VCacheExpW}
import quasar.fs.ReadFile.ReadHandle
import quasar.metastore.H2MetaStoreFixture

import java.time.Instant

import doobie.imports.ConnectionIO
import scalaz.{Failure => _, Zip =>_, _}, Scalaz._
import scalaz.concurrent.Task

import eu.timepit.refined.auto._

trait VCacheFixture extends H2MetaStoreFixture {
  import InMemory._

  type Eff2[A] = Coproduct[FileSystemFailure, FileSystem, A]
  type Eff1[A] = Coproduct[VCacheKVS, Eff2, A]
  type Eff0[A] = Coproduct[Timing, Eff1, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]
  type EffM[A] = Free[Eff, A]

  type ViewEff[A] =
    Coproduct[PathMismatchFailure,
      Coproduct[MountingFailure,
        Coproduct[Mounting,
          Coproduct[view.State,
            Coproduct[MonotonicSeq,
              Coproduct[VCacheExpR,
                Coproduct[VCacheExpW, Eff, ?], ?], ?], ?], ?], ?], A]

  val vcacheInterp: Task[VCacheKVS ~> Task] = KeyValueStore.impl.default[AFile, ViewCache]

  val vcache = VCacheKVS.Ops[ViewEff]

  def timingInterp(i: Instant) = λ[Timing ~> Task] {
    case Timing.Timestamp => Task.now(i)
    case Timing.Nanos     => Task.now(0)
  }

  def evalViewTest[A](
    now: Instant, mounts: Map[APath, MountConfig], inMemState: InMemState
  )(
    p: (ViewEff ~> Task, ViewEff ~> FailedResponseOr) => Task[A]
  ): Task[A] = {
    def viewFs: Task[ViewEff ~> Task] =
      (runFs(inMemState)                                         ⊛
       TaskRef(mounts)                                           ⊛
       KeyValueStore.impl.default[ReadHandle, view.ResultHandle] ⊛
       MonotonicSeq.from(0L)                                     ⊛
       TaskRef(Tags.Min(none[VCache.Expiration]))                ⊛
       MetaStoreFixture.createNewTestTransactor()
      ) { (fs, m, vs, s, r, t) =>
        val mountingInter =
          foldMapNT(KeyValueStore.impl.fromTaskRef(m)) compose Mounter.trivial[MountConfigs]

        val viewInterpF: ViewEff ~> Free[ViewEff, ?] =
          injectFT[PathMismatchFailure, ViewEff] :+:
          injectFT[MountingFailure, ViewEff]     :+:
          injectFT[Mounting, ViewEff]            :+:
          injectFT[view.State, ViewEff]          :+:
          injectFT[MonotonicSeq, ViewEff]        :+:
          injectFT[VCacheExpR, ViewEff]          :+:
          injectFT[VCacheExpW, ViewEff]          :+:
          injectFT[Task, ViewEff]                :+:
          injectFT[Timing, ViewEff]              :+:
          injectFT[VCacheKVS, ViewEff]           :+:
          injectFT[FileSystemFailure, ViewEff]   :+:
          view.fileSystem[ViewEff]

        val cw: VCacheExpW ~> Task =
          Write.fromTaskRef(r)

        val vc: VCacheKVS ~> Task =
          foldMapNT(
            (fs compose injectNT[ManageFile, FileSystem])   :+:
            Failure.showRuntimeError[Task, FileSystemError] :+:
            t.trans                                         :+:
            cw
          ) compose
            VCache.interp[(ManageFile :\: FileSystemFailure :\: ConnectionIO :/: VCacheExpW)#M]

        val viewInterp: ViewEff ~> Task =
          Failure.showRuntimeError[Task, Mounting.PathTypeMismatch] :+:
          Failure.showRuntimeError[Task, MountingError]             :+:
          mountingInter                                             :+:
          vs                                                        :+:
          s                                                         :+:
          Read.fromTaskRef(r)                                       :+:
          cw                                                        :+:
          reflNT[Task]                                              :+:
          timingInterp(now)                                         :+:
          vc                                                        :+:
          Failure.showRuntimeError[Task, FileSystemError]           :+:
          fs

        foldMapNT(viewInterp) compose viewInterpF
      }

    viewFs >>= (fs => p(fs, liftMT[Task, FailedResponseT] compose fs))
  }
}

object VCacheFixture extends VCacheFixture {
  val schema: quasar.db.Schema[Int] = quasar.metastore.Schema.schema
}
