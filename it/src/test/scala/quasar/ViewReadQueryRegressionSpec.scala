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

package quasar

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, APath}
import quasar.effect._
import quasar.fp._ , free._
import quasar.fs.{Empty, PhysicalError, ReadFile, UnhandledFSError}
import quasar.fs.mount._, BackendDef.DefinitionResult
import quasar.fs.mount.cache.VCache, VCache.VCacheKVS
import quasar.fs.mount.Fixture.runConstantVCache
import quasar.main._
import quasar.regression._
import quasar.sql.{ScopedExpr, Sql}

import matryoshka.data.Fix
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

class ViewReadQueryRegressionSpec
  extends QueryRegressionTest[BackendEffectIO](QueryRegressionTest.externalFS.map(_.take(1))) {

  val TestsDir = dir("resultFile").some

  val suiteName = "View Reads"

  type ViewFS[A] = (Mounting :\: view.State :\: VCacheKVS :\: MonotonicSeq :/: BackendEffectIO)#M[A]

  type FsAskPhysFsEff[A] = Coproduct[FsAsk, PhysFsEff, A]

  def mounts(path: APath, expr: Fix[Sql], vars: Variables): Task[Mounting ~> Task] =
    (
      TaskRef(Map[APath, MountConfig](path -> MountConfig.viewConfig(ScopedExpr(expr, Nil), vars))) |@|
      TaskRef(Empty.backendEffect[HierarchicalFsEffM]) |@|
      TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]) |@|
      physicalFileSystems(BackendConfig.Empty)   // test views just against mimir
    ) { (cfgsRef, hfsRef, mntdRef, mounts) =>
      val mnt =
        KvsMounter.interpreter[Task, FsAskPhysFsEff](
          KeyValueStore.impl.fromTaskRef(cfgsRef), hfsRef, mntdRef)

      foldMapNT(
        Read.constant[Task, BackendDef[PhysFsEffM]](mounts) :+:
        reflNT[Task] :+:
        Failure.toRuntimeError[Task, PhysicalError] {
          case UnhandledFSError(e) => e
        }
      ).compose(mnt)
    }

  val seq = TaskRef(0L).map(MonotonicSeq.fromTaskRef)

  val RF = ReadFile.Ops[ReadFile]

  def queryResults(query: Fix[Sql], vars: Variables, basePath: ADir) = {
    val path = basePath </> file("view")
    val prg: Process[RF.unsafe.M, Data] = RF.scanAll(path)
    val interp = mounts(path, query, vars).flatMap(interpViews).unsafePerformSync

    def t: RF.unsafe.M ~> CE.CompExecM =
      new (RF.unsafe.M ~> CE.CompExecM) {
        def apply[A](fa: RF.unsafe.M[A]): CE.CompExecM[A] = {
          val u: ReadFile ~> Free[BackendEffectIO, ?] =
            mapSNT(interp) compose view.readFile[ViewFS]

          EitherT(EitherT.rightT(WriterT.put(fa.run.flatMapSuspension(u))(Vector.empty)))
        }
      }

    prg.translate(t)
  }

  def interpViews(mnts: Mounting ~> Task): Task[ViewFS ~> BackendEffectIO] =
    (view.State.toTask(Map()) |@| seq)((v, s) =>
      (injectNT[Task, BackendEffectIO] compose mnts)                               :+:
      (injectNT[Task, BackendEffectIO] compose v)                                  :+:
      (injectNT[Task, BackendEffectIO] compose runConstantVCache[Task](Map.empty)) :+:
      (injectNT[Task, BackendEffectIO] compose s)                                  :+:
      reflNT[BackendEffectIO])
}
