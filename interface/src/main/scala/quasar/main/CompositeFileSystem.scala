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

package quasar.main

import slamdata.Predef._
import quasar.contrib.pathy.ADir
import quasar.contrib.scalaz.catchable._
import quasar.effect.{Read, Failure}
import quasar.fp._
import quasar.fp.free._
import quasar.fs.{BackendEffect, PhysErr, Empty, PhysicalError}
import quasar.fs.QueryFile.ResultHandle
import quasar.fs.mount.BackendDef.DefinitionResult
import quasar.fs.mount._
import quasar.main.metastore.jdbcMounter
import quasar.metastore.{MetaStore, MetaStoreAccess}

import doobie.imports.ConnectionIO
import doobie.syntax.connectionio._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

/** A FileSystem supporting filesystems mounted at various
  * points in the hierarchy.
  */
object CompositeFileSystem {

  type Mounting_QErrs_Task[A] = Coproduct[Mounting, QErrs_Task, A]

  type QErrs_FsAsk_Task[A]   = Coproduct[FsAsk, QErrs_Task, A]
  type QErrs_FsAsk[A]        = Coproduct[FsAsk, QErrs, A]
  type QErrs_CnxIO_FsAsk[A]  = Coproduct[ConnectionIO, QErrs_FsAsk, A]
  type QErrs_FsAsk_TaskM[A]  = Free[QErrs_FsAsk_Task, A]

  object QErrs_FsAsk_Task {
    def toMainTask(mountTypes: BackendDef[PhysFsEffM]): QErrs_FsAsk_TaskM ~> MainTask =
      foldMapNT(
        Read.constant[MainTask, BackendDef[PhysFsEffM]](mountTypes) :+:
        liftMT[Task, MainErrT] :+:
        QErrs.toMainErrT[Task])
  }

  def init(
    metaRef: TaskRef[MetaStore],
    mountTypes: BackendDef[PhysFsEffM],
    mounts: MountingsConfig
  ): MainTask[FS] = for {
    hfsRef     <- TaskRef(Empty.backendEffect[HierarchicalFsEffM]).liftM[MainErrT]
    mntdRef    <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]).liftM[MainErrT]

    ephmralMnt =  KvsMounter.interpreter[Task, QErrs_FsAsk_Task](
      KvsMounter.ephemeralMountConfigs[Task], hfsRef, mntdRef) andThen
      QErrs_FsAsk_Task.toMainTask(mountTypes)

    // TODO: Still need to expose these in the HTTP API, see SD-1131
    failedMnts <- attemptMountAll[Mounting](mounts) foldMap ephmralMnt
    _          <- failedMnts.toList.traverse_(logFailedMount).liftM[MainErrT]

    runCore    <- CompositeFileSystem.interpreter[Mounting_QErrs_Task](hfsRef).liftM[MainErrT]
  } yield {
    val g: QErrs_CnxIO_FsAsk ~> QErrs_TaskM =
      (injectFT[Task, QErrs_Task] compose connectionIOToTask(metaRef)) :+:
      Read.constant[QErrs_TaskM, BackendDef[PhysFsEffM]](mountTypes)   :+:
      injectFT[QErrs, QErrs_Task]
    val mounting: Mounting ~> QErrs_TaskM = foldMapNT(g) compose jdbcMounter[QErrs_CnxIO_FsAsk](hfsRef, mntdRef)
    val f: Mounting_QErrs_Task ~> QErrs_TaskM =
      mounting                   :+:
      injectFT[Task, QErrs_Task] :+:
      injectFT[QErrs, QErrs_Task]
    val h: BackendEffect ~> QErrs_TaskM = foldMapNT(f) compose runCore
    FS(h, mounting, mntdRef.read >>= closeAllFsMounts _)
  }

  def initWithMountsInMetaStore(loadConfig: BackendConfig, metaRef: TaskRef[MetaStore]): MainTask[FS] =
    for {
      mountTypes <- physicalFileSystems(loadConfig).liftM[MainErrT]

      metastore <- metaRef.read.liftM[MainErrT]

      mountsCfg  <- MetaStoreAccess.fsMounts
        .map(MountingsConfig(_))
        .transact(metastore.trans.transactor)
        .liftM[MainErrT]

      fsThing    <- init(metaRef, mountTypes, mountsCfg)
    } yield fsThing

  /** Interprets FileSystem given a TaskRef containing a hierarchical
    * FileSystem interpreter.
    *
    * TODO: The TaskRef is used as a communications channel so that
    *       the part of the system that deals with mounting can make
    *       new interpreters available to the part of the system that
    *       needs to interpret FileSystem operations.
    *
    *       This would probably be better served with a
    *       `Process[Task, FileSystem ~> HierarchicalFsEffM]` to allow
    *       for more flexible production of interpreters.
    */
  def interpreter[S[_]](
    hfsRef: TaskRef[BackendEffect ~> HierarchicalFsEffM]
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S,
    S2: Mounting :<: S,
    S4: MountingFailure :<: S,
    S5: PathMismatchFailure :<: S
  ): Task[BackendEffect ~> Free[S, ?]] =
    for {
      startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
      seqRef     <- TaskRef(startSeq)
      mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
    } yield
      HierarchicalFsEff.dynamicFileSystem(
        hfsRef,
        HierarchicalFsEff.interpreter[S](seqRef, mntedRHRef))

  private def closeFileSystem(dr: DefinitionResult[PhysFsEffM]): Task[Unit] = {
    val transform: PhysFsEffM ~> Task =
      foldMapNT(reflNT[Task] :+: (Failure.toCatchable[Task, Exception] compose Failure.mapError[PhysicalError, Exception](_.cause)))

    dr.translate(transform).close
  }

  private def closeAllFsMounts(mnts: Mounts[DefinitionResult[PhysFsEffM]]): Task[Unit] =
    mnts.traverse_(closeFileSystem(_).attemptNonFatal.void)
}
