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
import quasar.config.MetaStoreConfig
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.db.DbConnectionConfig
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.BackendDef.DefinitionResult
import quasar.main.metastore._
import quasar.metastore._

import doobie.imports.{ConnectionIO, Transactor}
import doobie.syntax.connectionio._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

/**
  * The Quasar Filesystem. Contains the `CoreEff` that can be used to interpret most
  * operations. Also has convenience methods for executing common operation within `Task`.
  * The `shutdown` task should be called once you are done with it.
  * @param interp
  * @param shutdown Trigger the underlying connector drivers to shutdown cleanly.
  */
final case class Quasar(interp: CoreEff ~> QErrs_TaskM, shutdown: Task[Unit]) {
  val taskInter: CoreEff ~> Task =
    foldMapNT(NaturalTransformation.refl[Task] :+: QErrs.toCatchable[Task]) compose interp
}

object Quasar {

  type QErrsCnxIO[A]  = Coproduct[ConnectionIO, QErrs, A]
  type QErrsTCnxIO[A] = Coproduct[Task, QErrsCnxIO, A]
  type QErrs_CnxIO_Task_MetaStoreLoc[A] = Coproduct[MetaStoreLocation, QErrsTCnxIO, A]
  type QErrs_CnxIO_Task_MetaStoreLocM[A] = Free[QErrs_CnxIO_Task_MetaStoreLoc, A]

  object QErrsCnxIO {
    def qErrsToMainErrT[F[_]: Catchable: Monad]: QErrs ~> MainErrT[F, ?] =
      liftMT[F, MainErrT].compose(QErrs.toCatchable[F])

    def toMainTask(transactor: Transactor[Task]): QErrsCnxIOM ~> MainTask = {
      val f: QErrsCnxIOM ~> MainErrT[ConnectionIO, ?] =
        foldMapNT(liftMT[ConnectionIO, MainErrT] :+: qErrsToMainErrT[ConnectionIO])

      Hoist[MainErrT].hoist(transactor.trans) compose f
    }
  }

  type QErrsCnxIOM[A]  = Free[QErrsCnxIO, A]
  type QErrsTCnxIOM[A] = Free[QErrsTCnxIO, A]

  object QErrsTCnxIO {
    def toMainTask(transactor: Transactor[Task]): QErrsTCnxIOM ~> MainTask = {
      val f: QErrsTCnxIOM ~> MainErrT[ConnectionIO, ?] =
        foldMapNT(
          (liftMT[ConnectionIO, MainErrT] compose taskToConnectionIO) :+:
            liftMT[ConnectionIO, MainErrT]                              :+:
            QErrsCnxIO.qErrsToMainErrT[ConnectionIO])

      Hoist[MainErrT].hoist(transactor.trans) compose f
    }
  }

  val absorbTask: QErrsTCnxIO ~> QErrsCnxIO =
    Inject[ConnectionIO, QErrsCnxIO].compose(taskToConnectionIO) :+: reflNT[QErrsCnxIO]

  /** Initialize the Quasar FileSystem assuming all defaults
    * The metastore can be changed but it will not be persisted to the config file.
    */
  val init: MainTask[Quasar] = initFromMetaConfig(None, _ => ().point[MainTask])

  /** Initialize the Quasar FileSytem using the specified metastore configuration
    * or with the default if not provided.
    */
  def initFromMetaConfig(metaCfg: Option[MetaStoreConfig], persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      metastoreCfg <- metaCfg.cata(Task.now, MetaStoreConfig.default).liftM[MainErrT]
      quasarFS     <- initWithDbConfig(metastoreCfg.database, persist)
    } yield quasarFS

  def initWithDbConfig(db: DbConnectionConfig, persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      metastore <- metastoreTransactor(db)
      metaRef   <- TaskRef(metastore).liftM[MainErrT]
      quasarFS  <- initWithMeta(metaRef, persist, db.isInMemory)
    } yield quasarFS

  def initWithMeta(metaRef: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit], initialize: Boolean): MainTask[Quasar] =
    for {
      metastore  <- metaRef.read.liftM[MainErrT]
      _          <- if (initialize)
        initUpdateMigrate(quasar.metastore.Schema.schema, metastore.trans.transactor, None)
      else Task.now(()).liftM[MainErrT]
      _          <- verifySchema(quasar.metastore.Schema.schema, metastore.trans.transactor).leftMap(_.message)
      hfsRef     <- TaskRef(Empty.backendEffect[HierarchicalFsEffM]).liftM[MainErrT]
      mntdRef    <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]).liftM[MainErrT]

      ephmralMnt =  KvsMounter.interpreter[Task, QErrsTCnxIO](
        KvsMounter.ephemeralMountConfigs[Task], hfsRef, mntdRef) andThen
        mapSNT(absorbTask)                                       andThen
        QErrsCnxIO.toMainTask(metastore.trans.transactor)

      mountsCfg  <- MetaStoreAccess.fsMounts
        .map(MountingsConfig(_))
        .transact(metastore.trans.transactor)
        .liftM[MainErrT]

      // TODO: Still need to expose these in the HTTP API, see SD-1131
      failedMnts <- attemptMountAll[Mounting](mountsCfg) foldMap ephmralMnt
      _          <- failedMnts.toList.traverse_(logFailedMount).liftM[MainErrT]

      runCore    <- CoreEff.runFs[QEffIO](hfsRef).liftM[MainErrT]
    } yield {
      val f: QEffIO ~> QErrs_CnxIO_Task_MetaStoreLocM =
        injectFT[Task, QErrs_CnxIO_Task_MetaStoreLoc]               :+:
          injectFT[MetaStoreLocation, QErrs_CnxIO_Task_MetaStoreLoc]  :+:
          jdbcMounter[QErrs_CnxIO_Task_MetaStoreLoc](hfsRef, mntdRef) :+:
          injectFT[QErrs, QErrs_CnxIO_Task_MetaStoreLoc]

      val connectionIOToTask: ConnectionIO ~> Task =
        λ[ConnectionIO ~> Task](io => metaRef.read.flatMap(t => t.trans.transactor.trans(io)))
      val g: QErrs_CnxIO_Task_MetaStoreLoc ~> QErrs_TaskM =
        (injectFT[Task, QErrs_Task] compose MetaStoreLocation.impl.default(metaRef, persist)) :+:
         injectFT[Task, QErrs_Task]                                                           :+:
        (injectFT[Task, QErrs_Task] compose connectionIOToTask)                               :+:
         injectFT[QErrs, QErrs_Task]

      Quasar(
        foldMapNT(g) compose foldMapNT(f) compose runCore,
        (mntdRef.read >>= closeAllFsMounts _) *> metaRef.read.flatMap(_.trans.shutdown))
    }

  private def closeFileSystem(dr: DefinitionResult[PhysFsEffM]): Task[Unit] = {
    val transform: PhysFsEffM ~> Task =
      foldMapNT(reflNT[Task] :+: (Failure.toCatchable[Task, Exception] compose Failure.mapError[PhysicalError, Exception](_.cause)))

    dr.translate(transform).close
  }

  private def closeAllFsMounts(mnts: Mounts[DefinitionResult[PhysFsEffM]]): Task[Unit] =
    mnts.traverse_(closeFileSystem(_).attemptNonFatal.void)
}