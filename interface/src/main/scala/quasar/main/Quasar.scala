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
import quasar.{QuasarErrT, QuasarError}
import quasar.config.MetaStoreConfig
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.concurrent._
import quasar.contrib.scalaz.eitherT._
import quasar.db.DbConnectionConfig
import quasar.effect.{Failure, Read, Write}
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs.{FileSystemError, PhysicalError}
import quasar.fs.mount.{Mounting, MountingError}
import quasar.fs.mount.cache.VCache
import quasar.fs.mount.module.Module
import quasar.metastore.MetaStore.ShouldInitialize
import quasar.metastore._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

/**
  * The Quasar Filesystem. Contains the `CoreEff` that can be used to interpret most
  * operations. Also has convenience methods for executing common operation within `Task`.
  * The `shutdown` task should be called once you are done with it.
  * @param interp
  * @param shutdown Trigger the underlying connector drivers to shutdown cleanly.
  */
final case class Quasar(interp: CoreEff ~> QErrs_CRW_TaskM, shutdown: Task[Unit]) {
  val taskInter: Task[CoreEff ~> Task] =
    Quasar.toTask ∘ (_ compose interp)

  def interpWithErrs: Task[CoreEff ~> EitherT[Task, QuasarError, ?]] = {
    val lift = liftMT[Task, QuasarErrT]
    TaskRef(Tags.Min(none[VCache.Expiration])).map{ r =>
      val preserveErrors: QErrs_CRW_TaskM ~> EitherT[Task, QuasarError, ?] =
        foldMapNT(
          (Read.fromTaskRef(r) andThen lift) :+:
            (Write.fromTaskRef(r) andThen lift) :+:
            lift :+:
            (Failure.toError[EitherT[Task, PhysicalError, ?], PhysicalError] andThen leftMapNT(e => e:QuasarError)) :+:
            (Failure.toError[EitherT[Task, Module.Error, ?], Module.Error] andThen leftMapNT(e => e:QuasarError)) :+:
            (Failure.toError[EitherT[Task, Mounting.PathTypeMismatch, ?], Mounting.PathTypeMismatch] andThen leftMapNT(e => e:QuasarError)) :+:
            (Failure.toError[EitherT[Task, MountingError, ?], MountingError] andThen leftMapNT(e => e:QuasarError)) :+:
            (Failure.toError[EitherT[Task, FileSystemError, ?], FileSystemError] andThen leftMapNT(e => e:QuasarError)))
      interp andThen preserveErrors
    }
  }

  def extendShutdown(step: Task[Unit]): Quasar =
    copy(shutdown = shutdown.attemptNonFatal.void >> step)
}

object Quasar {

  def initMimirOnly: MainTask[Quasar] =
    init(BackendConfig.Empty)

  /** Initialize the Quasar FileSystem assuming all defaults
    * The metastore can be changed but it will not be persisted to the config file.
    *
    * Not used in the codebase, but useful for manual testing at the console.
    */
  def init(loadConfig: BackendConfig): MainTask[Quasar] =
    initFromMetaConfig(loadConfig, None, _ => ().point[MainTask])

  /** Initialize the Quasar FileSytem using the specified metastore configuration
    * or with the default if not provided.
    */
  def initFromMetaConfig(loadConfig: BackendConfig, metaCfg: Option[MetaStoreConfig], persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      metastoreCfg <- metaCfg.cata(Task.now, MetaStoreConfig.default).liftM[MainErrT]
      quasarFS     <- initWithDbConfig(loadConfig, metastoreCfg.database, persist)
    } yield quasarFS

  def initWithDbConfig(loadConfig: BackendConfig, db: DbConnectionConfig, persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      metastore <- MetaStore.connect(db, ShouldInitialize(db.isInMemory), List(quasar.metastore.Schema.schema), List(MetaStore.copy)).leftMap(_.message)
      metaRef   <- TaskRef(metastore).liftM[MainErrT]
      quasarFS  <- initWithMeta(loadConfig, metaRef, persist)
    } yield quasarFS.extendShutdown(metaRef.read.flatMap(_.shutdown))

  def initWithMeta(loadConfig: BackendConfig, metaRef: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      _ <- shift.liftM[MainErrT]
      fsThing  <- CompositeFileSystem.initWithMountsInMetaStore(loadConfig, metaRef)
      _ <- shift.liftM[MainErrT]
      quasarFS <- initWithFS(fsThing, metaRef, persist).liftM[MainErrT]
      _ <- shift.liftM[MainErrT]
    } yield quasarFS.extendShutdown(fsThing.shutdown)

  def initWithFS(fsThing: FS, metaRef: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit]): Task[Quasar] =
    for {
      finalEval  <- CoreEff.defaultImpl(fsThing, metaRef, persist)
      tTask      <- toTask
      loggedEval =  flatMapSNT(QErrs_CRW_Task.logFatalErrors) compose finalEval
      cacheCtx   <- Caching.viewCacheRefreshCtx(foldMapNT(
                      reflNT[Task]                :+:
                      connectionIOToTask(metaRef) :+:
                      (loggedEval andThen tTask)))
      _          <- cacheCtx.start
    } yield Quasar(loggedEval, cacheCtx.shutdown)

  val toTask: Task[QErrs_CRW_TaskM ~> Task] =
    TaskRef(Tags.Min(none[VCache.Expiration])) ∘ (r =>
      foldMapNT(
        Read.fromTaskRef(r)  :+:
        Write.fromTaskRef(r) :+:
        reflNT[Task]         :+:
        QErrs.toCatchable[Task]))
}
