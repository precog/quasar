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
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
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
final case class Quasar(interp: CoreEff ~> QErrs_TaskM, shutdown: Task[Unit]) {
  val taskInter: CoreEff ~> Task =
    Quasar.toTask compose interp

  def extendShutdown(step: Task[Unit]): Quasar =
    copy(shutdown = shutdown.attemptNonFatal.void >> step)
}

object Quasar {

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
      metastore <- MetaStore.connect(db, db.isInMemory, List(quasar.metastore.Schema.schema)).leftMap(_.message)
      metaRef   <- TaskRef(metastore).liftM[MainErrT]
      quasarFS  <- initWithMeta(loadConfig, metaRef, persist)
    } yield quasarFS.extendShutdown(metaRef.read.flatMap(_.shutdown))

  def initWithMeta(loadConfig: BackendConfig, metaRef: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit]): MainTask[Quasar] =
    for {
      fsThing  <- CompositeFileSystem.initWithMountsInMetaStore(loadConfig,metaRef)
      quasarFS <- initWithFS(fsThing, metaRef, persist).liftM[MainErrT]
    } yield quasarFS.extendShutdown(fsThing.shutdown)

  def initWithFS(fsThing: FS, metaRef: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit]): Task[Quasar] =
    for {
      finalEval  <- CoreEff.defaultImpl(fsThing, metaRef, persist)

      cacheCtx   <- Caching.viewCacheRefreshCtx(foldMapNT(
        reflNT[Task]       :+:
          connectionIOToTask(metaRef) :+:
          (finalEval andThen toTask)))

      _          <- cacheCtx.start
    } yield Quasar(finalEval, cacheCtx.shutdown)

  val toTask: QErrs_TaskM ~> Task =
    foldMapNT(reflNT[Task] :+: QErrs.toCatchable[Task])
}
