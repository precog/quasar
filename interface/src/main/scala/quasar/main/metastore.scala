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
import quasar.console.stdout
import quasar.contrib.scalaz.catchable._
import quasar.db, db.{DbConnectionConfig, StatefulTransactor}
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._, FileSystemDef.DefinitionResult
import quasar.metastore._

import argonaut._, Argonaut._
import doobie.imports.{ConnectionIO, HC, Transactor}
import doobie.syntax.connectionio._
import eu.timepit.refined.auto._
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

object metastore {
  type QErrsCnxIO[A]  = Coproduct[ConnectionIO, QErrs, A]
  type QErrsTCnxIO[A] = Coproduct[Task, QErrsCnxIO, A]

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

  private val metastorePrompt: String =
    "Is the metastore database running?"

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

  final case class MetaStoreCtx(
    metastore: StatefulTransactor,
    interp: CoreEff ~> QErrsTCnxIOM,
    closeMnts: Task[Unit])

  val taskToConnectionIO: Task ~> ConnectionIO =
    λ[Task ~> ConnectionIO](t => HC.delay(t.unsafePerformSync))

  val absorbTask: QErrsTCnxIO ~> QErrsCnxIO =
    Inject[ConnectionIO, QErrsCnxIO].compose(taskToConnectionIO) :+: reflNT[QErrsCnxIO]

  def metastoreTransactor(mtaCfg: MetaStoreConfig): MainTask[StatefulTransactor] = {
    val connInfo = DbConnectionConfig.connectionInfo(mtaCfg.database)

    val statefulTransactor =
      stdout(s"Using metastore: ${connInfo.url}") *>
      db.poolingTransactor(connInfo, db.DefaultConfig)

    EitherT(statefulTransactor.attempt)
      .leftMap(t => {t.printStackTrace ; s"While initializing the MetaStore: ${t.getMessage}"})
  }

  def jdbcMounter[S[_]](
    hfsRef: TaskRef[AnalyticalFileSystem~> HierarchicalFsEffM],
    mntdRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
  )(implicit
    S0: ConnectionIO :<: S,
    S1: PhysErr :<: S
  ): Mounting ~> Free[S, ?] = {
    type M[A] = Free[MountEff, A]
    type G[A] = Coproduct[ConnectionIO, M, A]
    type T[A] = Coproduct[Task, S, A]

    val t: T ~> S =
      S0.compose(taskToConnectionIO) :+: reflNT[S]

    val g: G ~> Free[S, ?] =
      injectFT[ConnectionIO, S] :+:
      foldMapNT(mapSNT(t) compose MountEff.interpreter[T](hfsRef, mntdRef))

    val mounter = MetaStoreMounter[M, G](
      mountHandler.mount[MountEff](_),
      mountHandler.unmount[MountEff](_))

    foldMapNT(g) compose mounter
  }

  private def closeFileSystem(dr: DefinitionResult[PhysFsEffM]): Task[Unit] = {
    val transform: PhysFsEffM ~> Task =
      foldMapNT(reflNT[Task] :+: (Failure.toCatchable[Task, Exception] compose Failure.mapError[PhysicalError, Exception](_.cause)))

    dr.translate(transform).close
  }

  private def closeAllMounts(mnts: Mounts[DefinitionResult[PhysFsEffM]]): Task[Unit] =
    mnts.traverse_(closeFileSystem(_).attemptNonFatal.void)

  def verifySchema[A](schema: db.Schema[A], transactor: Transactor[Task]): MainTask[Unit] = {
    val verifyMS = Hoist[MainErrT].hoist(transactor.trans)
      .apply(verifyMetaStoreSchema(schema))
    EitherT(verifyMS.run.attempt.map(_.valueOr(t =>
      s"While verifying MetaStore schema: ${t.getMessage}. $metastorePrompt".left)))
  }

  def initUpdateMetaStore[A](
    schema: db.Schema[A], transactor: Transactor[Task], jCfg: Option[Json]
  ): MainTask[Option[Json]] = {
    val mntsFieldName = "mountings"

    val mountingsConfigDecodeJson: DecodeJson[Option[MountingsConfig]] =
      DecodeJson(cur => (cur --\ mntsFieldName).as[Option[MountingsConfig]])

    val mountingsConfig: ConnectionIO[Option[MountingsConfig]] =
      taskToConnectionIO(jCfg.traverse(
        mountingsConfigDecodeJson.decodeJson(_).fold({
          case (e, _) => Task.fail(new RuntimeException(
            s"malformed config, fix before attempting initUpdateMetaStore, ${e.shows}"))},
          Task.now)
        ) ∘ (_.unite))

    def migrateMounts(cfg: Option[MountingsConfig]): ConnectionIO[Unit] =
      cfg.traverse_(_.toMap.toList.traverse_((MetaStoreAccess.insertMount _).tupled))

    val op: ConnectionIO[Option[Json]] =
      for {
        cfg <- mountingsConfig
        ver <- schema.readVersion
        _   <- schema.updateToLatest
        _   <- ver.isEmpty.whenM(migrateMounts(cfg))
      } yield ver.isEmpty.fold(
        jCfg ∘ (_.withObject(_ - mntsFieldName)),
        jCfg)

    EitherT(transactor.trans(op).attempt ∘ (
      _.leftMap(t => s"While initializing and updating MetaStore: ${t.getMessage}. $metastorePrompt")))
  }

  def metastoreCtx[A](metastore: StatefulTransactor): MainTask[MetaStoreCtx] = {
    for {
      hfsRef       <- TaskRef(Empty.analyticalFileSystem[HierarchicalFsEffM]).liftM[MainErrT]
      mntdRef      <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]).liftM[MainErrT]

      ephmralMnt   =  KvsMounter.interpreter[Task, QErrsTCnxIO](
                        KvsMounter.ephemeralMountConfigs[Task], hfsRef, mntdRef) andThen
                      mapSNT(absorbTask)                                         andThen
                      QErrsCnxIO.toMainTask(metastore.transactor)

      mountsCfg    <- MetaStoreAccess.fsMounts
                        .map(MountingsConfig(_))
                        .transact(metastore.transactor)
                        .liftM[MainErrT]

      // TODO: Still need to expose these in the HTTP API, see SD-1131
      failedMnts   <- attemptMountAll[Mounting](mountsCfg) foldMap ephmralMnt
      _            <- failedMnts.toList.traverse_(logFailedMount).liftM[MainErrT]

      runCore      <- CoreEff.runFs[QEffIO](hfsRef).liftM[MainErrT]
    } yield {
      val f: QEffIO ~> QErrsTCnxIOM =
        injectFT[Task, QErrsTCnxIO]               :+:
        jdbcMounter[QErrsTCnxIO](hfsRef, mntdRef) :+:
        injectFT[QErrs, QErrsTCnxIO]

      MetaStoreCtx(
        metastore,
        foldMapNT(f) compose runCore,
        mntdRef.read >>= closeAllMounts _)
    }
  }
}
