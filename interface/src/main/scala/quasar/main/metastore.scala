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
import quasar.console.stdout
import quasar.db, db.DbConnectionConfig
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._, BackendDef.DefinitionResult
import quasar.metastore._

import argonaut._, Argonaut._
import doobie.imports._
import eu.timepit.refined.auto._
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

object metastore {

  val taskToConnectionIO: Task ~> ConnectionIO =
    λ[Task ~> ConnectionIO](t => HC.delay(t.unsafePerformSync))

  def metastoreTransactor(dbCfg: DbConnectionConfig): MainTask[MetaStore] = {
    val connInfo = DbConnectionConfig.connectionInfo(dbCfg)

    val statefulTransactor =
      stdout(s"Using metastore: ${connInfo.url}") *>
      MetaStore.connect(dbCfg)

    EitherT(statefulTransactor)
      .leftMap(t => {t.printStackTrace ; s"While initializing the MetaStore: ${t.getMessage}"})
  }

  def jdbcMounter[S[_]](
    hfsRef: TaskRef[BackendEffect~> HierarchicalFsEffM],
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

  def verifySchema[A: Show](schema: db.Schema[A], transactor: Transactor[Task]): EitherT[Task, MetastoreInitializationFailure, Unit] = {
    EitherT(verifyMetaStoreSchema(schema).run.transact(transactor).attempt.map(_.valueOr(t =>
      UnknownInitializationError(t).left)))
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
      _.leftMap(t => UnknownInitializationError(t).message)))
  }
}
