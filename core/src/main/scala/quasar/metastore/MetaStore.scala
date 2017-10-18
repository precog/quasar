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

package quasar.metastore

import slamdata.Predef._
import quasar.console.stdout
import quasar.db._
import quasar.fs.mount.MountingsConfig

import argonaut._
import doobie.util.transactor.Transactor
import doobie.free.connection.ConnectionIO
import doobie.syntax.connectionio._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

final case class MetaStore private (connectionInfo: DbConnectionConfig, trans: StatefulTransactor, schemas: List[Schema[Int]]) {
  def shutdown: Task[Unit] = trans.shutdown
  def transactor: Transactor[Task] = trans.transactor
}

object MetaStore {
  /**
    * Attempts to connect to a Quasar MetaStore. Will make sure the schema matches the expected Schema and print
    * a line to the console to inform the user that we just connected to a given MetaStore location once the
    * connection is established.
    * @param dbConfig The configuration specifying the location of the database backing the MetaStore
    * @param initializeOrUpdate Whether or not to immediately initialize or update the MetaStore upon connecting
    *                           to the underlying database. If the database has not been initialized and this value
    *                           is set to false, this call will fail to connect to the database with a
    *                           `MetastoreFailure` detailing the reason for the failure (requires initialization or an update).
    * @param schemas The Schemas that the underlying database is expected to have. You probably want to pass in
    *               `quasar.metastore.Schema.schema`, but additional Schemas may be desirable if the application
    *               needs to store additional information in the MetaStore
    * @return A `MetaStore` object containing the `transactor` to use to perform operation on the MetaStore as
    *         well as it's location and expected Schema
    */
  def connect(dbConfig: DbConnectionConfig, initializeOrUpdate: Boolean, schemas: List[Schema[Int]]): EitherT[Task, MetastoreFailure, MetaStore] =
    for {
      tx <- poolingTransactor(DbConnectionConfig.connectionInfo(dbConfig), DefaultConfig).leftMap(f => f:MetastoreFailure)
      _  <- onFailOrLeft(initializeOrUpdate.whenM(schemas.traverse(this.initializeOrUpdate(_, tx.transactor, None))) >>
            schemas.traverse(verifySchema(_, tx.transactor)) >>
            stdout(s"Using metastore: ${DbConnectionConfig.connectionInfo(dbConfig).url}").liftM[EitherT[?[_], MetastoreFailure, ?]])(tx.shutdown)
    } yield MetaStore(dbConfig, tx, schemas)

  private def onFailOrLeft[E, A](t: EitherT[Task, E, A])(f: Task[Unit]):EitherT[Task, E, A] =
    EitherT(t.run.onFinish {
      case Some(_) => f
      case None    => Task.now(())
    }.flatMap {
      case -\/(e) => f >> -\/(e).point[Task]
      case right  => right.point[Task]
    })

  def initializeOrUpdate[A](
    schema: Schema[A], transactor: Transactor[Task], jCfg: Option[Json]
  ): EitherT[Task, MetastoreFailure, Option[Json]] = {
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

    EitherT(transactor.trans.apply(op).attempt ∘ (
      _.leftMap(t => UnknownError(t, "While initializing or updating"))))
  }

  def verifySchema[A: Show](schema: Schema[A], transactor: Transactor[Task]): EitherT[Task, MetastoreFailure, Unit] = {
    EitherT(verifyMetaStoreSchema(schema).run.transact(transactor).attempt.map(_.valueOr(t =>
      UnknownError(t, "while verifying schema").left)))
  }

}