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
import quasar.fp._
import quasar.db.DbConnectionConfig
import quasar.effect.LiftedOps
import quasar.metastore.MetaStore

import scalaz._, Scalaz._
import scalaz.concurrent.Task

sealed abstract class MetaStoreLocation[A]

object MetaStoreLocation {

  final case object Get extends MetaStoreLocation[DbConnectionConfig]

  final case class Set(conn: DbConnectionConfig, initialize: Boolean)
    extends MetaStoreLocation[String \/ Unit]

  final class Ops[S[_] <: ACopK](implicit val ev: MetaStoreLocation :<: S) extends LiftedOps[MetaStoreLocation, S] {

    def get: Free[S, DbConnectionConfig] =
      lift(Get)

    def set(conn: DbConnectionConfig, initialize: Boolean): Free[S, String \/ Unit] =
      lift(Set(conn, initialize))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: MetaStoreLocation :<: S): Ops[S] =
      new Ops[S]
  }

  object impl {

    def default(ref: TaskRef[MetaStore], persist: DbConnectionConfig => MainTask[Unit]): MetaStoreLocation ~> Task =
      λ[MetaStoreLocation ~> Task] {
        case Get => ref.read.map(_.connectionInfo)
        case Set(conn, initialize) =>
          (for {
            currentSchemas <- ref.read.map(_.schemas).liftM[MainErrT]
            // Try connecting to the new metastore location
            m <- MetaStore.connect(conn, initialize, currentSchemas).leftMap(_.message)
            // Persist the change, if persisting fails, shutdown the new metastore connection and fail the change
            _ <- EitherT(persist(m.connectionInfo).foldM(persistFailure => m.shutdown.as(persistFailure.left), _ => ().right.point[Task]))
            // We successfully connected to the new metastore and persisted the change to the config file
            // so we shutdown the old one and
            _ <- ref.read.flatMap(_.shutdown.attempt).liftM[MainErrT]
            // change the value of the reference
            _ <- ref.write(m).liftM[MainErrT]
          } yield ()).run
      }

    def constant(config: DbConnectionConfig): MetaStoreLocation ~> Task =
      λ[MetaStoreLocation ~> Task] {
        case Get => config.point[Task]
        case Set(conn, initialize) => Task.fail(new Exception("This implementation does not allow changing MetaStore"))
      }
  }
}
