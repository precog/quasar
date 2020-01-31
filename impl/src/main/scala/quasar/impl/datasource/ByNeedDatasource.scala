/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasource

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource

import cats.effect.{Async, Resource}
import cats.effect.concurrent.MVar
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import ByNeedDatasource.NeedState

final class ByNeedDatasource[F[_], G[_], Q, R, P <: ResourcePathType] private (
    datasourceType: DatasourceType,
    mvar: MVar[F, NeedState[F, (Datasource[F, G, Q, R, P], F[Unit])]])(
    implicit F: Async[F])
    extends Datasource[F, G, Q, R, P] {

  val kind: DatasourceType = datasourceType

  def evaluate(query: Q): F[R] =
    getDatasource.flatMap(_.evaluate(query))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    getDatasource.flatMap(_.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    getDatasource.flatMap(_.prefixedChildPaths(path))

  ////

  private def getDatasource: F[Datasource[F, G, Q, R, P]] =
    for {
      needState <- mvar.read

      ds <- needState match {
        case NeedState.Uninitialized(init) =>
          initAndGet

        case NeedState.Initialized((ds, _)) =>
          ds.pure[F]
      }
    } yield ds

  private def initAndGet: F[Datasource[F, G, Q, R, P]] =
    mvar.tryTake flatMap {
      case Some(s @ NeedState.Uninitialized(init)) =>
        val doInit = for {
          ds <- init
          _  <- mvar.put(NeedState.Initialized(ds))
        } yield ds._1

        F.handleErrorWith(doInit) { e =>
          mvar.put(s) *> F.raiseError(e)
        }

      case Some(s @ NeedState.Initialized((ds, _))) =>
        mvar.put(s).as(ds)

      case None =>
        getDatasource
    }
}

object ByNeedDatasource {
  sealed trait NeedState[F[_], A]

  object NeedState {
    final case class Uninitialized[F[_], A](init: F[A]) extends NeedState[F, A]
    final case class Initialized[F[_], A](a: A) extends NeedState[F, A]
  }

  def apply[F[_]: Async, G[_], Q, R, P <: ResourcePathType](
      kind: DatasourceType,
      init: Resource[F, Datasource[F, G, Q, R, P]])
      : Resource[F, Datasource[F, G, Q, R, P]] = {

    def dispose(m: MVar[F, NeedState[F, (Datasource[F, G, Q, R, P], F[Unit])]]): F[Unit] =
      m.take flatMap {
        case NeedState.Initialized((_, dispose)) => dispose
        case _ => ().pure[F]
      }

    val mvar =
      MVar.uncancelableOf[F, NeedState[F, (Datasource[F, G, Q, R, P], F[Unit])]](
        NeedState.Uninitialized(init.allocated))

    Resource(mvar.map(mv => (new ByNeedDatasource(kind, mv), dispose(mv))))
  }
}
