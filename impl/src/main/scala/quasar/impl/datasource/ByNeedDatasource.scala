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

package quasar.impl.datasource

import slamdata.Predef._
import quasar.Disposable
import quasar.api.QueryEvaluator
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource

import cats.effect.Async
import cats.effect.concurrent.MVar
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import qdata.QDataEncode

import ByNeedDatasource.NeedState

final class ByNeedDatasource[F[_], G[_], Q] private (
    datasourceType: DatasourceType,
    mvar: MVar[F, NeedState[F, Disposable[F, Datasource[F, G, Q]]]])(
    implicit F: Async[F])
    extends Datasource[F, G, Q] {

  def evaluator[R: QDataEncode]: QueryEvaluator[F, Q, G[R]] =
    new QueryEvaluator[F, Q, G[R]] {
      def evaluate(query: Q): F[G[R]] =
        getDatasource.flatMap(_.evaluator.evaluate(query))
    }

  val kind: DatasourceType = datasourceType

  def pathIsResource(path: ResourcePath): F[Boolean] =
    getDatasource.flatMap(_.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]] =
    getDatasource.flatMap(_.prefixedChildPaths(path))

  ////

  private def getDatasource: F[Datasource[F, G, Q]] =
    for {
      needState <- mvar.read

      ds <- needState match {
        case NeedState.Uninitialized(init) =>
          initAndGet

        case NeedState.Initialized(ds) =>
          ds.unsafeValue.pure[F]
      }
    } yield ds

  private def initAndGet: F[Datasource[F, G, Q]] =
    mvar.tryTake.flatMap {
      case Some(s @ NeedState.Uninitialized(init)) =>
        val doInit = for {
          ds <- init
          _  <- mvar.put(NeedState.Initialized(ds))
        } yield ds.unsafeValue

        F.handleErrorWith(doInit) { e =>
          mvar.put(s) *> F.raiseError(e)
        }

      case Some(s @ NeedState.Initialized(ds)) =>
        mvar.put(s).as(ds.unsafeValue)

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

  def apply[F[_]: Async, G[_], Q](
      kind: DatasourceType,
      init: F[Disposable[F, Datasource[F, G, Q]]])
      : F[Disposable[F, Datasource[F, G, Q]]] = {

    def dispose(m: MVar[F, NeedState[F, Disposable[F, Datasource[F, G, Q]]]]): F[Unit] =
      m.take flatMap {
        case NeedState.Initialized(ds) => ds.dispose
        case _ => ().pure[F]
      }

    MVar.uncancelableOf[F, NeedState[F, Disposable[F, Datasource[F, G, Q]]]](
      NeedState.Uninitialized(init))
      .map(mv => Disposable(new ByNeedDatasource(kind, mv), dispose(mv)))
  }
}
