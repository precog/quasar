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

package quasar.mimir.evaluate

import quasar.RenderTreeT
import quasar.common.PhaseResultTell
import quasar.contrib.cats.effect.liftio._
import quasar.impl.evaluate.{FederatedQuery, QueryFederation}
import quasar.mimir._, MimirCake._
import quasar.qscript.MonadPlannerErr

import cats.effect.{IO, LiftIO}
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{Monad, WriterT}
import scalaz.std.list._
import scalaz.syntax.traverse._
import shims._

import scala.concurrent.ExecutionContext

final class MimirQueryFederation[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: LiftIO: Monad: MonadPlannerErr: PhaseResultTell] private (
    P: Cake, optimizeQScript: Boolean)(
    implicit ec: ExecutionContext)
    extends QueryFederation[T, F, QueryAssociate[T, IO], Stream[IO, MimirRepr]] {

  type FinalizersT[X[_], A] = WriterT[X, List[IO[Unit]], A]

  private val qscriptEvaluator =
    MimirQScriptEvaluator[T, FinalizersT[F, ?]](P, optimizeQScript)

  def evaluateFederated(q: FederatedQuery[T, QueryAssociate[T, IO]]): F[Stream[IO, MimirRepr]] = {
    val finalize: ((List[IO[Unit]], MimirRepr)) => Stream[IO, MimirRepr] = {
      case (fs, s) => fs.foldLeft(Stream(s).covary[IO])(_ onFinalize _)
    }

    qscriptEvaluator
      .evaluate(q.query)
      .run(q.sources)
      .run
      .map(finalize)
  }
}

object MimirQueryFederation {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: LiftIO: Monad: MonadPlannerErr: PhaseResultTell](
      P: Cake, optimizeQScript: Boolean)(
      implicit ec: ExecutionContext)
      : QueryFederation[T, F, QueryAssociate[T, IO], Stream[IO, MimirRepr]] =
    new MimirQueryFederation[T, F](P, optimizeQScript)
}
