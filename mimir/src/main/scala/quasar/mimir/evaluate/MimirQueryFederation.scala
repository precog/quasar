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
import quasar.common.data.Data
import quasar.contrib.cats.effect.liftio._
import quasar.evaluate.{FederatedQuery, QueryFederation}
import quasar.mimir._, MimirCake._
import quasar.qscript.MonadPlannerErr

import cats.effect.{IO, LiftIO}
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{Monad, WriterT}
import scalaz.std.list._
import scalaz.syntax.traverse._
import shims._

final class MimirQueryFederation[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: LiftIO: Monad: MonadPlannerErr] private (
    P: Cake)
    extends QueryFederation[T, F, QueryAssociate[T, IO], Stream[IO, Data]] {

  type FinalizersT[X[_], A] = WriterT[X, List[IO[Unit]], A]

  private val qscriptEvaluator =
    MimirQScriptEvaluator[T, WriterT[F, List[IO[Unit]], ?]](P)

  def evaluateFederated(q: FederatedQuery[T, QueryAssociate[T, IO]]): F[Stream[IO, Data]] = {
    val finalize: ((List[IO[Unit]], Stream[IO, Data])) => Stream[IO, Data] = {
      case (fs, s) => fs.foldLeft(s)(_ onFinalize _)
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
      F[_]: LiftIO: Monad: MonadPlannerErr](
      P: Cake)
      : QueryFederation[T, F, QueryAssociate[T, IO], Stream[IO, Data]] =
    new MimirQueryFederation[T, F](P)
}
