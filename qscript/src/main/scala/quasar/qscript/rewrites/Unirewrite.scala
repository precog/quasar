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

package quasar.qscript.rewrites

import quasar.contrib.iota._
import quasar.qscript._

import iotaz.{CopK, TListK}
import matryoshka.BirecursiveT
import scalaz.{Functor, Monad, Traverse}
import scalaz.syntax.applicative._

sealed trait Unirewrite[T[_[_]], L <: TListK] {
  def apply[F[_]: Monad: MonadPlannerErr](r: Rewrite[T])
      : T[QScriptRead[T, ?]] => F[T[CopK[L, ?]]]
}

object Unirewrite {

  def apply[T[_[_]], L <: TListK, F[_]: Monad: MonadPlannerErr](
      rew: Rewrite[T])(
      implicit U: Unirewrite[T, L]) =
    U.apply[F](rew)

  implicit def fileRead[T[_[_]]: BirecursiveT, L <: TListK](
    implicit
      FC: Functor[CopK[L, ?]],
      TC0: Traverse[CopK[L, ?]],
      J: SimplifyJoin.Aux[T, QScriptRead[T, ?], CopK[L, ?]],
      C: Coalesce.Aux[T, QScriptRead[T, ?], QScriptRead[T, ?]],
      N: Normalizable[QScriptRead[T, ?]])
      : Unirewrite[T, L] =
    new Unirewrite[T, L] {
      def apply[F[_]: Monad: MonadPlannerErr](r: Rewrite[T])
          : T[QScriptRead[T, ?]] => F[T[CopK[L, ?]]] = { qs =>
        r.simplifyJoinOnNorm[QScriptRead[T, ?], CopK[L, ?]]
          .apply(qs).point[F]
      }
  }
}
