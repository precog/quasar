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

import quasar.qscript._

import matryoshka.BirecursiveT
import scalaz.Functor

sealed trait Unirewrite[T[_[_]], G[_]] {
  def apply(r: Rewrite[T]): T[QScriptEducated[T, ?]] => T[G]
}

object Unirewrite {

  def apply[T[_[_]], G[_]](
      rew: Rewrite[T])(
      implicit U: Unirewrite[T, G]) =
    U.apply(rew)

  implicit def unirewrite[T[_[_]]: BirecursiveT, G[_]: Functor](
    implicit
      J: ThetaToEquiJoin.Aux[T, QScriptEducated[T, ?], G],
      C: Coalesce.Aux[T, QScriptEducated[T, ?], QScriptEducated[T, ?]],
      N: Normalizable[QScriptEducated[T, ?]])
      : Unirewrite[T, G] =
    new Unirewrite[T, G] {
      def apply(r: Rewrite[T]): T[QScriptEducated[T, ?]] => T[G] =
        r.normalize[G]
  }
}
