/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.contrib.matryoshka

import quasar.Predef._

import matryoshka._, Recursive.ops._
import scalaz._
import simulacrum.typeclass

@typeclass trait EqualT[T[_[_]]] {
  def equal[F[_]: Functor](tf1: T[F], tf2: T[F])(implicit del: Delay[Equal, F])
      : Boolean

  def equalT[F[_]: Functor](implicit del: Delay[Equal, F]): Equal[T[F]] =
    Equal.equal[T[F]](equal[F](_, _))
}

object EqualT {
  def recursive[T[_[_]]: Recursive]: EqualT[T] = new EqualT[T] {
    def equal[F[_]: Functor]
      (tf1: T[F], tf2: T[F])
      (implicit del: Delay[Equal, F]) =
      del(equalT[F]).equal(tf1.project, tf2.project)
    }

  implicit val fix: EqualT[Fix] = recursive
  implicit val mu:  EqualT[Mu]  = recursive
  implicit val nu:  EqualT[Mu]  = recursive
}
