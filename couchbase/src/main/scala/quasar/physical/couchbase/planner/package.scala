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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.NameGenerator
import quasar.Planner.InternalError
import quasar.Planner.PlannerErrorME

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

package object planner {
  import N1QL.{Id, Select}, Select.{Value, ResultExpr}

  def genId[T, F[_]: Functor: NameGenerator]: F[Id[T]] =
    NameGenerator[F].prefixedName("_") ∘ (Id(_))

  def selectOrElse[T]
    (a: T, whenSelect: T, otherwise: T)
    (implicit T: Recursive.Aux[T, N1QL])
      : T =
    a.project match {
      case Select(_, _, _, _, _, _, _, _, _) => whenSelect
      case _                                 => otherwise
    }

  def wrapSelect[T[_[_]]: BirecursiveT](a: T[N1QL]): T[N1QL] =
    selectOrElse[T[N1QL]](
      a, a,
      Select(
        Value(true), ResultExpr(a, none).wrapNel, keyspace = none, join = none,
        unnest = none, let = nil,  filter = none, groupBy = none, orderBy = Nil).embed)

  def unexpected[F[_]: PlannerErrorME, A](name: String): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unexpected $name"))

  def unimplemented[F[_]: PlannerErrorME, A](name: String): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unimplemented $name"))
}
