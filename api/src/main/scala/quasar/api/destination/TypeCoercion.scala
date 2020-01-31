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

package quasar.api.destination

import cats.{Applicative, Eq, Eval, Show, Traverse}
import cats.data.NonEmptyList
import cats.implicits._

import quasar.api.table.ColumnType

import scala.{List, Option, Product, Serializable, StringContext}

sealed trait TypeCoercion[+A] extends Product with Serializable

object TypeCoercion {
  final case class Unsatisfied[A](
      alternatives: List[ColumnType.Scalar],
      top: Option[A])
      extends TypeCoercion[A]

  final case class Satisfied[A](priority: NonEmptyList[A])
      extends TypeCoercion[A]

  implicit def equal[A: Eq]: Eq[TypeCoercion[A]] =
    Eq instance {
      case (Unsatisfied(a1, t1), Unsatisfied(a2, t2)) =>
        a1 === a2 && t1 === t2

      case (Satisfied(s1), Satisfied(s2)) =>
        s1 === s2

      case _ =>
        false
    }

  implicit def show[A: Show]: Show[TypeCoercion[A]] =
    Show show {
      case Unsatisfied(alt, top) =>
        s"Unsatisfied(${alt.show}, ${top.show})"

      case Satisfied(priority) =>
        s"Satisfied(${priority.show})"
    }

  implicit val traverse: Traverse[TypeCoercion] =
    new Traverse[TypeCoercion] {
      def traverse[G[_]: Applicative, A, B](fa: TypeCoercion[A])(f: A => G[B]): G[TypeCoercion[B]] =
        fa match {
          case Unsatisfied(alts, top) =>
            top.traverse(f).map(Unsatisfied(alts, _))

          case Satisfied(prio) =>
            prio.traverse(f).map(Satisfied(_))
        }

      def foldLeft[A, B](fa: TypeCoercion[A], b: B)(f: (B, A) => B): B =
        fa match {
          case Unsatisfied(_, top) => top.foldLeft(b)(f)
          case Satisfied(prio) => prio.foldLeft(b)(f)
        }

      def foldRight[A, B](fa: TypeCoercion[A], b: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa match {
          case Unsatisfied(_, top) => top.foldr(b)(f)
          case Satisfied(prio) => prio.foldr(b)(f)
        }
    }
}
