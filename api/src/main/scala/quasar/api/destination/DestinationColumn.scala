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

import slamdata.Predef._

import cats.{Applicative, Eq, Eval, Show, Traverse}
import cats.implicits._

/**
 * Analogous to TableColumn, except parametric. In theory, we could replace
 * TableColumn with DestinationColumn[ColumnType.Scalar]
 */
final case class DestinationColumn[T](name: String, tpe: T)

object DestinationColumn {

  implicit val traverse: Traverse[DestinationColumn] = new Traverse[DestinationColumn] {

    def foldLeft[A, B](fa: DestinationColumn[A], b: B)(f: (B, A) => B): B =
      f(b, fa.tpe)

    def foldRight[A, B](fa: DestinationColumn[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      f(fa.tpe, lb)

    override def map[A, B](fa: DestinationColumn[A])(f: A => B): DestinationColumn[B] =
      fa.copy(tpe = f(fa.tpe))

    def traverse[G[_]: Applicative, A, B](fa: DestinationColumn[A])(f: A => G[B]): G[DestinationColumn[B]] =
      f(fa.tpe).map(b => fa.copy(tpe = b))
  }

  implicit def equalTableColumn[T: Eq]: Eq[DestinationColumn[T]] =
    Eq.by(c => (c.name, c.tpe))

  implicit def showTableColumn[T: Show]: Show[DestinationColumn[T]] =
    Show show { tc =>
      "DestinationColumn(" + tc.name + ", " + tc.tpe.show + ")"
    }
}
