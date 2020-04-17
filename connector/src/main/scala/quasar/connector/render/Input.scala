/*
 * Copyright 2020 Precog Data
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

package quasar.connector.render

import slamdata.Predef.{Eq => _, _}

import cats._
import cats.implicits._

sealed trait Input[+A] {
  def value: A
}

object Input {
  final case class Initial[A](value: A) extends Input[A]
  final case class Incremental[A](value: A) extends Input[A]

  implicit def inputEq[A: Eq]: Eq[Input[A]] =
    new Eq[Input[A]] {
      def eqv(x: Input[A], y: Input[A]): Boolean =
        (x, y) match {
          case (Initial(vx), Initial(vy)) => vx === vy
          case (Incremental(vx), Incremental(vy)) => vx === vy
          case (_, _) => false
        }
    }

  implicit def inputShow[A: Show]: Show[Input[A]] =
    Show show {
      case Initial(value) => s"Initial(${value.show})"
      case Incremental(value) => s"Incremental(${value.show})"
    }
}
