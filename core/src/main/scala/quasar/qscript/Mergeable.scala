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

package quasar.qscript

import quasar.Predef._
import quasar.namegen._

import simulacrum.typeclass
import scalaz._

@typeclass trait Mergeable[A] {
  type IT[F[_]]

  def mergeSrcs(fm1: FreeMap[IT], fm2: FreeMap[IT], a1: A, a2: A):
      OptionT[State[NameGen, ?], SrcMerge[A, FreeMap[IT]]]
}

object Mergeable {
  type Aux[T[_[_]], A] = Mergeable[A] { type IT[F[_]] = T[F] }

  implicit def const[T[_[_]], A](implicit ma: Mergeable.Aux[T, A]):
      Mergeable.Aux[T, Const[A, Unit]] =
    new Mergeable[Const[A, Unit]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[T],
        right: FreeMap[T],
        p1: Const[A, Unit],
        p2: Const[A, Unit]) =
        ma.mergeSrcs(left, right, p1.getConst, p2.getConst).map {
          case SrcMerge(src, l, r) => SrcMerge(Const(src), l, r)
        }
    }

  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit mf: Mergeable.Aux[T, F[Unit]],
             mg: Mergeable.Aux[T, G[Unit]]):
      Mergeable.Aux[T, Coproduct[F, G, Unit]] =
    new Mergeable[Coproduct[F, G, Unit]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        cp1: Coproduct[F, G, Unit],
        cp2: Coproduct[F, G, Unit]) = {
        (cp1.run, cp2.run) match {
          case (-\/(left1), -\/(left2)) =>
            mf.mergeSrcs(left, right, left1, left2).map {
              case SrcMerge(src, left, right) => SrcMerge(Coproduct(-\/(src)), left, right)
            }
          case (\/-(right1), \/-(right2)) =>
            mg.mergeSrcs(left, right, right1, right2).map {
              case SrcMerge(src, left, right) => SrcMerge(Coproduct(\/-(src)), left, right)
            }
          case (_, _) => OptionT.none
        }
      }
    }


}
