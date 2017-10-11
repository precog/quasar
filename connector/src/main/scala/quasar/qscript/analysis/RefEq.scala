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

package quasar.qscript.analysis

import slamdata.Predef._

import quasar.common.SortDir
import quasar.fp._
import quasar.qscript._

import matryoshka._
import matryoshka.data.free._

import scalaz._, Scalaz._
import scalaz.NonEmptyList._

trait RefEq[A] {
  def refEq(a1: A, a2: A): Boolean

  // this instance may not be lawful
  val equal: Equal[A] = Equal.equal(refEq)
}

object RefEq {
  import DeepShape._

  def refEq[A](a1: A, a2: A)(implicit A0: RefEq[A]): Boolean =
    A0.refEq(a1, a2)

  /* A version of equality that compares `UnknownShape`s as unequal.
   * It is used for computing deep shape equality in source merging.
   *
   * This equality instance is not reflexive and so it is not lawful.
   */
  implicit def refEqShapeMeta[T[_[_]]: BirecursiveT: EqualT]
      (implicit R: RefEq[ReduceFunc[FreeShape[T]]],
                S: RefEq[FreeShape[T]])
      : RefEq[ShapeMeta[T]] =
    new RefEq[ShapeMeta[T]] {
      def refEq(a1: ShapeMeta[T], a2: ShapeMeta[T]): Boolean =
        (a1, a2) match {
          case (RootShape(), RootShape()) => true
          case (UnknownShape(), UnknownShape()) => false // two unknown shapes compare as `false`
          case (Reducing(funcL), Reducing(funcR)) => R.refEq(funcL, funcR)
          case (Shifting(idL, structL), Shifting(idR, structR)) =>
            idL ≟ idR && S.refEq(structL, structR)
          case (_, _) => false
        }
      }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def refEqFreeShape[T[_[_]]: BirecursiveT: EqualT]: RefEq[FreeShape[T]] =
    new RefEq[FreeShape[T]] {
      def refEq(a1: FreeShape[T], a2: FreeShape[T]) =
        freeEqual[MapFunc[T, ?]].apply(refEqShapeMeta.equal).equal(a1, a2)
    }

  implicit def refEqSortDir: RefEq[SortDir] =
    new RefEq[SortDir] {
      def refEq(a1: SortDir, a2: SortDir): Boolean =
        Equal[SortDir].equal(a1, a2)
    }

  implicit def refEqList[A](implicit A0: RefEq[A]): RefEq[List[A]] =
    new RefEq[List[A]] {
      def refEq(a1: List[A], a2: List[A]): Boolean =
        listEqual[A](A0.equal).equal(a1, a2)
    }

  implicit def refEqNEL[A](implicit A0: RefEq[A]): RefEq[NonEmptyList[A]] =
    new RefEq[NonEmptyList[A]] {
      def refEq(a1: NonEmptyList[A], a2: NonEmptyList[A]): Boolean =
        nonEmptyListEqual[A](A0.equal).equal(a1, a2)
    }

  implicit def refEqTuple2[A, B]
    (implicit A0: RefEq[A], B0: RefEq[B]): RefEq[(A, B)] =
    new RefEq[(A, B)] {
      def refEq(a1: (A, B), a2: (A, B)): Boolean =
        tuple2Equal(A0.equal, B0.equal).equal(a1, a2)
    }

  implicit def refEqReduceFunc[A](implicit A0: RefEq[A]): RefEq[ReduceFunc[A]] =
    new RefEq[ReduceFunc[A]] {
      def refEq(a1: ReduceFunc[A], a2: ReduceFunc[A]): Boolean =
        ReduceFunc.equal.apply(A0.equal).equal(a1, a2)
    }
}
