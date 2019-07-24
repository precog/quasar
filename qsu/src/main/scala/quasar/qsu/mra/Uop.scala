/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu.mra

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree, RenderedTree, Terminal}
import quasar.RenderTree.ops._

import scala.collection.immutable.SortedSet

import cats.{Foldable, Order, Show}
import cats.instances.sortedSet._
import cats.kernel.{BoundedSemilattice, CommutativeMonoid, CommutativeSemigroup, Semigroup}
import cats.syntax.foldable._
import cats.syntax.semigroup._
import cats.syntax.show._

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

/** A distinct union of products. */
final class Uop[A] private (val toSortedSet: SortedSet[A]) {
  import Uop.emptySet

  /** Alias for `and`. */
  def ∧ (that: Uop[A])(implicit asg: Semigroup[A], aord: Order[A]): Uop[A] =
    and(that)

  /** Alias for `or`. */
  def ∨ (that: Uop[A]): Uop[A] =
    or(that)

  /** The product of two `Uop`, distributing over the union. */
  def and(that: Uop[A])(implicit asg: Semigroup[A], aord: Order[A]): Uop[A] =
    if (isEmpty)
      that
    else if (that.isEmpty)
      this
    else
      new Uop(that.toSortedSet.foldLeft(emptySet[A]) { (as, thats) =>
        as ++ toSortedSet.map(_ |+| thats)
      })

  def isEmpty: Boolean =
    toSortedSet.isEmpty

  def map[B: Order](f: A => B): Uop[B] =
    new Uop(toSortedSet.foldLeft(Uop.emptySet[B])((bs, a) => bs + f(a)))

  /** The union of two `Uop`. */
  def or(that: Uop[A]): Uop[A] =
    if (isEmpty)
      that
    else if (that.isEmpty)
      this
    else
      new Uop(toSortedSet.union(that.toSortedSet))

  def toList: List[A] =
    toSortedSet.toList

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def toString: String = {
    implicit val renderA = RenderTree.make[A](a => Terminal(List(a.toString), None))
    this.show
  }
}

object Uop extends UopInstances {
  def empty[A: Order]: Uop[A] =
    new Uop(emptySet[A])

  def fromFoldable[F[_]: Foldable, A: Order](fa: F[A]): Uop[A] =
    new Uop(fa.foldLeft(emptySet[A])(_ + _))

  def of[A: Order](as: A*): Uop[A] =
    new Uop(emptySet[A] ++ as)

  def one[A: Order](a: A): Uop[A] =
    new Uop(emptySet[A] + a)

  ////

  private def emptySet[A](implicit A: Order[A]): SortedSet[A] =
    SortedSet.empty[A](A.toOrdering)
}

sealed abstract class UopInstances {
  implicit def conjCommutativeMonoid[A: CommutativeSemigroup: Order]: CommutativeMonoid[Uop[A] @@ Conjunction] =
    new CommutativeMonoid[Uop[A] @@ Conjunction] {
      val empty = Conjunction(Uop.empty[A])

      def combine(x: Uop[A] @@ Conjunction, y: Uop[A] @@ Conjunction) =
        Conjunction(x.unwrap ∧ y.unwrap)
    }

  implicit def disjBoundedSemilattice[A: Order]: BoundedSemilattice[Uop[A] @@ Disjunction] =
    new BoundedSemilattice[Uop[A] @@ Disjunction] {
      val empty = Disjunction(Uop.empty[A])

      def combine(x: Uop[A] @@ Disjunction, y: Uop[A] @@ Disjunction) =
        Disjunction(x.unwrap ∨ y.unwrap)
    }

  implicit def order[A: Order]: Order[Uop[A]] =
    Order.by(_.toSortedSet)

  implicit def renderTree[A: RenderTree]: RenderTree[Uop[A]] =
    RenderTree make { uop =>
      NonTerminal(List("Union"), None, uop.toList.map(_.render))
    }

  implicit def show[A: RenderTree]: Show[Uop[A]] =
    Show.show(uop => scalaz.Show[RenderedTree].shows(uop.render))
}
