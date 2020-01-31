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

package quasar.qsu.mra

import slamdata.Predef.{Boolean, Int, List}

import quasar.contrib.cats.data.nonEmptySet._

import scala.collection.immutable.SortedSet

import cats.{Order, Show}
import cats.data.NonEmptySet
import cats.kernel.{BoundedSemilattice, CommutativeMonoid}
import cats.syntax.functor._
import cats.syntax.order._
import cats.syntax.show._

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

/** A sum of products of join keys. */
final class JoinKeys[S, V] private (protected val uop: Uop[NonEmptySet[JoinKey[S, V]]]) {
  def ∧ (that: JoinKeys[S, V])(implicit sord: Order[S], vord: Order[V]): JoinKeys[S, V] =
    and(that)

  def ∨ (that: JoinKeys[S, V]): JoinKeys[S, V] =
    or(that)

  def and(that: JoinKeys[S, V])(implicit sord: Order[S], vord: Order[V]): JoinKeys[S, V] =
    new JoinKeys(uop ∧ that.uop)

  def isEmpty: Boolean =
    uop.isEmpty

  def mapKeys[T: Order, W: Order](f: JoinKey[S, V] => JoinKey[T, W]): JoinKeys[T, W] =
    new JoinKeys(uop.map(_.map(f)))

  def or(that: JoinKeys[S, V]): JoinKeys[S, V] =
    new JoinKeys(uop ∨ that.uop)

  def toList: List[NonEmptySet[JoinKey[S, V]]] =
    uop.toList

  def toSortedSet: SortedSet[NonEmptySet[JoinKey[S, V]]] =
    uop.toSortedSet

  def compare(that: JoinKeys[S, V])(implicit sord: Order[S], vord: Order[V]): Int =
    uop.compare(that.uop)
}

object JoinKeys extends JoinKeysInstances {
  def conj[S: Order, V: Order](k: JoinKey[S, V], ks: JoinKey[S, V]*): JoinKeys[S, V] =
    new JoinKeys(Uop.one(NonEmptySet.of(k, ks: _*)))

  def empty[S: Order, V: Order]: JoinKeys[S, V] =
    new JoinKeys(Uop.empty)

  def one[S: Order, V: Order](k: JoinKey[S, V]): JoinKeys[S, V] =
    conj(k)
}

sealed abstract class JoinKeysInstances {

  implicit def conjCommutativeMonoid[S: Order, V: Order]: CommutativeMonoid[JoinKeys[S, V] @@ Conjunction] =
    new CommutativeMonoid[JoinKeys[S, V] @@ Conjunction] {
      val empty = Conjunction(JoinKeys.empty[S, V])

      def combine(x: JoinKeys[S, V] @@ Conjunction, y: JoinKeys[S, V] @@ Conjunction) =
        Conjunction(x.unwrap ∧ y.unwrap)
    }

  implicit def disjBoundedSemilattice[S: Order, V: Order]: BoundedSemilattice[JoinKeys[S, V] @@ Disjunction] =
    new BoundedSemilattice[JoinKeys[S, V] @@ Disjunction] {
      val empty = Disjunction(JoinKeys.empty[S, V])

      def combine(x: JoinKeys[S, V] @@ Disjunction, y: JoinKeys[S, V] @@ Disjunction) =
        Disjunction(x.unwrap ∨ y.unwrap)
    }

  implicit def order[S: Order, V: Order]: Order[JoinKeys[S, V]] =
    Order.from(_ compare _)

  implicit def show[S: Show, V: Show]: Show[JoinKeys[S, V]] =
    Show.show(jks => "JoinKeys" + jks.toSortedSet.toIterator.map(_.show).mkString("(", ", ", ")"))
}
