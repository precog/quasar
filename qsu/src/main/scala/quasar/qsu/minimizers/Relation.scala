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

package quasar.qsu
package minimizers

import slamdata.Predef._

import scala.collection.mutable

import cats.Foldable
import cats.syntax.foldable._

private[minimizers] final case class Relation(index: Map[Symbol, Set[Symbol]]) {
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private[this] val memo = mutable.Map.empty[Symbol, Set[Symbol]]
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private[this] val computed = mutable.Set.empty[Symbol]

  /**
   * Produces the reflexive transitive closure of the directed
   * relation represented by `index`.
   */
  def closure(ref: Symbol): Set[Symbol] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def compute(start: Symbol, reach: Symbol): Unit =
      if (computed(reach)) {
        memo += (start -> (memo.getOrElse(start, Set()) ++ memo(reach)))
      } else {
        val updated = memo.getOrElse(start, Set()) + reach

        memo += (start -> updated)

        index.getOrElse(reach, Set())
          .filterNot(updated)
          .foreach(compute(start, _))
      }

    if (!computed(ref)) {
      compute(ref, ref)
      computed += ref
    }

    memo(ref)
  }

  def +(pair: (Symbol, Symbol)): Relation = {
    val (left, right) = pair

    val to = index.getOrElse(left, Set())
    val from = index.getOrElse(right, Set())

    Relation(index + (left -> (to + right)) + (right -> (from + left)))
  }

  def closures: Set[Set[Symbol]] = {
    @SuppressWarnings(Array(
      "org.wartremover.warts.Recursion",
      "org.wartremover.warts.TraversableOps"))
    def loop(remaining: Set[Symbol]): Set[Set[Symbol]] =
      if (remaining.isEmpty) {
        Set()
      } else {
        val sym = remaining.head    // axiom of choice ftw
        val cl = closure(sym)
        loop(remaining -- cl) + cl
      }

    loop(index.keys.toSet)
  }
}

private[minimizers] object Relation extends (Map[Symbol, Set[Symbol]] => Relation) {

  val Empty: Relation = Relation(Map())

  def allPairs[F[_]: Foldable, A](fa: F[A])(sym: A => Symbol)(p: (A, A) => Boolean): Relation = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(pairs: List[A], acc: Relation): Relation = pairs match {
      case hd :: tail =>
        val acc1 = tail.filter(p(hd, _)).foldLeft(acc)((acc, a) => acc + (sym(hd) -> sym(a)))
        loop(tail, acc1)

      case Nil => acc
    }

    loop(fa.toList, Empty)
  }
}
