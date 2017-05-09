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

package quasar.yggdrasil
package table

import quasar.blueeyes._
import quasar.precog.common._
import quasar.yggdrasil.util._
import scala.collection.mutable
import scalaz._, Scalaz._

/**
  * Represents a way to traverse a list of CPaths in sorted order. This takes
  * into account mixes of homogeneous and heterogeneous arrays.
  */
sealed trait CPathTraversal { self =>
  import MaybeOrdering._
  import CPathTraversal._

  /**
    * Creates an order on rows in a set of columns. You can also optionally use
    * 2 column sets for the 1st and 2nd paramters of the SpireOrder. This order will
    * not allocate any objects or arrays, but it is also not threadsafe.
    */
  def rowOrder(cpaths: List[CPath], left: Map[CPath, Set[Column]], optRight: Option[Map[CPath, Set[Column]]] = None): SpireOrder[Int] = {
    val right = optRight getOrElse left

    def plan0(t: CPathTraversal, paths: List[(List[CPathNode], List[CPathNode])], idx: Int): CPathComparator = t match {
      case Done =>
        val validPaths = paths map { case (_, nodes) => CPath(nodes.reverse) }

        def makeCols(pathToCol: Map[CPath, Set[Column]]): Array[(CPath, Column)] = {
          validPaths.flatMap({ path =>
            pathToCol.getOrElse(path, Set.empty).toList map ((path, _))
          })(collection.breakOut)
        }

        val lCols = makeCols(left)
        val rCols = makeCols(right)

        val comparators: Array[CPathComparator] = (for ((lPath, lCol) <- lCols; (rPath, rCol) <- rCols) yield {
          CPathComparator(lPath, lCol, rPath, rCol)
        })(collection.breakOut)

        // Return the first column in the array defined at the row, or -1 if none are defined for that row
        @inline def firstDefinedIndexFor(columns: Array[(CPath, Column)], row: Int): Int = {
          var i = 0
          while (i < columns.length && !columns(i)._2.isDefinedAt(row)) { i += 1 }
          if (i == columns.length) -1 else i
        }

        new CPathComparator {
          def compare(r1: Int, r2: Int, indices: Array[Int]) = {
            val i = firstDefinedIndexFor(lCols, r1)
            val j = firstDefinedIndexFor(rCols, r2)
            if (i == -1) {
              if (j == -1) {
                NoComp
              } else {
                Lt
              }
            } else if (j == -1) {
              Gt
            } else {
              comparators(i * rCols.length + j).compare(r1, r2, indices)
            }
          }
        }

      case Sequence(ts) =>
        val comparators: Array[CPathComparator] = ts.map(plan0(_, paths, idx))(collection.breakOut)

        new CPathComparator {
          def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
            var i                     = 0
            var result: MaybeOrdering = NoComp
            while ((result == Eq || result == NoComp) && i < comparators.length) {
              val iResult = comparators(i).compare(r1, r2, indices)
              if (iResult != NoComp) {
                result = iResult
              }

              i += 1
            }
            result
          }
        }

      case Select(CPathArray, t) =>
        plan0(Loop(0, None, t), paths, idx)

      case Select(CPathIndex(i), t) =>
        val matches = paths collect {
          case (CPathArray :: ns, p)      => (ns, CPathArray :: p)
          case (CPathIndex(`i`) :: ns, p) => (ns, CPathIndex(i) :: p)
        }
        val tailComp = plan0(t, matches, idx + 1)
        new CPathComparator {
          def compare(row1: Int, row2: Int, indices: Array[Int]) = {
            indices(idx) = i
            tailComp.compare(row1, row2, indices)
          }
        }

      case Select(n, t) =>
        val matches = paths collect { case (`n` :: ns, p) => (ns, n :: p) }
        plan0(t, matches, idx)

      case Loop(s, e, t) =>
        val matches  = paths collect { case (CPathArray :: ns, p) => (ns, CPathArray :: p) }
        val tailComp = plan0(t, matches, idx + 1)
        new CPathComparator {
          def compare(row1: Int, row2: Int, indices: Array[Int]) = {
            val max = e.getOrElse(Int.MaxValue) // Yep.

            indices(idx) = 0
            var result: MaybeOrdering = tailComp.compare(row1, row2, indices)

            var i = 1
            if (result != NoComp) {
              while (result == Eq && i < max) {
                indices(idx) = i
                result = tailComp.compare(row1, row2, indices)
                i += 1
              }
              if (result == NoComp) {
                result = Eq
              }
            }

            result
          }
        }
    }

    new SpireOrder[Int] {
      private val indices  = new Array[Int](self.arrayDepth)
      private val pathComp = plan0(self, cpaths map (_.nodes -> Nil), 0)

      def compare(row1: Int, row2: Int) = {
        pathComp.compare(row1, row2, indices).toInt
      }
    }
  }

  /**
    * Returns the maximum array depth of this traversal.
    */
  def arrayDepth: Int = this match {
    case Done                        => 0
    case Select(CPathArray, tail)    => 1 + tail.arrayDepth
    case Select(CPathIndex(_), tail) => 1 + tail.arrayDepth
    case Select(_, tail)             => tail.arrayDepth
    case Sequence(ts)                => ts.map(_.arrayDepth).max
    case Loop(_, _, tail)            => 1 + tail.arrayDepth
  }
}

object CPathTraversal {

  case object Done extends CPathTraversal
  case class Sequence(traversals: List[CPathTraversal])               extends CPathTraversal
  case class Select(path: CPathNode, next: CPathTraversal)            extends CPathTraversal
  case class Loop(start: Int, end: Option[Int], tail: CPathTraversal) extends CPathTraversal

  def apply(p: CPath): CPathTraversal = p.nodes.foldRight(Done: CPathTraversal)(Select(_, _))

  /**
    * Creates a traversal for the given `CPath`s. Note that this will always
    * create the traversal in sorted order of the paths, regardless of the order
    * given.
    */
  def apply(paths: List[CPath]): CPathTraversal = {

    // Basically does a mix of collect, takeWhile and dropWhile all in one go.
    def collectWhile[A, B](xs: List[A])(f: PartialFunction[A, B]): (List[B], List[A]) = {
      def loop(left: List[B], right: List[A]): (List[B], List[A]) = right match {
        case x :: right if f.isDefinedAt(x) => loop(f(x) :: left, right)
        case _                              => (left.reverse, right)
      }

      loop(Nil, xs)
    }

    // Joins (merges) a list of sorted traversals into a single traversal.
    def join(os: List[CPathTraversal], seq: List[CPathTraversal]): CPathTraversal = os match {
      case Select(n, t) :: os =>
        val (ts, os2) = collectWhile(os) { case Select(`n`, t2) => t2 }
        join(os2, Select(n, join(t :: ts, Nil)) :: seq)

      case Loop(s, e, t) :: os =>
        val (ts, os2) = collectWhile(os) { case Loop(`s`, `e`, t2) => t2 }
        join(os2, Loop(s, e, join(t :: ts, Nil)) :: seq)

      case t :: os =>
        join(os, t :: seq)

      case Nil =>
        seq match {
          case Nil      => Done
          case x :: Nil => x
          case xs       => Sequence(xs.reverse)
        }
    }

    val ordered = CPathPosition.disjointOrder(paths) map (fromPositioned(_))
    join(ordered, Nil)
  }

  // Beyond here be dragons (aka implementation details).

  private def fromPositioned(ps: List[CPathPosition]): CPathTraversal = {
    @tailrec
    def loop(ps: List[CPathPosition], tail: CPathTraversal): CPathTraversal = ps match {
      case CPathPoint(n) :: ps                       => loop(ps, Select(n, tail))
      case CPathRange(_, 0, None) :: ps              => loop(ps, Select(CPathArray, tail))
      case CPathRange(_, i, Some(j)) :: ps if i == j => loop(ps, Select(CPathIndex(i), tail))
      case CPathRange(_, start, end) :: ps           => loop(ps, Loop(start, end, tail))
      case Nil                                       => tail
    }

    loop(ps.reverse, Done)
  }

  private sealed trait CPathPosition
  private case class CPathPoint(node: CPathNode)                                     extends CPathPosition
  private case class CPathRange(nodes: Set[CPathNode], start: Int, end: Option[Int]) extends CPathPosition

  private object CPathPosition {

    def contains(ps: List[CPathPosition], path: CPath): Boolean = {

      @tailrec
      def loop(ps: List[CPathPosition], ns: List[CPathNode]): Boolean = (ps, ns) match {
        case (CPathPoint(p) :: ps, n :: ns) if p == n               => loop(ps, ns)
        case (CPathRange(ms, _, _) :: ps, n :: ns) if ms contains n => loop(ps, ns)
        case (Nil, Nil)                                             => true
        case _                                                      => false
      }

      loop(ps, path.nodes)
    }

    implicit object CPathPositionOrder extends ScalazOrder[CPathPosition] {
      import scalaz.Ordering._

      private val nodeOrder = implicitly[ScalazOrder[CPathNode]]

      def order(p1: CPathPosition, p2: CPathPosition): scalaz.Ordering = (p1, p2) match {
        case (CPathPoint(CPathIndex(i)), CPathRange(_, l, r)) =>
          if (i < l) LT else if (i > l) GT else EQ //if (r map (_ == i) getOrElse false) EQ else GT
        case (CPathRange(_, l, r), CPathPoint(CPathIndex(i))) =>
          if (i < l) GT else if (i > l) LT else EQ //if (r map (_ == i) getOrElse false) EQ else LT
        case (CPathRange(_, l1, r1), CPathRange(_, l2, r2)) =>
          if (l1 < l2) LT
          else if (l2 < l1) GT
          else {
            (r1, r2) match {
              case (Some(r1), Some(r2)) => implicitly[ScalazOrder[Int]].order(r1, r2)
              case (None, None)         => EQ
              case (_, None)            => LT
              case (None, _)            => GT
            }
          }
        case (CPathPoint(a), CPathPoint(b))       => nodeOrder.order(a, b)
        case (CPathPoint(a), CPathRange(_, i, _)) => nodeOrder.order(a, CPathIndex(i))
        case (CPathRange(_, i, _), CPathPoint(a)) => nodeOrder.order(CPathIndex(i), a)
      }
    }

    /**
      * Returns a `p` as a positioned CPath.
      */
    def position(p: CPath): List[CPathPosition] = p.nodes map {
      case CPathArray => CPathRange(Set(CPathArray), 0, None)
      case node       => CPathPoint(node)
    }

    private def overlaps(l1: Int, r1: Option[Int], l2: Int, r2: Option[Int]): Boolean = {
      (l2 >= l1 && l2 <= r1.getOrElse(l2)) || (l1 >= l2 && l1 <= r2.getOrElse(l1))
    }

    /**
      * This splits 2 positioned CPaths into a list of completely disjoint positioned CPaths.
      */
    def disjoint(c1: List[CPathPosition], c2: List[CPathPosition]): List[List[CPathPosition]] = {
      import scalaz.syntax.apply._
      import scalaz.std.option._

      // case 1: c1 and c2 are disjoint, just return List(c1, c2)
      // case 2: c1 and c2 overlap, return non-overlapping regions of c1 & c2 and the intersection of c1 & c2.
      @tailrec
      def loop(ps: List[CPathPosition], qs: List[CPathPosition], is: List[CPathPosition], rss: List[List[CPathPosition]]): List[List[CPathPosition]] =
        (ps, qs) match {
          case (Nil, Nil) =>
            is.reverse :: rss

          case (p :: ps, q :: qs) if p == q =>
            loop(ps, qs, p :: is, rss)

          case (CPathPoint(n @ CPathIndex(i)) :: ps, CPathRange(ns, j, k) :: qs) if i >= j && i <= k.getOrElse(i) =>
            val rss0 = if (j < i) {
              ((CPathRange(ns, j, Some(i - 1)) :: is) reverse_::: qs) :: rss
            } else rss

            val rss1 = if (k map (_ > i) getOrElse true) {
              ((CPathRange(ns, i + 1, k) :: is) reverse_::: qs) :: rss0
            } else rss0

            loop(ps, qs, CPathRange(ns + n, i, Some(i)) :: is, rss1)

          case (CPathRange(ns, j, k) :: ps, CPathPoint(n @ CPathIndex(i)) :: qs) if i >= j && i <= k.getOrElse(i) =>
            val rss0 = if (j < i) {
              ((CPathRange(ns, j, Some(i - 1)) :: is) reverse_::: ps) :: rss
            } else rss

            val rss1 = if (k map (_ > i) getOrElse true) {
              ((CPathRange(ns, i + 1, k) :: is) reverse_::: ps) :: rss0
            } else rss0

            loop(ps, qs, CPathRange(ns + n, i, Some(i)) :: is, rss1)

          case (CPathRange(ns1, l1, r1) :: ps, CPathRange(ns2, l2, r2) :: qs) if overlaps(l1, r1, l2, r2) =>
            val rss0 = if (l1 < l2) {
              ((CPathRange(ns1, l1, Some(l2 - 1)) :: is) reverse_::: ps) :: rss
            } else if (l2 < l1) {
              ((CPathRange(ns2, l2, Some(l1 - 1)) :: is) reverse_::: qs) :: rss
            } else {
              rss
            }

            val rss1 = (r1, r2) match {
              case (r1, Some(r2)) if r1 map (_ > r2) getOrElse true =>
                ((CPathRange(ns1, r2 + 1, r1) :: is) reverse_::: ps) :: rss0
              case (Some(r1), r2) if r2 map (_ > r1) getOrElse true =>
                ((CPathRange(ns2, r1 + 1, r2) :: is) reverse_::: qs) :: rss0
              case _ =>
                rss0
            }

            loop(ps, qs, CPathRange(ns1 ++ ns2, math.max(l1, l2), ^(r1, r2)(math.min(_, _)) orElse r1 orElse r2) :: is, rss1)

          case (ps, qs) =>
            List(is reverse_::: ps, is reverse_::: qs)
        }

      loop(c1, c2, Nil, Nil)
    }

    def intersect(as: List[CPathPosition], bs: List[CPathPosition]): Boolean = {

      @tailrec
      def loop(as: List[CPathPosition], bs: List[CPathPosition]): Boolean = (as, bs) match {
        case (a :: as, b :: bs) if a == b                                                                  => loop(as, bs)
        case (CPathPoint(CPathIndex(i)) :: as, CPathRange(_, j, k) :: bs) if i >= j && i <= k.getOrElse(i) => loop(as, bs)
        case (CPathRange(_, j, k) :: as, CPathPoint(CPathIndex(i)) :: bs) if i >= j && i <= k.getOrElse(i) => loop(as, bs)
        case (CPathRange(_, l1, r1) :: as, CPathRange(_, l2, r2) :: bs) if overlaps(l1, r1, l2, r2)        => loop(as, bs)
        case (Nil, Nil)                                                                                    => true
        case _                                                                                             => false
      }

      loop(as, bs)
    }

    /**
      * Returns an ordering of `paths` where all the elements are pairwise
      * disjoint.
      */
    def disjointOrder(paths: List[CPath]): List[List[CPathPosition]] = {
      import scalaz.std.list._

      val pq = mutable.PriorityQueue[List[CPathPosition]](paths map (position(_)): _*) {
        implicitly[ScalazOrder[List[CPathPosition]]].reverseOrder.toScalaOrdering
      }

      @tailrec
      def rec(a: List[CPathPosition], ts: List[List[CPathPosition]]): List[List[CPathPosition]] = {
        if (!pq.isEmpty) {
          val b = pq.dequeue()
          if (intersect(a, b)) {
            pq.enqueue(disjoint(a, b): _*)
            rec(pq.dequeue(), ts)
          } else {
            rec(b, a :: ts)
          }
        } else {
          (a :: ts).reverse
        }
      }

      if (pq.isEmpty) Nil else rec(pq.dequeue(), Nil)
    }
  }
}
