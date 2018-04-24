/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._
import quasar._
import quasar.std.StdLib._

import matryoshka._
import scalaz._, Scalaz._

sealed abstract class ReduceFunc[A]

object ReduceFunc {
  import ReduceFuncs._

  implicit val equal: Delay[Equal, ReduceFunc] =
    new Delay[Equal, ReduceFunc] {
      def apply[A](eq: Equal[A]) = Equal.equal {
        case (Count(a),        Count(b))        => eq.equal(a, b)
        case (Sum(a),          Sum(b))          => eq.equal(a, b)
        case (Min(a),          Min(b))          => eq.equal(a, b)
        case (Max(a),          Max(b))          => eq.equal(a, b)
        case (Avg(a),          Avg(b))          => eq.equal(a, b)
        case (Arbitrary(a),    Arbitrary(b))    => eq.equal(a, b)
        case (First(a),        First(b))        => eq.equal(a, b)
        case (Last(a),         Last(b))         => eq.equal(a, b)
        case (UnshiftArray(a), UnshiftArray(b)) => eq.equal(a, b)
        case (UnshiftMap(a1, a2), UnshiftMap(b1, b2)) => eq.equal(a1, b1) && eq.equal(a2, b2)
        case (_,               _)               => false
      }
    }

  implicit val show: Delay[Show, ReduceFunc] =
    new Delay[Show, ReduceFunc] {
      def apply[A](show: Show[A]) = Show.show {
        case Count(a)        => Cord("Count(") ++ show.show(a) ++ Cord(")")
        case Sum(a)          => Cord("Sum(") ++ show.show(a) ++ Cord(")")
        case Min(a)          => Cord("Min(") ++ show.show(a) ++ Cord(")")
        case Max(a)          => Cord("Max(") ++ show.show(a) ++ Cord(")")
        case Avg(a)          => Cord("Avg(") ++ show.show(a) ++ Cord(")")
        case Arbitrary(a)    => Cord("Arbitrary(") ++ show.show(a) ++ Cord(")")
        case First(a)        => Cord("First(") ++ show.show(a) ++ Cord(")")
        case Last(a)         => Cord("Last(") ++ show.show(a) ++ Cord(")")
        case UnshiftArray(a) => Cord("UnshiftArray(") ++ show.show(a) ++ Cord(")")
        case UnshiftMap(a1, a2) => Cord("UnshiftMap(") ++ show.show(a1) ++ Cord(", ") ++ show.show(a2) ++ Cord(")")
      }
    }

  implicit val renderTree: Delay[RenderTree, ReduceFunc] =
    new Delay[RenderTree, ReduceFunc] {
      val nt = "ReduceFunc" :: Nil

      def apply[A](r: RenderTree[A]) = {
        def nAry(typ: String, as: A*): RenderedTree =
          NonTerminal(typ :: nt, None, as.toList.map(r.render(_)))

        RenderTree.make {
          case Count(a)           => nAry("Count", a)
          case Sum(a)             => nAry("Sum", a)
          case Min(a)             => nAry("Min", a)
          case Max(a)             => nAry("Max", a)
          case Avg(a)             => nAry("Avg", a)
          case Arbitrary(a)       => nAry("Arbitrary", a)
          case First(a)           => nAry("First", a)
          case Last(a)            => nAry("Last", a)
          case UnshiftArray(a)    => nAry("UnshiftArray", a)
          case UnshiftMap(a1, a2) => nAry("UnshiftMap", a1, a2)
        }
      }
    }

  implicit val traverse1: Traverse1[ReduceFunc] = new Traverse1[ReduceFunc] {
    def foldMapRight1[A, B](fa: ReduceFunc[A])(z: (A) ⇒ B)(f: (A, ⇒ B) ⇒ B): B =
      fa match {
        case Count(a)        => z(a)
        case Sum(a)          => z(a)
        case Min(a)          => z(a)
        case Max(a)          => z(a)
        case Avg(a)          => z(a)
        case Arbitrary(a)    => z(a)
        case First(a)        => z(a)
        case Last(a)         => z(a)
        case UnshiftArray(a) => z(a)
        case UnshiftMap(a1, a2) => f(a1, z(a2))
      }

    def traverse1Impl[G[_]: Apply, A, B](fa: ReduceFunc[A])(f: (A) ⇒ G[B]) =
      fa match {
        case Count(a)        => f(a) ∘ (Count(_))
        case Sum(a)          => f(a) ∘ (Sum(_))
        case Min(a)          => f(a) ∘ (Min(_))
        case Max(a)          => f(a) ∘ (Max(_))
        case Avg(a)          => f(a) ∘ (Avg(_))
        case Arbitrary(a)    => f(a) ∘ (Arbitrary(_))
        case First(a)        => f(a) ∘ (First(_))
        case Last(a)         => f(a) ∘ (Last(_))
        case UnshiftArray(a) => f(a) ∘ (UnshiftArray(_))
        case UnshiftMap(a1, a2) => (f(a1) ⊛ f(a2))(UnshiftMap(_, _))
      }
  }

  def translateUnaryReduction[A]: PartialFunction[UnaryFunc, A => ReduceFunc[A]] = {
    case agg.Count               => Count(_)
    case agg.Sum                 => Sum(_)
    case agg.Min                 => Min(_)
    case agg.Max                 => Max(_)
    case agg.First               => First(_)
    case agg.Last                => Last(_)
    case agg.Avg                 => Avg(_)
    case agg.Arbitrary           => Arbitrary(_)
    case structural.UnshiftArray => UnshiftArray(_)
  }

  def translateBinaryReduction[A]: PartialFunction[BinaryFunc, (A, A) => ReduceFunc[A]] = {
    case structural.UnshiftMap => UnshiftMap(_, _)
  }

  /** Indicates whether the order of the set going into the reduction is important. */
  val isOrderDependent: ReduceFunc[_] => Boolean = {
    case Count(_)         => false
    case Sum(_)           => false
    case Min(_)           => false
    case Max(_)           => false
    case Avg(_)           => false
    case Arbitrary(_)     => false
    case First(_)         => true
    case Last(_)          => true
    case UnshiftArray(_)  => true
    case UnshiftMap(_, _) => true
  }
}

// TODO we should statically verify that these have a `DimensionalEffect` of `Reduction`
object ReduceFuncs {
  final case class Count[A](a: A)        extends ReduceFunc[A]
  final case class Sum[A](a: A)          extends ReduceFunc[A]
  final case class Min[A](a: A)          extends ReduceFunc[A]
  final case class Max[A](a: A)          extends ReduceFunc[A]
  final case class Avg[A](a: A)          extends ReduceFunc[A]
  /** This is intended to be the “cheapest” way to get a single value out of a
    * set, where it doesn’t matter which one (usually used in the case where all
    * entries are known to be the same).
    */
  final case class Arbitrary[A](a: A)          extends ReduceFunc[A]
  final case class First[A](a: A)              extends ReduceFunc[A]
  final case class Last[A](a: A)               extends ReduceFunc[A]
  final case class UnshiftArray[A](a: A)       extends ReduceFunc[A]
  final case class UnshiftMap[A](a1: A, a2: A) extends ReduceFunc[A]
}
