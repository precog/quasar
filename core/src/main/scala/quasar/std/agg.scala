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

package quasar.std

import quasar.Predef._
import quasar._

import org.threeten.bp.Duration
import scalaz._, Validation.{success, failureNel}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._

trait AggLib extends Library {
  private val reflexiveUntyper: Func.Untyper =
    untyper(t => success(List(t)))

  val Count = Reduction("COUNT", "Counts the values in a set",
    Type.Int, Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(xs))) => Type.Const(Data.Int(xs.length))
      case List(_)                        => Type.Int
    },
    basicUntyper)

  val Sum = Reduction("SUM", "Sums the values in a set",
    Type.Numeric ⨿ Type.Interval, Type.Numeric ⨿ Type.Interval :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(Data.Set(Nil))) =>
        success(Type.Const(Data.Int(0)))

      case List(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Set(Type.Int) =>
        intSet(xs)
          .map(ys => Type.Const(Data.Int(ys.sum)))
          .validationNel

      case List(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Set(Type.Dec) =>
        decSet(xs)
          .map(ys => Type.Const(Data.Dec(ys.sum)))
          .validationNel

      case List(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Set(Type.Interval) =>
        ivlSet(xs)
          .map(ys => Type.Const(Data.Interval(ys.foldLeft(Duration.ZERO)(_ plus _))))
          .validationNel

      case List(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Min = Reduction("MIN", "Finds the minimum in a set of values",
    Type.Comparable, Type.Comparable :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(Data.Set(xs))) =>
        reduceComparableSet(Data.Comparable.min)(xs)
          .map(c => Type.Const(c.value))

      case List(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Max = Reduction("MAX", "Finds the maximum in a set of values",
    Type.Comparable, Type.Comparable :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(Data.Set(xs))) =>
        reduceComparableSet(Data.Comparable.max)(xs)
          .map(c => Type.Const(c.value))

      case List(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Avg = Reduction("AVG", "Finds the average in a set of numeric values",
    Type.Numeric ⨿ Type.Interval, Type.Numeric ⨿ Type.Interval :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(Data.Set(Nil))) =>
        expectedNonEmptySet

      case List(Type.Const(Data.Set(xs))) =>
        numSet(xs)
          .map(ns => Type.Const(Data.Dec(ns.sum / ns.length)))
          .validationNel

      case List(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Arbitrary = Reduction("ARBITRARY", "Returns an arbitrary value from a set",
    Type.Top, Type.Top :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Const(Data.Set(Nil))) =>
        expectedNonEmptySet

      case List(Type.Const(Data.Set(x :: xs))) =>
        success(Type.Const(x))

      case List(t) =>
        success(t)
    },
    reflexiveUntyper)

  def functions = Count :: Sum :: Min :: Max :: Avg :: Arbitrary :: Nil

  ////

  private val errSetF = Functor[SemanticError \/ ?].compose[List]

  private def expectedNonEmptySet[A]: ValidationNel[SemanticError, A] =
    failureNel(SemanticError.DomainError(Data.Set(Nil), some("Expected non-empty Set")))

  private def reduceComparableSet(
    f: (Data.Comparable, Data.Comparable) => Option[Data.Comparable]
  ): List[Data] => ValidationNel[SemanticError, Data.Comparable] =
    _.toNel.fold(expectedNonEmptySet[Data.Comparable])(xs =>
      xs.traverse(Data.Comparable(_))
        .flatMap(ys => ys.tail.foldLeftM(ys.head)(f))
        .toSuccessNel(SemanticError.DomainError(
          Data.Set(xs.list),
          some("Expected Set of comparable values"))))

  private val numSet: List[Data] => SemanticError \/ List[BigDecimal] =
    set =>
      errSetF.map(ivlSet(set))(d => BigDecimal(d.toMillis))
        .orElse(errSetF.map(intSet(set))(BigDecimal(_)))
        .orElse(decSet(set))
        .leftAs(SemanticError.DomainError(Data.Set(set), some("Expected Set of numeric values")))

  private val ivlSet: List[Data] => SemanticError \/ List[Duration] =
    homogenizedPF({ case Data.Interval(d) => d }, "Expected Set(Interval)")

  private val decSet: List[Data] => SemanticError \/ List[BigDecimal] =
    homogenizedPF({ case Data.Dec(n) => n }, "Expected Set(Dec)")

  private val intSet: List[Data] => SemanticError \/ List[BigInt] =
    homogenizedPF({ case Data.Int(n) => n }, "Expected Set(Int)")

  private def homogenizedPF[A](f: PartialFunction[Data, A], err: String): List[Data] => SemanticError \/ List[A] =
    homogenized(f.lift, err)

  private def homogenized[A](f: Data => Option[A], err: String): List[Data] => SemanticError \/ List[A] =
    set => set.traverseU(f) \/> SemanticError.DomainError(Data.Set(set), some(err))
}

object AggLib extends AggLib
