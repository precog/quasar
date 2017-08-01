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

package quasar.std

import slamdata.Predef._
import quasar._

import java.time.Duration
import scalaz._, Validation.{success, failureNel}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import shapeless.{Data => _, :: => _, _}

trait AggLib extends Library {
  private val reflexiveUntyper: Func.Untyper[nat._1] =
    untyper[nat._1](t => success(Func.Input1(t)))

  val Count = UnaryFunc(
    Reduction,
    "Counts the values in a set",
    Type.Int,
    Func.Input1(Type.Top),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Set(xs))) => Type.Const(Data.Int(xs.length))
      case Sized(Type.Const(_))            => Type.Const(Data.Int(1))
      case Sized(_)                        => Type.Int
    },
    basicUntyper)

  val Sum = UnaryFunc(
    Reduction,
    "Sums the values in a set",
    Type.Numeric ⨿ Type.Interval,
    Func.Input1(Type.Numeric ⨿ Type.Interval),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Set(Nil))) =>
        success(Type.Const(Data.Int(0)))

      case Sized(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Int =>
        intSet(xs)
          .map(ys => Type.Const(Data.Int(ys.sum)))
          .validationNel

      case Sized(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Dec =>
        decSet(xs)
          .map(ys => Type.Const(Data.Dec(ys.sum)))
          .validationNel

      case Sized(Type.Const(s @ Data.Set(xs))) if s.dataType == Type.Interval =>
        ivlSet(xs)
          .map(ys => Type.Const(Data.Interval(ys.foldLeft(Duration.ZERO)(_ plus _))))
          .validationNel

      case Sized(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Min = UnaryFunc(
    Reduction,
    "Finds the minimum in a set of values",
    Type.Comparable,
    Func.Input1(Type.Comparable),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Set(xs))) =>
        reduceComparableSet(Data.Comparable.min)(xs)
          .map(c => Type.Const(c.value))

      case Sized(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Max = UnaryFunc(
    Reduction,
    "Finds the maximum in a set of values",
    Type.Comparable,
    Func.Input1(Type.Comparable),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Set(xs))) =>
        reduceComparableSet(Data.Comparable.max)(xs)
          .map(c => Type.Const(c.value))

      case Sized(t) =>
        success(t)
    },
    reflexiveUntyper)

  val First = UnaryFunc(
    Reduction,
    "Finds the first value in a set.",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(t) => success(t)
    },
    reflexiveUntyper)

  val Last = UnaryFunc(
    Reduction,
    "Finds the last value in a set.",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(t) => success(t)
    },
    reflexiveUntyper)

  val Avg = UnaryFunc(
    Reduction,
    "Finds the average in a set of numeric values",
    Type.Numeric ⨿ Type.Interval,
    Func.Input1(Type.Numeric ⨿ Type.Interval),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Set(Nil))) =>
        expectedNonEmptySet

      case Sized(Type.Const(Data.Set(xs))) =>
        numSet(xs)
          .map(ns => Type.Const(Data.Dec(ns.sum / ns.length)))
          .validationNel

      case Sized(t) =>
        success(t)
    },
    reflexiveUntyper)

  val Arbitrary = UnaryFunc(
    Reduction,
    "Returns an arbitrary value from a set",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Set(Nil))) =>
        expectedNonEmptySet

      case Sized(Type.Const(Data.Set(x :: xs))) =>
        success(Type.Const(x))

      case Sized(t) =>
        success(t)
    },
    reflexiveUntyper)

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
          Data.Set(xs.list.toList),
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
    set => set.traverse(f) \/> SemanticError.DomainError(Data.Set(set), some(err))
}

object AggLib extends AggLib
