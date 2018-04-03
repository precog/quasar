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

package quasar.std

import slamdata.Predef._
import quasar.{Data, Func, Type, Mapping, UnaryFunc, BinaryFunc, TernaryFunc, GenericFunc}
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

import matryoshka._
import scalaz._, Scalaz._
import shapeless._

trait RelationsLib extends Library {
  val Eq = BinaryFunc(
    Mapping,
    "Determines if two values are equal",
    Type.Bool,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(data1), Type.Const(data2)) =>
        Type.Const(Data.Bool(data1 == data2))

      case Sized(type1, type2)
        if Type.lub(type1, type2) == Type.Top && type1 != Type.Top && type2 != Type.Top =>
          Type.Const(Data.Bool(false))

      case _ => Type.Bool
    },
    basicUntyper)

  val Neq = BinaryFunc(
    Mapping,
    "Determines if two values are not equal",
    Type.Bool,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(data1), Type.Const(data2)) =>
        Type.Const(Data.Bool(data1 != data2))

      case Sized(type1, type2)
        if Type.lub(type1, type2) == Type.Top && type1 != Type.Top && type2 != Type.Top =>
          Type.Const(Data.Bool(true))

      case _ => Type.Bool
    },
    basicUntyper)

  val Lt = BinaryFunc(
    Mapping,
    "Determines if one value is less than another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) =>
        Type.Const(Data.Bool(v1 < v2))

      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) =>
        Type.Const(Data.Bool(v1 < v2))

      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) =>
        Type.Const(Data.Bool(v1 < v2))

      case Sized(Type.Const(Data.LocalDate(v1)), Type.Const(Data.LocalDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case Sized(Type.Const(Data.LocalDateTime(v1)), Type.Const(Data.LocalDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case Sized(Type.Const(Data.LocalTime(v1)), Type.Const(Data.LocalTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case Sized(Type.Const(Data.OffsetDate(v1)), Type.Const(Data.OffsetDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case Sized(Type.Const(Data.OffsetDateTime(v1)), Type.Const(Data.OffsetDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case Sized(Type.Const(Data.OffsetTime(v1)), Type.Const(Data.OffsetTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) < 0))

      case _ => Type.Bool
    },
    basicUntyper)

  val Lte = BinaryFunc(
    Mapping,
    "Determines if one value is less than or equal to another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) =>
        Type.Const(Data.Bool(v1 <= v2))

      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) =>
        Type.Const(Data.Bool(v1 <= v2))

      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) =>
        Type.Const(Data.Bool(v1 <= v2))

      case Sized(Type.Const(Data.LocalDate(v1)), Type.Const(Data.LocalDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case Sized(Type.Const(Data.LocalDateTime(v1)), Type.Const(Data.LocalDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case Sized(Type.Const(Data.LocalTime(v1)), Type.Const(Data.LocalTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case Sized(Type.Const(Data.OffsetDate(v1)), Type.Const(Data.OffsetDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case Sized(Type.Const(Data.OffsetDateTime(v1)), Type.Const(Data.OffsetDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case Sized(Type.Const(Data.OffsetTime(v1)), Type.Const(Data.OffsetTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) <= 0))

      case _ => Type.Bool
    },
    basicUntyper)

  val Gt = BinaryFunc(
    Mapping,
    "Determines if one value is greater than another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) =>
        Type.Const(Data.Bool(v1 > v2))

      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) =>
        Type.Const(Data.Bool(v1 > v2))

      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) =>
        Type.Const(Data.Bool(v1 > v2))

      case Sized(Type.Const(Data.LocalDate(v1)), Type.Const(Data.LocalDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case Sized(Type.Const(Data.LocalDateTime(v1)), Type.Const(Data.LocalDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case Sized(Type.Const(Data.LocalTime(v1)), Type.Const(Data.LocalTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case Sized(Type.Const(Data.OffsetDate(v1)), Type.Const(Data.OffsetDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case Sized(Type.Const(Data.OffsetDateTime(v1)), Type.Const(Data.OffsetDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case Sized(Type.Const(Data.OffsetTime(v1)), Type.Const(Data.OffsetTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) > 0))

      case _ => Type.Bool
    },
    basicUntyper)

  val Gte = BinaryFunc(
    Mapping,
    "Determines if one value is greater than or equal to another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) =>
        Type.Const(Data.Bool(v1 >= v2))

      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) =>
        Type.Const(Data.Bool(v1 >= v2))

      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) =>
        Type.Const(Data.Bool(v1 >= v2))

      case Sized(Type.Const(Data.LocalDate(v1)), Type.Const(Data.LocalDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case Sized(Type.Const(Data.LocalDateTime(v1)), Type.Const(Data.LocalDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case Sized(Type.Const(Data.LocalTime(v1)), Type.Const(Data.LocalTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case Sized(Type.Const(Data.OffsetDate(v1)), Type.Const(Data.OffsetDate(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case Sized(Type.Const(Data.OffsetDateTime(v1)), Type.Const(Data.OffsetDateTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case Sized(Type.Const(Data.OffsetTime(v1)), Type.Const(Data.OffsetTime(v2))) =>
        Type.Const(Data.Bool(v1.compareTo(v2) >= 0))

      case _ => Type.Bool
    },
    basicUntyper)

  val Between = TernaryFunc(
    Mapping,
    "Determines if a value is between two other values of the same type, inclusive",
    Type.Bool,
    Func.Input3(Type.Comparable, Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._3] {
      // TODO: partial evaluation for Int and Dec and possibly other constants
      case Sized(Type.Const(Data.Dec(mid)), Type.Const(Data.Dec(lo)), Type.Const(Data.Dec(hi))) =>
        Type.Const(Data.Bool(lo <= mid && mid <= hi))

      case Sized(Type.Const(Data.Int(mid)), Type.Const(Data.Int(lo)), Type.Const(Data.Int(hi))) =>
        Type.Const(Data.Bool(lo <= mid && mid <= hi))

      case Sized(Type.Const(Data.Str(mid)), Type.Const(Data.Str(lo)), Type.Const(Data.Str(hi))) =>
        Type.Const(Data.Bool(lo <= mid && mid <= hi))

      case Sized(Type.Const(Data.OffsetDateTime(mid)), Type.Const(Data.OffsetDateTime(lo)), Type.Const(Data.OffsetDateTime(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.OffsetTime(mid)), Type.Const(Data.OffsetTime(lo)), Type.Const(Data.OffsetTime(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.OffsetDate(mid)), Type.Const(Data.OffsetDate(lo)), Type.Const(Data.OffsetDate(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.LocalDateTime(mid)), Type.Const(Data.LocalDateTime(lo)), Type.Const(Data.LocalDateTime(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.LocalTime(mid)), Type.Const(Data.LocalTime(lo)), Type.Const(Data.LocalTime(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.LocalDate(mid)), Type.Const(Data.LocalDate(lo)), Type.Const(Data.LocalDate(hi))) =>
        Type.Const(Data.Bool(lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0))

      case Sized(Type.Const(Data.Bool(mid)), Type.Const(Data.Bool(lo)), Type.Const(Data.Bool(hi))) =>
        Type.Const(Data.Bool(((lo == mid) && (mid == hi)) || (!lo && ((!mid && hi) || (mid && hi)))))

      case Sized(_, _, _) => Type.Bool

      case _ => Type.Bool
    },
    basicUntyper)

  val IfUndefined = BinaryFunc(
    Mapping,
    "This is the only way to recognize an undefined value. If the first argument is undefined, return the second argument, otherwise, return the first.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyper {
      case Sized(value, fallback) => Type.TypeOrMonoid.append(value, fallback)
    },
    basicUntyper)

  val And = BinaryFunc(
    Mapping,
    "Performs a logical AND of two boolean values",
    Type.Bool,
    Func.Input2(Type.Bool, Type.Bool),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.True)), Embed(r))) => r.some
          case Invoke(_, Sized(Embed(l), Embed(Constant(Data.True)))) => l.some
          case _                                                       => None
        }
    },
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 && v2))
      case Sized(Type.Const(Data.Bool(false)), _) => Type.Const(Data.Bool(false))
      case Sized(_, Type.Const(Data.Bool(false))) => Type.Const(Data.Bool(false))
      case Sized(Type.Const(Data.Bool(true)), x) => x
      case Sized(x, Type.Const(Data.Bool(true))) => x
      case _ => Type.Bool
    },
    basicUntyper)

  val Or = BinaryFunc(
    Mapping,
    "Performs a logical OR of two boolean values",
    Type.Bool,
    Func.Input2(Type.Bool, Type.Bool),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.False)), Embed(r))) => r.some
          case Invoke(_, Sized(Embed(l), Embed(Constant(Data.False)))) => l.some
          case _                                                        => None
        }
    },
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 || v2))
      case Sized(Type.Const(Data.Bool(true)), _) => Type.Const(Data.Bool(true))
      case Sized(_, Type.Const(Data.Bool(true))) => Type.Const(Data.Bool(true))
      case Sized(Type.Const(Data.Bool(false)), x) => x
      case Sized(x, Type.Const(Data.Bool(false))) => x
      case _ => Type.Bool
    },
    basicUntyper)

  val Not = UnaryFunc(
    Mapping,
    "Performs a logical negation of a boolean value",
    Type.Bool,
    Func.Input1(Type.Bool),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Bool(v))) => Type.Const(Data.Bool(!v))
      case _ => Type.Bool
    },
    basicUntyper)

  val Cond = TernaryFunc(
    Mapping,
    "Chooses between one of two cases based on the value of a boolean expression",
    Type.Top,
    Func.Input3(Type.Bool, Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(Constant(Data.True)),  Embed(c), _)) => c.some
          case Invoke(_, Sized(Embed(Constant(Data.False)), _, Embed(a))) => a.some
          case _                                                            => None
        }
    },
    partialTyper[nat._3] {
      case Sized(Type.Const(Data.Bool(true)), ifTrue, _) => ifTrue
      case Sized(Type.Const(Data.Bool(false)), _, ifFalse) => ifFalse
      case Sized(Type.Bool, ifTrue, ifFalse) => Type.lub(ifTrue, ifFalse)
    },
    basicUntyper[nat._3])

  def flip(f: GenericFunc[nat._2]): Option[GenericFunc[nat._2]] = f match {
    case Eq  => Some(Eq)
    case Neq => Some(Neq)
    case Lt  => Some(Gt)
    case Lte => Some(Gte)
    case Gt  => Some(Lt)
    case Gte => Some(Lte)
    case And => Some(And)
    case Or  => Some(Or)
    case _   => None
  }

  def negate(f: GenericFunc[nat._2]): Option[GenericFunc[nat._2]] = f match {
    case Eq  => Some(Neq)
    case Neq => Some(Eq)
    case Lt  => Some(Gte)
    case Lte => Some(Gt)
    case Gt  => Some(Lte)
    case Gte => Some(Lt)
    case _   => None
  }
}

object RelationsLib extends RelationsLib
