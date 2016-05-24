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
import quasar.{Data, Func, LogicalPlan, Type, Mapping, UnaryFunc, BinaryFunc, TernaryFunc, GenericFunc}, LogicalPlan._

import matryoshka._
import scalaz._, Scalaz._, Validation.success
import shapeless._

// TODO: Cleanup duplication in case statements!
trait RelationsLib extends Library {
  val Eq = BinaryFunc(
    Mapping,
    "(=)",
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
    "(<>)",
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
    "(<)",
    "Determines if one value is less than another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 < v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Bool(v1 < v2))
      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) => Type.Const(Data.Bool(v1 < v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Timestamp(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) < 0))
      case _ => Type.Bool
    },
    basicUntyper)

  val Lte = BinaryFunc(
    Mapping,
    "(<=)",
    "Determines if one value is less than or equal to another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 <= v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Bool(v1 <= v2))
      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) => Type.Const(Data.Bool(v1 <= v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Timestamp(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) <= 0))
      case _ => Type.Bool
    },
    basicUntyper)

  val Gt = BinaryFunc(
    Mapping,
    "(>)",
    "Determines if one value is greater than another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 > v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Bool(v1 > v2))
      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) => Type.Const(Data.Bool(v1 > v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Timestamp(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) > 0))
      case _ => Type.Bool
    },
    basicUntyper)

  val Gte = BinaryFunc(
    Mapping,
    "(>=)",
    "Determines if one value is greater than or equal to another value of the same type",
    Type.Bool,
    Func.Input2(Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Bool(v1)), Type.Const(Data.Bool(v2))) => Type.Const(Data.Bool(v1 >= v2))
      case Sized(Type.Const(Data.Number(v1)), Type.Const(Data.Number(v2))) => Type.Const(Data.Bool(v1 >= v2))
      case Sized(Type.Const(Data.Str(v1)), Type.Const(Data.Str(v2))) => Type.Const(Data.Bool(v1 >= v2))
      case Sized(Type.Const(Data.Timestamp(v1)), Type.Const(Data.Timestamp(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case Sized(Type.Const(Data.Interval(v1)), Type.Const(Data.Interval(v2))) => Type.Const(Data.Bool(v1.compareTo(v2) >= 0))
      case _ => Type.Bool
    },
    basicUntyper)

  val Between = TernaryFunc(
    Mapping,
    "(BETWEEN)",
    "Determines if a value is between two other values of the same type, inclusive",
    Type.Bool,
    Func.Input3(Type.Comparable, Type.Comparable, Type.Comparable),
    noSimplification,
    partialTyper[nat._3] {
      // TODO: partial evaluation for Int and Dec and possibly other constants
      case Sized(_, _, _) => Type.Bool
      case _ => Type.Bool
    },
    basicUntyper)

  val And = BinaryFunc(
    Mapping,
    "(AND)",
    "Performs a logical AND of two boolean values",
    Type.Bool,
    Func.Input2(Type.Bool, Type.Bool),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(Embed(ConstantF(Data.True)), Embed(r))) => r.some
          case InvokeF(_, Sized(Embed(l), Embed(ConstantF(Data.True)))) => l.some
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
    "(OR)",
    "Performs a logical OR of two boolean values",
    Type.Bool,
    Func.Input2(Type.Bool, Type.Bool),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(Embed(ConstantF(Data.False)), Embed(r))) => r.some
          case InvokeF(_, Sized(Embed(l), Embed(ConstantF(Data.False)))) => l.some
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
    "NOT",
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
    "(IF_THEN_ELSE)",
    "Chooses between one of two cases based on the value of a boolean expression",
    Type.Bottom,
    Func.Input3(Type.Bool, Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(Embed(ConstantF(Data.True)),  Embed(c), _)) => c.some
          case InvokeF(_, Sized(Embed(ConstantF(Data.False)), _, Embed(a))) => a.some
          case _                                                           => None
        }
    },
    partialTyper[nat._3] {
      case Sized(Type.Const(Data.Bool(true)), ifTrue, ifFalse) => ifTrue
      case Sized(Type.Const(Data.Bool(false)), ifTrue, ifFalse) => ifFalse
      case Sized(Type.Bool, ifTrue, ifFalse) => Type.lub(ifTrue, ifFalse)
    },
    untyper[nat._3](t => success(Func.Input3(Type.Bool, t, t))))

  val Coalesce = BinaryFunc(
    Mapping, 
    "coalesce",
    "Returns the first of its arguments that isn't null.",
    Type.Bottom,
    Func.Input2(Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(Embed(ConstantF(Data.Null)), Embed(second))) => second.some
          case InvokeF(_, Sized(Embed(first), Embed(ConstantF(Data.Null))))  => first.some
          case _                                                            => None
        }
    },
    partialTyper[nat._2] {
      case Sized(Type.Null, v2) => v2
      case Sized(Type.Const(Data.Null), v2) => v2
      case Sized(Type.Const(v1), _ ) => Type.Const(v1)
      case Sized(v1, Type.Null) => v1
      case Sized(v1, Type.Const(Data.Null)) => v1
      case Sized(v1, v2) => Type.lub(v1, v2)
    },
    untyper[nat._2] {
      case Type.Null => success(Func.Input2(Type.Null, Type.Null))
      case t         => success(Func.Input2(t ⨿ Type.Null, Type.Top))
    })

  def unaryFunctions: List[GenericFunc[nat._1]] = Not :: Nil

  def binaryFunctions: List[GenericFunc[nat._2]] =
    Eq :: Neq :: Lt :: Lte :: Gt :: Gte :: And :: Or :: Coalesce :: Nil

  def ternaryFunctions: List[GenericFunc[nat._3]] = Between :: Cond :: Nil

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
