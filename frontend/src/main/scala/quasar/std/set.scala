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
import quasar._
import quasar.common.JoinType
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.sql.JoinDir

import scala.collection.immutable.NumericRange

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._, Validation.success
import shapeless.{Data => _, _}

trait SetLib extends Library {
  val Take: BinaryFunc = BinaryFunc(
    Sifting,
    "Takes the first N elements from a set",
    Type.Top,
    Func.Input2(Type.Top, Type.Int),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Take, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Take(src, Constant[T](Data.Int(m.min(n))).embed).some
          case _ => None
        }
    },
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Int))))

  val Sample: BinaryFunc = BinaryFunc(
    Sifting,
    "Randomly selects N elements from a set",
    Type.Top,
    Func.Input2(Type.Top, Type.Int),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Take, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Take(src, Constant[T](Data.Int(m.min(n))).embed).some
          case _ => None
        }
    },
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Int))))

  val Drop: BinaryFunc = BinaryFunc(
    Sifting,
    "Drops the first N elements from a set",
    Type.Top,
    Func.Input2(Type.Top, Type.Int),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(set), Embed(Constant(Data.Int(n)))))
              if n == 0 =>
            set.some
          case InvokeUnapply(_, Sized(Embed(InvokeUnapply(Drop, Sized(src, Embed(Constant(Data.Int(m)))))), Embed(Constant(Data.Int(n))))) =>
            Drop(src, Constant[T](Data.Int(m + n)).embed).some
          case _ => None
        }
    },
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Int))))

  val Range = BinaryFunc(
    Mapping,
    "Creates an array of values in the range from `a` to `b`, inclusive.",
    Type.FlexArr(0, None, Type.Int),
    Func.Input2(Type.Int, Type.Int),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(Data.Int(a)), Type.Const(Data.Int(b))) if a <= b =>
        Type.Const(
          Data.Arr(NumericRange.inclusive[BigInt](a, b, 1).toList ∘ (Data.Int(_))))
      case Sized(_, _) => Type.Int
    },
    basicUntyper)

  val Filter = BinaryFunc(
    Sifting,
    "Filters a set to include only elements where a projection is true",
    Type.Top,
    Func.Input2(Type.Top, Type.Bool),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(Embed(set), Embed(Constant(Data.True)))) =>
            set.some
          case _ => None
        }
    },
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Bool))))

  def joinTyper(ty: JoinType): Func.Typer[nat._3] = ty match {
    case JoinType.Inner      => partialTyper[nat._3] {
      case Sized(s1, s2, _) =>
        Type.Obj(Map(JoinDir.Left.name -> s1, JoinDir.Right.name -> s2), None)
    }
    case JoinType.LeftOuter  => partialTyper[nat._3] {
      case Sized(s1, s2, _) =>
        Type.Obj(Map(JoinDir.Left.name -> s1, JoinDir.Right.name -> (s2 ⨿ Type.Null)), None)
    }
    case JoinType.RightOuter => partialTyper[nat._3] {
      case Sized(s1, s2, _) =>
        Type.Obj(Map(JoinDir.Left.name -> (s1 ⨿ Type.Null), JoinDir.Right.name -> s2), None)
    }
    case JoinType.FullOuter  => partialTyper[nat._3] {
      case Sized(s1, s2, _) =>
        Type.Obj(Map(JoinDir.Left.name -> (s1 ⨿ Type.Null), JoinDir.Right.name -> (s2 ⨿ Type.Null)), None)
    }
  }

  def joinUntyper: Func.Untyper[nat._3] =
    untyper[nat._3](t =>
      (t.mapKey(Type.Const(JoinDir.Left.data)) |@| t.mapKey(Type.Const(JoinDir.Right.data)))((l, r) =>
        Func.Input3(l, r, Type.Bool)))

  val GroupBy = BinaryFunc(
    Transformation,
    "Groups a projection of a set by another projection",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  val Union = BinaryFunc(
    Transformation,
    "Creates a new set with all the elements of each input set, keeping duplicates.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(s1, s2)
        if s1.contains(Type.Top) || s2.contains(Type.Top) => Type.Top
      case Sized(s1, s2)                                  => s1 ⨿ s2
    },
    untyper[nat._2](t => success(Func.Input2(t, t))))

  val Intersect = BinaryFunc(
    Transformation,
    "Creates a new set with only the elements that exist in both input sets, keeping duplicates.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyperV {
      case Sized(t1, t2) => success(Type.TypeOrMonoid.append(t1.widenConst, t2.widenConst))
    },
    untyper[nat._2](t => success(Func.Input2(t, t))))

  val Except = BinaryFunc(
    Transformation,
    "Removes the elements of the second set from the first set.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  // TODO: Handle “normal” functions without creating Funcs. They should be in
  //       a separate functor and inlined prior to getting this far. It will
  //       also allow us to make simplification non-Corecursive and ∴ operate
  //       on Cofree.
  val In = BinaryFunc(
    Sifting,
    "Determines whether a value is in a given set.",
    Type.Bool,
    Func.Input2(Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T]
        (orig: LP[T])
        (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) =
        orig match {
          case Invoke(_, Sized(item, set)) => set.project match {
            case Constant(_)            => RelationsLib.Eq(item, set).some
            case lp                     => Within(item, StructuralLib.UnshiftArray(set).embed).some
          }
          case _ => None
        }
    },
    basicTyper,
    basicUntyper)

  val Within = BinaryFunc(
    Mapping,
    "Determines whether a value is in a given array.",
    Type.Bool,
    Func.Input2(Type.Top, Type.AnyArray),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(_, Type.Const(Data.Arr(Nil))) =>
        Type.Const(Data.Bool(false))
      case Sized(Type.Const(x), Type.Const(Data.Arr(arr))) =>
        Type.Const(Data.Bool(arr.contains(x)))
      case Sized(_, _) => Type.Bool
    },
    basicUntyper)

  val Constantly = BinaryFunc(
    Transformation,
    "Always return the same value",
    Type.Bottom,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    widenConstTyper[nat._2](_(0)),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))
}

object SetLib extends SetLib
