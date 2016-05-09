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
import quasar.fp._
import quasar._, LogicalPlan._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._, Validation.success
import shapeless.{Data => _, _}

trait SetLib extends Library {
  // NB: MRA should make this go away, as we insert dimensiality adjustements
  //     in the appropriate places.
  private def setTyper[N <: Nat](f: Func.Typer[N]): Func.Typer[N] =
    ts => f(ts).map {
      case x @ Type.Const(Data.Set(_)) => x
      case rez                         => rez
    }

  val Take = BinaryFunc(
    Sifting,
    "(LIMIT)",
    "Takes the first N elements from a set",
    Type.Top,
    Func.Input2(Type.Top, Type.Int),
    noSimplification,
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(_, Type.Const(Data.Int(n))) if n == 0 =>
        Type.Const(Data.Set(Nil))
      case Sized(Type.Const(Data.Set(s)), Type.Const(Data.Int(n)))
          if n.isValidInt =>
        Type.Const(Data.Set(s.take(n.intValue)))
      case Sized(t, _) => t
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Int))))

  val Drop = BinaryFunc(
    Sifting,
    "(OFFSET)",
    "Drops the first N elements from a set",
    Type.Top,
    Func.Input2(Type.Top, Type.Int),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, Sized(Embed(set), Embed(ConstantF(Data.Int(n)))))
            if n == 0 =>
          set.some
        case _ => None
      }
    },
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(Type.Const(Data.Set(s)), Type.Const(Data.Int(n)))
          if n.isValidInt =>
        Type.Const(Data.Set(s.drop(n.intValue)))
      case Sized(t, _) => t
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Int))))

  val OrderBy = TernaryFunc(
    Sifting,
    "ORDER BY",
    "Orders a set by the natural ordering of a projection on the set",
    Type.Top,
    Func.Input3(Type.Top, Type.Top, Type.Top),
    noSimplification,
    setTyper[nat._3](partialTyper[nat._3] {
      case Sized(set, _, _) => set
    }),
    untyper[nat._3](t => success(Func.Input3(t, Type.Top, Type.Top))))

  val Filter = BinaryFunc(
    Sifting,
    "WHERE",
    "Filters a set to include only elements where a projection is true",
    Type.Top,
    Func.Input2(Type.Top, Type.Bool),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(Embed(set), Embed(ConstantF(Data.True)))) =>
            set.some
          case _ => None
        }
    },
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(_ , Type.Const(Data.False)) => Type.Const(Data.Set(Nil))
      case Sized(set, _) => set
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Bool))))

  object JoinFunc {
    def unapply(func: TernaryFunc): Option[TernaryFunc] =
      func match {
        case (InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin) => Some(func)
        case _ => None
      }
  }

  val InnerJoin = TernaryFunc(
    Transformation,
    "INNER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition.",
    Type.Top,
    Func.Input3(Type.Top, Type.Top, Type.Bool),
    noSimplification,
    setTyper[nat._3](partialTyper[nat._3] {
      case Sized(_, _, Type.Const(Data.Bool(false))) => Type.Const(Data.Set(Nil))
      case Sized(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case Sized(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case Sized(s1, s2, _) => Type.Obj(Map("left" -> s1, "right" -> s2), None)
    }),
    untyper[nat._3](t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        Func.Input3(l, r, Type.Bool))))

  val LeftOuterJoin = TernaryFunc(
    Transformation,
    "LEFT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the left set.",
    Type.Top,
    Func.Input3(Type.Top, Type.Top, Type.Bool),
    noSimplification,
    setTyper[nat._3](partialTyper[nat._3] {
      case Sized(s1, _, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> s1, "right" -> Type.Null), None)
      case Sized(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case Sized(s1, s2, _) =>
        Type.Obj(Map("left" -> s1, "right" -> (s2 ⨿ Type.Null)), None)
    }),
    untyper[nat._3](t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        Func.Input3(l, r, Type.Bool))))

  val RightOuterJoin = TernaryFunc(
    Transformation,
    "RIGHT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the right set.",
    Type.Top,
    Func.Input3(Type.Top, Type.Top, Type.Bool),
    noSimplification,
    setTyper[nat._3](partialTyper[nat._3] {
      case Sized(_, s2, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> Type.Null, "right" -> s2), None)
      case Sized(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case Sized(s1, s2, _) => Type.Obj(Map("left" -> (s1 ⨿ Type.Null), "right" -> s2), None)
    }),
    untyper[nat._3](t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        Func.Input3(l, r, Type.Bool))))

  val FullOuterJoin = TernaryFunc(
    Transformation,
    "FULL OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from either set.",
    Type.Top,
    Func.Input3(Type.Top, Type.Top, Type.Bool),
    noSimplification,
    setTyper[nat._3](partialTyper[nat._3] {
      case Sized(Type.Const(Data.Set(Nil)), Type.Const(Data.Set(Nil)), _) =>
        Type.Const(Data.Set(Nil))
      case Sized(s1, s2, _) =>
        Type.Obj(Map("left" -> (s1 ⨿ Type.Null), "right" -> (s2 ⨿ Type.Null)), None)
    }),
    untyper[nat._3](t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        Func.Input3(l, r, Type.Bool))))

  val GroupBy = BinaryFunc(
    Transformation,
    "GROUP BY",
    "Groups a projection of a set by another projection",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(s1, _) => s1
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  val Distinct = UnaryFunc(
    Sifting,
    "DISTINCT",
    "Discards all but the first instance of each unique value",
    Type.Top,
    Func.Input1(Type.Top),
    noSimplification,
    setTyper[nat._1](partialTyper[nat._1] {
      case Sized(a) => a
    }),
    untyper[nat._1](t => success(Func.Input1(t))))

  val DistinctBy = BinaryFunc(
    Sifting,
    "DISTINCT BY",
    "Discards all but the first instance of the first argument, based on uniqueness of the second argument",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(a, _) => a
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  val Union = BinaryFunc(
    Transformation,
    "(UNION ALL)",
    "Creates a new set with all the elements of each input set, keeping duplicates.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(Type.Const(Data.Set(Nil)), s2) => s2
      case Sized(s1, Type.Const(Data.Set(Nil))) => s1
      case Sized(s1, s2)                        => s1 ⨿ s2
    }),
    untyper[nat._2](t => success(Func.Input2(t, t))))

  val Intersect = BinaryFunc(
    Transformation,
    "(INTERSECT ALL)",
    "Creates a new set with only the elements that exist in both input sets, keeping duplicates.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(s1, s2) => if (s1 == s2) s1 else Type.Const(Data.Set(Nil))
    }),
    untyper[nat._2](t => success(Func.Input2(t, t))))

  val Except = BinaryFunc(
    Transformation,
    "(EXCEPT)",
    "Removes the elements of the second set from the first set.",
    Type.Top,
    Func.Input2(Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case InvokeF(_, Sized(Embed(set), Embed(ConstantF(Data.Set(Nil))))) =>
          set.some
        case _ => None
      }
    },
    setTyper[nat._2](partialTyper[nat._2] {
      case Sized(s1, _) => s1
    }),
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  // TODO: Handle “normal” functions without creating Funcs. They should be in
  //       a separate functor and inlined prior to getting this far. It will
  //       also allow us to make simplification non-Corecursive and ∴ operate
  //       on Cofree.
  val In = BinaryFunc(
    Mapping,
    "(in)",
    "Determines whether a value is in a given set.",
    Type.Bool,
    Func.Input2(Type.Top, Type.Top),
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, Sized(item, set)) => set.project match {
            case ConstantF(Data.Set(_)) => Within(item, StructuralLib.UnshiftArray(set).embed).some
            case ConstantF(_)           => RelationsLib.Eq(item, set).some
            case lp                     => Within(item, StructuralLib.UnshiftArray(set).embed).some
          }
          case _ => None
        }
    },
    partialTyper[nat._2] {
      case Sized(_, Type.Const(Data.Set(Nil))) =>
        Type.Const(Data.Bool(false))
      case Sized(Type.Const(x), Type.Const(Data.Set(set))) =>
        Type.Const(Data.Bool(set.contains(x)))
      case Sized(Type.Const(x), Type.Const(y)) =>
        Type.Const(Data.Bool(x == y))
      case Sized(_, _) => Type.Bool
    },
    basicUntyper)

  val Within = BinaryFunc(
    Mapping,
    "within",
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
    Mapping,
    "CONSTANTLY", "Always return the same value",
    Type.Bottom,
    Func.Input2(Type.Top, Type.Top),
    noSimplification,
    partialTyper[nat._2] {
      case Sized(Type.Const(const), Type.Const(Data.Set(s))) =>
        Type.Const(Data.Set(s.map(κ(const))))
      case Sized(const, _) => const
    },
    untyper[nat._2](t => success(Func.Input2(t, Type.Top))))

  def unaryFunctions: List[GenericFunc[nat._1]] =
    Distinct :: Nil

  def binaryFunctions: List[GenericFunc[nat._2]] =
    Take :: Drop :: Filter :: GroupBy :: DistinctBy ::
    Union :: Intersect :: Except :: In :: Within :: Constantly :: Nil

  def ternaryFunctions: List[GenericFunc[nat._3]] =
    OrderBy :: InnerJoin :: LeftOuterJoin :: RightOuterJoin :: FullOuterJoin :: Nil
}

object SetLib extends SetLib
