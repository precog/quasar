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

trait SetLib extends Library {
  // NB: MRA should make this go away, as we insert dimensiality adjustements
  //     in the appropriate places.
  private def setTyper(f: Func.Typer): Func.Typer =
    ts => f(ts).map {
      case x @ Type.Const(Data.Set(_)) => x
      case rez                         => rez
    }

  val Take = Sifting("(LIMIT)", "Takes the first N elements from a set",
    Type.Top, Type.Top :: Type.Int :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case _ :: Type.Const(Data.Int(n)) :: Nil if n == 0 =>
        Type.Const(Data.Set(Nil))
      case Type.Const(Data.Set(s)) :: Type.Const(Data.Int(n)) :: Nil
          if n.isValidInt =>
        Type.Const(Data.Set(s.take(n.intValue)))
      case t :: _ :: Nil => t
    }),
    untyper(t => success(t :: Type.Int :: Nil)))

  val Drop = Sifting("(OFFSET)", "Drops the first N elements from a set",
    Type.Top, Type.Top :: Type.Int :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case IsInvoke(_, List(set, ConstantF(Data.Int(n)))) if n == 0 =>
          set.some
        case _ => None
      }
    },
    setTyper(partialTyper {
      case Type.Const(Data.Set(s)) :: Type.Const(Data.Int(n)) :: Nil
          if n.isValidInt =>
        Type.Const(Data.Set(s.drop(n.intValue)))
      case t :: _ :: Nil => t
    }),
    untyper(t => success(t :: Type.Int :: Nil)))

  val OrderBy = Sifting("ORDER BY", "Orders a set by the natural ordering of a projection on the set",
    Type.Top, Type.Top :: Type.Top :: Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper { case set :: _ :: _ :: Nil => set }),
    untyper(t => success(t :: Type.Top :: Type.Top :: Nil)))

  val Filter = Sifting("WHERE", "Filters a set to include only elements where a projection is true",
    Type.Top, Type.Top :: Type.Bool :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(set, ConstantF(Data.True))) => set.some
          case _                                            => None
        }
    },
    setTyper(partialTyper {
      case _   :: Type.Const(Data.False) :: Nil => Type.Const(Data.Set(Nil))
      case set :: _                      :: Nil => set
    }),
    untyper(t => success(t :: Type.Bool :: Nil)))

  val InnerJoin = Transformation(
    "INNER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition.",
    Type.Top, Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(_, _, Type.Const(Data.Bool(false))) => Type.Const(Data.Set(Nil))
      case List(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case List(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) => Type.Obj(Map("left" -> s1, "right" -> s2), None)
    }),
    untyper(t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        l :: r :: Type.Bool :: Nil)))

  val LeftOuterJoin = Transformation(
    "LEFT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the left set.",
    Type.Top, Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(s1, _, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> s1, "right" -> Type.Null), None)
      case List(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) =>
        Type.Obj(Map("left" -> s1, "right" -> (s2 ⨿ Type.Null)), None)
    }),
    untyper(t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        l :: r :: Type.Bool :: Nil)))

  val RightOuterJoin = Transformation(
    "RIGHT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the right set.",
    Type.Top, Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(_, s2, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> Type.Null, "right" -> s2), None)
      case List(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) => Type.Obj(Map("left" -> (s1 ⨿ Type.Null), "right" -> s2), None)
    }),
    untyper(t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        l :: r :: Type.Bool :: Nil)))

  val FullOuterJoin = Transformation(
    "FULL OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from either set.",
    Type.Top, Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(Type.Const(Data.Set(Nil)), Type.Const(Data.Set(Nil)), _) =>
        Type.Const(Data.Set(Nil))
      case List(s1, s2, _) =>
        Type.Obj(Map("left" -> (s1 ⨿ Type.Null), "right" -> (s2 ⨿ Type.Null)), None)
    }),
    untyper(t =>
      (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))((l, r) =>
        l :: r :: Type.Bool :: Nil)))

  val GroupBy = Transformation("GROUP BY", "Groups a projection of a set by another projection",
    Type.Top, Type.Top :: Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper { case s1 :: _ :: Nil => s1 }),
    untyper(t => success(t :: Type.Top :: Nil)))

  val Distinct = Sifting("DISTINCT", "Discards all but the first instance of each unique value",
    Type.Top, Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper { case a :: Nil => a}),
    untyper(t => success(t :: Nil)))

  val DistinctBy = Sifting("DISTINCT BY", "Discards all but the first instance of the first argument, based on uniqueness of the second argument",
    Type.Top, Type.Top :: Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper { case a :: _ :: Nil => a }),
    untyper(t => success(t :: Type.Top :: Nil)))

  val Union = Transformation("(UNION ALL)",
    "Creates a new set with all the elements of each input set, keeping duplicates.",
    Type.Top, Type.Top :: Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(Type.Const(Data.Set(Nil)), s2) => s2
      case List(s1, Type.Const(Data.Set(Nil))) => s1
      case List(s1, s2)                        => s1 ⨿ s2
    }),
    untyper(t => success(t :: t :: Nil)))

  val Intersect = Transformation("(INTERSECT ALL)",
    "Creates a new set with only the elements that exist in both input sets, keeping duplicates.",
    Type.Top, Type.Top :: Type.Top :: Nil,
    noSimplification,
    setTyper(partialTyper {
      case List(s1, s2) => if (s1 == s2) s1 else Type.Const(Data.Set(Nil))
    }),
    untyper(t => success(t :: t :: Nil)))

  val Except = Transformation("(EXCEPT)",
    "Removes the elements of the second set from the first set.",
    Type.Top, Type.Top :: Type.Top :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) = orig match {
        case IsInvoke(_, List(set, ConstantF(Data.Set(Nil)))) => set.some
        case _                                                => None
      }
    },
    setTyper(partialTyper { case List(s1, _) => s1 }),
    untyper(t => success(t :: Type.Top :: Nil)))

  // TODO: Handle “normal” functions without creating Funcs. They should be in
  //       a separate functor and inlined prior to getting this far. It will
  //       also allow us to make simplification non-Corecursive and ∴ operate
  //       on Cofree.
  val In = Mapping(
    "(in)",
    "Determines whether a value is in a given set.",
    Type.Bool, Type.Top :: Type.Top :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case InvokeF(_, List(item, set)) => set.project match {
            case ConstantF(Data.Set(_)) => Within(item, StructuralLib.UnshiftArray(set).embed).some
            case ConstantF(_)           => RelationsLib.Eq(item, set).some
            case lp                     => Within(item, StructuralLib.UnshiftArray(set).embed).some
          }
          case _ => None
        }
    },
    partialTyper {
      case List(_,             Type.Const(Data.Set(Nil))) =>
        Type.Const(Data.Bool(false))
      case List(Type.Const(x), Type.Const(Data.Set(set))) =>
        Type.Const(Data.Bool(set.contains(x)))
      case List(Type.Const(x), Type.Const(y))             =>
        Type.Const(Data.Bool(x == y))
      case List(_,             _)                         => Type.Bool
    },
    basicUntyper)

  val Within = Mapping(
    "within",
    "Determines whether a value is in a given array.",
    Type.Bool, Type.Top :: Type.AnyArray :: Nil,
    noSimplification,
    partialTyper {
      case List(_,             Type.Const(Data.Arr(Nil))) =>
        Type.Const(Data.Bool(false))
      case List(Type.Const(x), Type.Const(Data.Arr(arr))) =>
        Type.Const(Data.Bool(arr.contains(x)))
      case List(_,             _)                         => Type.Bool
    },
    basicUntyper)

  val Constantly = Mapping("CONSTANTLY", "Always return the same value",
    Type.Bottom, Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(const) :: Type.Const(Data.Set(s)) :: Nil =>
        Type.Const(Data.Set(s.map(κ(const))))
      case const :: _ :: Nil => const
    },
    untyper(t => success(t :: Type.Top :: Nil)))

  def functions =
    Take :: Drop :: OrderBy :: Filter ::
      InnerJoin :: LeftOuterJoin :: RightOuterJoin :: FullOuterJoin ::
      GroupBy :: Distinct :: DistinctBy :: Union :: Intersect :: Except ::
      In :: Constantly :: Nil
}
object SetLib extends SetLib
