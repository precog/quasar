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

package quasar.qscript

import quasar.Predef._
import quasar._, RenderTree.ops._
import quasar.ejson._
import quasar.fp._
import quasar.fp.ski._
import quasar.std.StdLib._, date.TemporalPart

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import monocle.macros.Lenses
import scalaz._, Scalaz._

sealed abstract class MapFunc[T[_[_]], A]

sealed abstract class Nullary[T[_[_]], A] extends MapFunc[T, A]

sealed abstract class Unary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
}
sealed abstract class Binary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
  def a2: A
}
sealed abstract class Ternary[T[_[_]], A] extends MapFunc[T, A] {
  def a1: A
  def a2: A
  def a3: A
}

// TODO all `Free` should be generalized to `T` once we can handle recursive `Free`
object MapFunc {
  import MapFuncs._

  type CoMapFuncR[T[_[_]], A] = CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]]

  def rollMF[T[_[_]], A](mf: MapFunc[T, FreeMapA[T, A]])
      : CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]] =
    CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]](mf.right[A])

  /** Returns a List that maps element-by-element to a MapFunc array. If we
    * can’t statically determine _all_ of the elements, it doesn’t match.
    */
  object StaticArray {
    def unapply[T[_[_]]: BirecursiveT, A](mf: CoMapFuncR[T, A]):
        Option[List[FreeMapA[T, A]]] =
      mf match {
        case ConcatArraysN(as) =>
          as.foldRightM[Option, List[FreeMapA[T, A]]](
            Nil)(
            (mf, acc) => (mf.project.run.toOption collect {
              case MakeArray(value) => (value :: acc)
              case Constant(Embed(ejson.Common(ejson.Arr(values)))) =>
                values.map(v => rollMF[T, A](Constant(v)).embed) ++ acc
            }))
        case _ => None
      }
  }

  /** Like `StaticArray`, but returns as much of the array as can be statically
    * determined. Useful if you just want to statically lookup into an array if
    * possible, and punt otherwise.
    */
  object StaticArrayPrefix {
    def unapply[T[_[_]]: BirecursiveT, A](mf: CoMapFuncR[T, A]):
        Option[List[FreeMapA[T, A]]] =
      mf match {
        case ConcatArraysN(as) =>
          as.foldLeftM[List[FreeMapA[T, A]] \/ ?, List[FreeMapA[T, A]]](
            Nil)(
            (acc, mf) => mf.project.run.fold(
              κ(acc.left),
              _ match {
                case MakeArray(value) => (acc :+ value).right
                case Constant(Embed(ejson.Common(ejson.Arr(values)))) =>
                  (acc ++ values.map(v => rollMF[T, A](Constant(v)).embed)).right
                case _ => acc.left
              })).merge.some
        case _ => None
      }
  }

  object StaticMap {
    def unapply[T[_[_]]: BirecursiveT, A](mf: CoMapFuncR[T, A]):
        Option[List[(T[EJson], FreeMapA[T, A])]] =
      mf match {
        case ConcatMapsN(as) =>
          as.foldRightM[Option, List[(T[EJson], FreeMapA[T, A])]](
            Nil)(
            (mf, acc) => (mf.project.run.toOption >>=
              {
                case MakeMap(Embed(CoEnv(\/-(Constant(k)))), v) => ((k, v) :: acc).some
                case Constant(Embed(ejson.Extension(ejson.Map(kvs)))) =>
                  (kvs.map(_.map(v => rollMF[T, A](Constant(v)).embed)) ++ acc).some
                case _ => None
              }))
        case _ => None
      }
  }

  object EmptyArray {
    def apply[T[_[_]]: CorecursiveT, A]: Constant[T, A] =
      Constant[T, A](EJson.fromCommon[T[EJson]].apply(ejson.Arr[T[EJson]](Nil)))
  }

  // TODO: subtyping is preventing embedding of MapFuncs
  /** This returns the set of expressions that are concatenated together. It can
    * include statically known pieces, like `MakeArray` and `Constant(Arr)`, but
    * also arbitrary expressions that may evaluate to an array of any size.
    */
  object ConcatArraysN {
    def apply[T[_[_]]: BirecursiveT, A](args: List[FreeMapA[T, A]])
        : CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]] = {
      args.toList match {
        case h :: t => t.foldLeft(h)((a, b) => rollMF[T, A](ConcatArrays(a, b)).embed).project
        case Nil    => rollMF[T, A](EmptyArray[T, FreeMapA[T, A]])
      }
    }

    def unapply[T[_[_]]: BirecursiveT, A](mf: CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]]):
        Option[List[FreeMapA[T, A]]] =
      mf.run.fold(
        κ(None),
        {
          case MakeArray(_) | Constant(Embed(ejson.Common(ejson.Arr(_)))) =>
            List(mf.embed).some
          case ConcatArrays(h, t) =>
            (unapply(h.project).getOrElse(List(h)) ++
              unapply(t.project).getOrElse(List(t))).some
          case _ => None
        })

  }

  // TODO subtyping is preventing embeding of MapFuncs
  object ConcatMapsN {
    def apply[T[_[_]]: BirecursiveT, A](args: List[FreeMapA[T, A]])
        : CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]] = {
      args.toList match {
        case h :: t => t.foldLeft(h)((a, b) => rollMF[T, A](ConcatMaps(a, b)).embed).project
        case Nil    => rollMF[T, A](Constant(EJson.fromCommon[T[EJson]].apply(ejson.Arr[T[EJson]](Nil))))
      }
    }

    def unapply[T[_[_]]: BirecursiveT, A](mf: CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]]):
        Option[List[FreeMapA[T, A]]] =
      mf.run.fold(
        κ(None),
        {
          case MakeMap(_, _) | Constant(Embed(ejson.Extension(ejson.Map(_)))) =>
            List(mf.embed).some
          case ConcatMaps(h, t) =>
            (unapply(h.project).getOrElse(List(h)) ++
              unapply(t.project).getOrElse(List(t))).some
          case _ => None
        })
  }

  // Transform effectively constant `MapFunc` into a `Constant` value.
  // This is a mini-evaluator for constant qscript values.
  def foldConstant[T[_[_]]: BirecursiveT, A]
    (implicit C: ejson.Common :<: ejson.EJson, E: ejson.Extension :<: ejson.EJson)
      : CoMapFuncR[T, A] => Option[T[EJson]] = {
    object EjConstCommon {
      def unapply[B](tco: FreeMapA[T, B]): Option[ejson.Common[T[EJson]]] =
        tco match {
          case Embed(CoEnv(\/-(Constant(Embed(ejson.Common(v)))))) => Some(v)
          case _                                                   => None
        }
    }

    _.run.fold[Option[ejson.EJson[T[ejson.EJson]]]](
      κ(None),
      {
        // relations
        case And(EjConstCommon(ejson.Bool(v1)), EjConstCommon(ejson.Bool(v2))) =>
          C.inj(ejson.Bool(v1 && v2)).some
        case Or(EjConstCommon(ejson.Bool(v1)), EjConstCommon(ejson.Bool(v2)))  =>
          C.inj(ejson.Bool(v1 || v2)).some
        case Not(EjConstCommon(ejson.Bool(v1)))                                =>
          C.inj(ejson.Bool(!v1)).some

        // string
        case Lower(EjConstCommon(ejson.Str(v1))) =>
          C.inj(ejson.Str(v1.toLowerCase)).some
        case Upper(EjConstCommon(ejson.Str(v1))) =>
          C.inj(ejson.Str(v1.toUpperCase)).some

        // structural
        case MakeArray(Embed(CoEnv(\/-(Constant(v1)))))                               =>
          C.inj(ejson.Arr(List(v1))).some
        case MakeMap(EjConstCommon(ejson.Str(v1)), Embed(CoEnv(\/-(Constant(v2)))))   =>
          E.inj(ejson.Map(List(C.inj(ejson.Str[T[ejson.EJson]](v1)).embed -> v2))).some
        case ConcatArrays(EjConstCommon(ejson.Arr(v1)), EjConstCommon(ejson.Arr(v2))) =>
          C.inj(ejson.Arr(v1 ++ v2)).some
        case _ => None
      }) ∘ (_.embed)
  }

  def normalize[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Show]
      : CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]] => CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]] =
    repeatedly(rewrite[T, A]) ⋘
      orOriginal(foldConstant[T, A].apply(_) ∘ (const => rollMF[T, A](Constant(const))))

  // TODO: This could be split up as it is in LP, with each function containing
  //       its own normalization.
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT, A: Show]:
      CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] = {
    _.run.fold(
      κ(None),
      {
        case Eq(Embed(CoEnv(\/-(Constant(v1)))), Embed(CoEnv(\/-(Constant(v2))))) =>
          rollMF[T, A](
            Constant(EJson.fromCommon[T[EJson]].apply(
              ejson.Bool[T[EJson]](v1 ≟ v2)))).some

        case ProjectIndex(Embed(StaticArrayPrefix(as)), Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Int(index)))))))) =>
          if (index.isValidInt)
            as.lift(index.intValue).map(_.project)
          else None

        case ProjectField(Embed(ConcatMapsN(as)), Embed(CoEnv(\/-(Constant(field))))) =>
          as.collectFirst {
            // TODO: Perhaps we could have an extractor so these could be
            //       handled by the same case
            case Embed(CoEnv(\/-(MakeMap(Embed(CoEnv(\/-(Constant(src)))), Embed(value))))) if field ≟ src =>
              value
            case Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Map(m))))))) =>
              m.find {
                case (k, v) => k ≟ field
              }.map(p => rollMF[T, A](Constant(p._2))).get
          }

        // elide Nil array on the left
        case ConcatArrays(
          Embed(CoEnv(\/-(Constant(Embed(ejson.Common(ejson.Arr(Nil))))))),
          Embed(CoEnv(\/-(rhs)))) =>
            rollMF[T, A](rhs).some

        // elide Nil array on the right
        case ConcatArrays(
          Embed(CoEnv(\/-(lhs))),
          Embed(CoEnv(\/-(Constant(Embed(ejson.Common(ejson.Arr(Nil)))))))) =>
            rollMF[T, A](lhs).some

        // elide Nil map on the left
        case ConcatMaps(
          Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Map(Nil))))))),
          Embed(CoEnv(\/-(rhs)))) =>
            rollMF[T, A](rhs).some

        // elide Nil map on the right
        case ConcatMaps(
          Embed(CoEnv(\/-(lhs))),
          Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Map(Nil)))))))) =>
            rollMF[T, A](lhs).some

        case _ => None
      })
  }

  implicit def traverse[T[_[_]]]: Traverse[MapFunc[T, ?]] =
    new Traverse[MapFunc[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: MapFunc[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[MapFunc[T, B]] = fa match {
        // nullary
        case Constant(v) => G.point(Constant[T, B](v))
        case Undefined() => G.point(Undefined[T, B]())
        case Now() => G.point(Now[T, B]())

        // unary
        case ExtractCentury(a1) => f(a1) ∘ (ExtractCentury(_))
        case ExtractDayOfMonth(a1) => f(a1) ∘ (ExtractDayOfMonth(_))
        case ExtractDecade(a1) => f(a1) ∘ (ExtractDecade(_))
        case ExtractDayOfWeek(a1) => f(a1) ∘ (ExtractDayOfWeek(_))
        case ExtractDayOfYear(a1) => f(a1) ∘ (ExtractDayOfYear(_))
        case ExtractEpoch(a1) => f(a1) ∘ (ExtractEpoch(_))
        case ExtractHour(a1) => f(a1) ∘ (ExtractHour(_))
        case ExtractIsoDayOfWeek(a1) => f(a1) ∘ (ExtractIsoDayOfWeek(_))
        case ExtractIsoYear(a1) => f(a1) ∘ (ExtractIsoYear(_))
        case ExtractMicroseconds(a1) => f(a1) ∘ (ExtractMicroseconds(_))
        case ExtractMillennium(a1) => f(a1) ∘ (ExtractMillennium(_))
        case ExtractMilliseconds(a1) => f(a1) ∘ (ExtractMilliseconds(_))
        case ExtractMinute(a1) => f(a1) ∘ (ExtractMinute(_))
        case ExtractMonth(a1) => f(a1) ∘ (ExtractMonth(_))
        case ExtractQuarter(a1) => f(a1) ∘ (ExtractQuarter(_))
        case ExtractSecond(a1) => f(a1) ∘ (ExtractSecond(_))
        case ExtractTimezone(a1) => f(a1) ∘ (ExtractTimezone(_))
        case ExtractTimezoneHour(a1) => f(a1) ∘ (ExtractTimezoneHour(_))
        case ExtractTimezoneMinute(a1) => f(a1) ∘ (ExtractTimezoneMinute(_))
        case ExtractWeek(a1) => f(a1) ∘ (ExtractWeek(_))
        case ExtractYear(a1) => f(a1) ∘ (ExtractYear(_))
        case Date(a1) => f(a1) ∘ (Date(_))
        case Time(a1) => f(a1) ∘ (Time(_))
        case Timestamp(a1) => f(a1) ∘ (Timestamp(_))
        case Interval(a1) => f(a1) ∘ (Interval(_))
        case StartOfDay(a1) => f(a1) ∘ (StartOfDay(_))
        case TemporalTrunc(a1, a2) => f(a2) ∘ (TemporalTrunc(a1, _))
        case TimeOfDay(a1) => f(a1) ∘ (TimeOfDay(_))
        case ToTimestamp(a1) => f(a1) ∘ (ToTimestamp(_))
        case TypeOf(a1) => f(a1) ∘ (TypeOf(_))
        case Negate(a1) => f(a1) ∘ (Negate(_))
        case Not(a1) => f(a1) ∘ (Not(_))
        case Length(a1) => f(a1) ∘ (Length(_))
        case Lower(a1) => f(a1) ∘ (Lower(_))
        case Upper(a1) => f(a1) ∘ (Upper(_))
        case Bool(a1) => f(a1) ∘ (Bool(_))
        case Integer(a1) => f(a1) ∘ (Integer(_))
        case Decimal(a1) => f(a1) ∘ (Decimal(_))
        case Null(a1) => f(a1) ∘ (Null(_))
        case ToString(a1) => f(a1) ∘ (ToString(_))
        case MakeArray(a1) => f(a1) ∘ (MakeArray(_))
        case Meta(a1) => f(a1) ∘ (Meta(_))

        // binary
        case Add(a1, a2) => (f(a1) ⊛ f(a2))(Add(_, _))
        case Multiply(a1, a2) => (f(a1) ⊛ f(a2))(Multiply(_, _))
        case Subtract(a1, a2) => (f(a1) ⊛ f(a2))(Subtract(_, _))
        case Divide(a1, a2) => (f(a1) ⊛ f(a2))(Divide(_, _))
        case Modulo(a1, a2) => (f(a1) ⊛ f(a2))(Modulo(_, _))
        case Power(a1, a2) => (f(a1) ⊛ f(a2))(Power(_, _))
        case Eq(a1, a2) => (f(a1) ⊛ f(a2))(Eq(_, _))
        case Neq(a1, a2) => (f(a1) ⊛ f(a2))(Neq(_, _))
        case Lt(a1, a2) => (f(a1) ⊛ f(a2))(Lt(_, _))
        case Lte(a1, a2) => (f(a1) ⊛ f(a2))(Lte(_, _))
        case Gt(a1, a2) => (f(a1) ⊛ f(a2))(Gt(_, _))
        case Gte(a1, a2) => (f(a1) ⊛ f(a2))(Gte(_, _))
        case IfUndefined(a1, a2) => (f(a1) ⊛ f(a2))(IfUndefined(_, _))
        case And(a1, a2) => (f(a1) ⊛ f(a2))(And(_, _))
        case Or(a1, a2) => (f(a1) ⊛ f(a2))(Or(_, _))
        case Within(a1, a2) => (f(a1) ⊛ f(a2))(Within(_, _))
        case MakeMap(a1, a2) => (f(a1) ⊛ f(a2))(MakeMap(_, _))
        case ConcatMaps(a1, a2) => (f(a1) ⊛ f(a2))(ConcatMaps(_, _))
        case ProjectIndex(a1, a2) => (f(a1) ⊛ f(a2))(ProjectIndex(_, _))
        case ProjectField(a1, a2) => (f(a1) ⊛ f(a2))(ProjectField(_, _))
        case DeleteField(a1, a2) => (f(a1) ⊛ f(a2))(DeleteField(_, _))
        case ConcatArrays(a1, a2) => (f(a1) ⊛ f(a2))(ConcatArrays(_, _))
        case Range(a1, a2) => (f(a1) ⊛ f(a2))(Range(_, _))

        //  ternary
        case Between(a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(Between(_, _, _))
        case Cond(a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(Cond(_, _, _))
        case Search(a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(Search(_, _, _))
        case Substring(a1, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(Substring(_, _, _))
        case Guard(a1, tpe, a2, a3) => (f(a1) ⊛ f(a2) ⊛ f(a3))(Guard(_, tpe, _, _))
      }
  }

  implicit def equal[T[_[_]]: EqualT, A]: Delay[Equal, MapFunc[T, ?]] =
    new Delay[Equal, MapFunc[T, ?]] {
      def apply[A](in: Equal[A]): Equal[MapFunc[T, A]] = Equal.equal {
        // nullary
        case (Constant(v1), Constant(v2)) => v1.equals(v2)
        case (Undefined(), Undefined()) => true
        case (Now(), Now()) => true

        // unary
        case (ExtractCentury(a1), ExtractCentury(a2)) => in.equal(a1, a2)
        case (ExtractDayOfMonth(a1), ExtractDayOfMonth(a2)) => in.equal(a1, a2)
        case (ExtractDecade(a1), ExtractDecade(a2)) => in.equal(a1, a2)
        case (ExtractDayOfWeek(a1), ExtractDayOfWeek(a2)) => in.equal(a1, a2)
        case (ExtractDayOfYear(a1), ExtractDayOfYear(a2)) => in.equal(a1, a2)
        case (ExtractEpoch(a1), ExtractEpoch(a2)) => in.equal(a1, a2)
        case (ExtractHour(a1), ExtractHour(a2)) => in.equal(a1, a2)
        case (ExtractIsoDayOfWeek(a1), ExtractIsoDayOfWeek(a2)) => in.equal(a1, a2)
        case (ExtractIsoYear(a1), ExtractIsoYear(a2)) => in.equal(a1, a2)
        case (ExtractMicroseconds(a1), ExtractMicroseconds(a2)) => in.equal(a1, a2)
        case (ExtractMillennium(a1), ExtractMillennium(a2)) => in.equal(a1, a2)
        case (ExtractMilliseconds(a1), ExtractMilliseconds(a2)) => in.equal(a1, a2)
        case (ExtractMinute(a1), ExtractMinute(a2)) => in.equal(a1, a2)
        case (ExtractMonth(a1), ExtractMonth(a2)) => in.equal(a1, a2)
        case (ExtractQuarter(a1), ExtractQuarter(a2)) => in.equal(a1, a2)
        case (ExtractSecond(a1), ExtractSecond(a2)) => in.equal(a1, a2)
        case (ExtractTimezone(a1), ExtractTimezone(a2)) => in.equal(a1, a2)
        case (ExtractTimezoneHour(a1), ExtractTimezoneHour(a2)) => in.equal(a1, a2)
        case (ExtractTimezoneMinute(a1), ExtractTimezoneMinute(a2)) => in.equal(a1, a2)
        case (ExtractWeek(a1), ExtractWeek(a2)) => in.equal(a1, a2)
        case (ExtractYear(a1), ExtractYear(a2)) => in.equal(a1, a2)
        case (Date(a1), Date(b1)) => in.equal(a1, b1)
        case (Time(a1), Time(b1)) => in.equal(a1, b1)
        case (Timestamp(a1), Timestamp(b1)) => in.equal(a1, b1)
        case (Interval(a1), Interval(b1)) => in.equal(a1, b1)
        case (StartOfDay(a1), StartOfDay(b1)) => in.equal(a1, b1)
        case (TemporalTrunc(a1, a2), TemporalTrunc(b1, b2)) => a1 ≟ b1 && in.equal(a2, b2)
        case (TimeOfDay(a1), TimeOfDay(b1)) => in.equal(a1, b1)
        case (ToTimestamp(a1), ToTimestamp(b1)) => in.equal(a1, b1)
        case (TypeOf(a1), TypeOf(b1)) => in.equal(a1, b1)
        case (Negate(a1), Negate(b1)) => in.equal(a1, b1)
        case (Not(a1), Not(b1)) => in.equal(a1, b1)
        case (Length(a1), Length(b1)) => in.equal(a1, b1)
        case (Lower(a1), Lower(b1)) => in.equal(a1, b1)
        case (Upper(a1), Upper(b1)) => in.equal(a1, b1)
        case (Bool(a1), Bool(b1)) => in.equal(a1, b1)
        case (Integer(a1), Integer(b1)) => in.equal(a1, b1)
        case (Decimal(a1), Decimal(b1)) => in.equal(a1, b1)
        case (Null(a1), Null(b1)) => in.equal(a1, b1)
        case (ToString(a1), ToString(b1)) => in.equal(a1, b1)
        case (MakeArray(a1), MakeArray(b1)) => in.equal(a1, b1)
        case (Meta(a1), Meta(b1)) => in.equal(a1, b1)

        //  binary
        case (Add(a1, a2), Add(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Multiply(a1, a2), Multiply(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Subtract(a1, a2), Subtract(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Divide(a1, a2), Divide(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Modulo(a1, a2), Modulo(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Power(a1, a2), Power(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Eq(a1, a2), Eq(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Neq(a1, a2), Neq(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Lt(a1, a2), Lt(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Lte(a1, a2), Lte(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Gt(a1, a2), Gt(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Gte(a1, a2), Gte(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (IfUndefined(a1, a2), IfUndefined(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (And(a1, a2), And(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Or(a1, a2), Or(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Within(a1, a2), Within(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (MakeMap(a1, a2), MakeMap(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (ConcatMaps(a1, a2), ConcatMaps(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (ProjectIndex(a1, a2), ProjectIndex(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (ProjectField(a1, a2), ProjectField(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (DeleteField(a1, a2), DeleteField(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (ConcatArrays(a1, a2), ConcatArrays(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
        case (Range(a1, a2), Range(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)

        //  ternary
        case (Between(a1, a2, a3), Between(b1, b2, b3)) => in.equal(a1, b1) && in.equal(a2, b2) && in.equal(a3, b3)
        case (Cond(a1, a2, a3), Cond(b1, b2, b3)) => in.equal(a1, b1) && in.equal(a2, b2) && in.equal(a3, b3)
        case (Search(a1, a2, a3), Search(b1, b2, b3)) => in.equal(a1, b1) && in.equal(a2, b2) && in.equal(a3, b3)
        case (Substring(a1, a2, a3), Substring(b1, b2, b3)) => in.equal(a1, b1) && in.equal(a2, b2) && in.equal(a3, b3)
        case (Guard(a1, atpe, a2, a3), Guard(b1, btpe, b2, b3)) => atpe ≟ btpe && in.equal(a1, b1) && in.equal(a2, b2) && in.equal(a3, b3)

        case (_, _) => false
      }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, MapFunc[T, ?]] =
    new Delay[Show, MapFunc[T, ?]] {
      def apply[A](sh: Show[A]): Show[MapFunc[T, A]] = {
        def shz(label: String, a: A*) =
          Cord(label) ++ Cord("(") ++ a.map(sh.show).toList.intercalate(Cord(", ")) ++ Cord(")")

        Show.show {
          // nullary
          case Constant(v) => Cord("Constant(") ++ v.show ++ Cord(")")
          case Undefined() => Cord("Undefined()")
          case Now() => Cord("Now()")

          // unary
          case ExtractCentury(a1) => shz("ExtractCentury", a1)
          case ExtractDayOfMonth(a1) => shz("ExtractDayOfMonth", a1)
          case ExtractDecade(a1) => shz("ExtractDecade", a1)
          case ExtractDayOfWeek(a1) => shz("ExtractDayOfWeek", a1)
          case ExtractDayOfYear(a1) => shz("ExtractDayOfYear", a1)
          case ExtractEpoch(a1) => shz("ExtractEpoch", a1)
          case ExtractHour(a1) => shz("ExtractHour", a1)
          case ExtractIsoDayOfWeek(a1) => shz("ExtractIsoDayOfWeek", a1)
          case ExtractIsoYear(a1) => shz("ExtractIsoYear", a1)
          case ExtractMicroseconds(a1) => shz("ExtractMicroseconds", a1)
          case ExtractMillennium(a1) => shz("ExtractMillennium", a1)
          case ExtractMilliseconds(a1) => shz("ExtractMilliseconds", a1)
          case ExtractMinute(a1) => shz("ExtractMinute", a1)
          case ExtractMonth(a1) => shz("ExtractMonth", a1)
          case ExtractQuarter(a1) => shz("ExtractQuarter", a1)
          case ExtractSecond(a1) => shz("ExtractSecond", a1)
          case ExtractTimezone(a1) => shz("ExtractTimezone", a1)
          case ExtractTimezoneHour(a1) => shz("ExtractTimezoneHour", a1)
          case ExtractTimezoneMinute(a1) => shz("ExtractTimezoneMinute", a1)
          case ExtractWeek(a1) => shz("ExtractWeek", a1)
          case ExtractYear(a1) => shz("ExtractYear", a1)
          case Date(a1) => shz("Date", a1)
          case Time(a1) => shz("Time", a1)
          case Timestamp(a1) => shz("Timestamp", a1)
          case Interval(a1) => shz("Interval", a1)
          case StartOfDay(a1) => shz("StartOfDay", a1)
          case TemporalTrunc(a1, a2) => Cord("TemporalTrunc(", a1.show, ", ", sh.show(a2), ")")
          case TimeOfDay(a1) => shz("TimeOfDay", a1)
          case ToTimestamp(a1) => shz("ToTimestamp", a1)
          case TypeOf(a1) => shz("TypeOf", a1)
          case Negate(a1) => shz("Negate", a1)
          case Not(a1) => shz("Not", a1)
          case Length(a1) => shz("Length", a1)
          case Lower(a1) => shz("Lower", a1)
          case Upper(a1) => shz("Upper", a1)
          case Bool(a1) => shz("Bool", a1)
          case Integer(a1) => shz("Integer", a1)
          case Decimal(a1) => shz("Decimal", a1)
          case Null(a1) => shz("Null", a1)
          case ToString(a1) => shz("ToString", a1)
          case MakeArray(a1) => shz("MakeArray", a1)
          case Meta(a1) => shz("Meta", a1)

          // binary
          case Add(a1, a2) => shz("Add", a1, a2)
          case Multiply(a1, a2) => shz("Multiply", a1, a2)
          case Subtract(a1, a2) => shz("Subtract", a1, a2)
          case Divide(a1, a2) => shz("Divide", a1, a2)
          case Modulo(a1, a2) => shz("Modulo", a1, a2)
          case Power(a1, a2) => shz("Power", a1, a2)
          case Eq(a1, a2) => shz("Eq", a1, a2)
          case Neq(a1, a2) => shz("Neq", a1, a2)
          case Lt(a1, a2) => shz("Lt", a1, a2)
          case Lte(a1, a2) => shz("Lte", a1, a2)
          case Gt(a1, a2) => shz("Gt", a1, a2)
          case Gte(a1, a2) => shz("Gte", a1, a2)
          case IfUndefined(a1, a2) => shz("IfUndefined", a1, a2)
          case And(a1, a2) => shz("And", a1, a2)
          case Or(a1, a2) => shz("Or", a1, a2)
          case Within(a1, a2) => shz("Within", a1, a2)
          case MakeMap(a1, a2) => shz("MakeMap", a1, a2)
          case ConcatMaps(a1, a2) => shz("ConcatMaps", a1, a2)
          case ProjectIndex(a1, a2) => shz("ProjectIndex", a1, a2)
          case ProjectField(a1, a2) => shz("ProjectField", a1, a2)
          case DeleteField(a1, a2) => shz("DeleteField", a1, a2)
          case ConcatArrays(a1, a2) => shz("ConcatArrays", a1, a2)
          case Range(a1, a2) => shz("Range", a1, a2)

          //  ternary
          case Between(a1, a2, a3) => shz("Between", a1, a2, a3)
          case Cond(a1, a2, a3) => shz("Cond", a1, a2, a3)
          case Search(a1, a2, a3) => shz("Search", a1, a2, a3)
          case Substring(a1, a2, a3) => shz("Substring", a1, a2, a3)
          case Guard(a1, tpe, a2, a3) =>
            Cord("Guard(") ++
              sh.show(a1) ++ Cord(", ") ++
              tpe.show ++ Cord(", ") ++
              sh.show(a2) ++ Cord(", ") ++
              sh.show(a3) ++ Cord(")")
        }
      }
    }

  // TODO: replace this with some kind of pretty-printing based on a syntax for
  // MapFunc + EJson.
  implicit def renderTree[T[_[_]]: ShowT]: Delay[RenderTree, MapFunc[T, ?]] =
    new Delay[RenderTree, MapFunc[T, ?]] {
      val nt = "MapFunc" :: Nil

      @SuppressWarnings(Array("org.wartremover.warts.ToString"))
      def apply[A](r: RenderTree[A]): RenderTree[MapFunc[T, A]] = {
        def nAry(typ: String, as: A*): RenderedTree =
          NonTerminal(typ :: nt, None, as.toList.map(r.render(_)))

        RenderTree.make {
          // nullary
          case Constant(a1) => Terminal("Constant" :: nt, a1.shows.some)
          case Undefined() => Terminal("Undefined" :: nt, None)
          case Now() => Terminal("Now" :: nt, None)

          // unary
          case ExtractCentury(a1) => nAry("ExtractCentury", a1)
          case ExtractDayOfMonth(a1) => nAry("ExtractDayOfMonth", a1)
          case ExtractDecade(a1) => nAry("ExtractDecade", a1)
          case ExtractDayOfWeek(a1) => nAry("ExtractDayOfWeek", a1)
          case ExtractDayOfYear(a1) => nAry("ExtractDayOfYear", a1)
          case ExtractEpoch(a1) => nAry("ExtractEpoch", a1)
          case ExtractHour(a1) => nAry("ExtractHour", a1)
          case ExtractIsoDayOfWeek(a1) => nAry("ExtractIsoDayOfWeek", a1)
          case ExtractIsoYear(a1) => nAry("ExtractIsoYear", a1)
          case ExtractMicroseconds(a1) => nAry("ExtractMicroseconds", a1)
          case ExtractMillennium(a1) => nAry("ExtractMillennium", a1)
          case ExtractMilliseconds(a1) => nAry("ExtractMilliseconds", a1)
          case ExtractMinute(a1) => nAry("ExtractMinute", a1)
          case ExtractMonth(a1) => nAry("ExtractMonth", a1)
          case ExtractQuarter(a1) => nAry("ExtractQuarter", a1)
          case ExtractSecond(a1) => nAry("ExtractSecond", a1)
          case ExtractTimezone(a1) => nAry("ExtractTimezone", a1)
          case ExtractTimezoneHour(a1) => nAry("ExtractTimezoneHour", a1)
          case ExtractTimezoneMinute(a1) => nAry("ExtractTimezoneMinute", a1)
          case ExtractWeek(a1) => nAry("ExtractWeek", a1)
          case ExtractYear(a1) => nAry("ExtractYear", a1)
          case Date(a1) => nAry("Date", a1)
          case Time(a1) => nAry("Time", a1)
          case Timestamp(a1) => nAry("Timestamp", a1)
          case Interval(a1) => nAry("Interval", a1)
          case StartOfDay(a1) => nAry("StartOfDay", a1)
          case TemporalTrunc(a1, a2) => NonTerminal("TemporalTrunc" :: nt, a1.shows.some, List(r.render(a2)))
          case TimeOfDay(a1) => nAry("TimeOfDay", a1)
          case ToTimestamp(a1) => nAry("ToTimestamp", a1)
          case TypeOf(a1) => nAry("TypeOf", a1)
          case Negate(a1) => nAry("Negate", a1)
          case Not(a1) => nAry("Not", a1)
          case Length(a1) => nAry("Length", a1)
          case Lower(a1) => nAry("Lower", a1)
          case Upper(a1) => nAry("Upper", a1)
          case Bool(a1) => nAry("Bool", a1)
          case Integer(a1) => nAry("Integer", a1)
          case Decimal(a1) => nAry("Decimal", a1)
          case Null(a1) => nAry("Null", a1)
          case ToString(a1) => nAry("ToString", a1)
          case MakeArray(a1) => nAry("MakeArray", a1)
          case Meta(a1) => nAry("Meta", a1)

          // binary
          case Add(a1, a2) => nAry("Add", a1, a2)
          case Multiply(a1, a2) => nAry("Multiply", a1, a2)
          case Subtract(a1, a2) => nAry("Subtract", a1, a2)
          case Divide(a1, a2) => nAry("Divide", a1, a2)
          case Modulo(a1, a2) => nAry("Modulo", a1, a2)
          case Power(a1, a2) => nAry("Power", a1, a2)
          case Eq(a1, a2) => nAry("Eq", a1, a2)
          case Neq(a1, a2) => nAry("Neq", a1, a2)
          case Lt(a1, a2) => nAry("Lt", a1, a2)
          case Lte(a1, a2) => nAry("Lte", a1, a2)
          case Gt(a1, a2) => nAry("Gt", a1, a2)
          case Gte(a1, a2) => nAry("Gte", a1, a2)
          case IfUndefined(a1, a2) => nAry("IfUndefined", a1, a2)
          case And(a1, a2) => nAry("And", a1, a2)
          case Or(a1, a2) => nAry("Or", a1, a2)
          case Within(a1, a2) => nAry("Within", a1, a2)
          case MakeMap(a1, a2) => nAry("MakeMap", a1, a2)
          case ConcatMaps(a1, a2) => nAry("ConcatMaps", a1, a2)
          case ProjectIndex(a1, a2) => nAry("ProjectIndex", a1, a2)
          case ProjectField(a1, a2) => nAry("ProjectField", a1, a2)
          case DeleteField(a1, a2) => nAry("DeleteField", a1, a2)
          case ConcatArrays(a1, a2) => nAry("ConcatArrays", a1, a2)
          case Range(a1, a2) => nAry("Range", a1, a2)

          //  ternary
          case Between(a1, a2, a3) => nAry("Between", a1, a2, a3)
          case Cond(a1, a2, a3) => nAry("Cond", a1, a2, a3)
          case Search(a1, a2, a3) => nAry("Search", a1, a2, a3)
          case Substring(a1, a2, a3) => nAry("Substring", a1, a2, a3)
          case Guard(a1, tpe, a2, a3) => NonTerminal("Guard" :: nt, None,
            List(r.render(a1), tpe.render, r.render(a2), r.render(a3)))
        }
      }
    }

  def translateNullaryMapping[T[_[_]], A]: NullaryFunc => MapFunc[T, A] = {
    case date.Now => Now()
  }

  def translateUnaryMapping[T[_[_]], A]: UnaryFunc => A => MapFunc[T, A] = {
    case date.ExtractCentury => ExtractCentury(_)
    case date.ExtractDayOfMonth => ExtractDayOfMonth(_)
    case date.ExtractDecade => ExtractDecade(_)
    case date.ExtractDayOfWeek => ExtractDayOfWeek(_)
    case date.ExtractDayOfYear => ExtractDayOfYear(_)
    case date.ExtractEpoch => ExtractEpoch(_)
    case date.ExtractHour => ExtractHour(_)
    case date.ExtractIsoDayOfWeek => ExtractIsoDayOfWeek(_)
    case date.ExtractIsoYear => ExtractIsoYear(_)
    case date.ExtractMicroseconds => ExtractMicroseconds(_)
    case date.ExtractMillennium => ExtractMillennium(_)
    case date.ExtractMilliseconds => ExtractMilliseconds(_)
    case date.ExtractMinute => ExtractMinute(_)
    case date.ExtractMonth => ExtractMonth(_)
    case date.ExtractQuarter => ExtractQuarter(_)
    case date.ExtractSecond => ExtractSecond(_)
    case date.ExtractTimezone => ExtractTimezone(_)
    case date.ExtractTimezoneHour => ExtractTimezoneHour(_)
    case date.ExtractTimezoneMinute => ExtractTimezoneMinute(_)
    case date.ExtractWeek => ExtractWeek(_)
    case date.ExtractYear => ExtractYear(_)
    case date.Date => Date(_)
    case date.Time => Time(_)
    case date.Timestamp => Timestamp(_)
    case date.Interval => Interval(_)
    case date.StartOfDay => StartOfDay(_)
    case date.TimeOfDay => TimeOfDay(_)
    case date.ToTimestamp => ToTimestamp(_)
    case identity.TypeOf => TypeOf(_)
    case math.Negate => Negate(_)
    case relations.Not => Not(_)
    case string.Length => Length(_)
    case string.Lower => Lower(_)
    case string.Upper => Upper(_)
    case string.Boolean => Bool(_)
    case string.Integer => Integer(_)
    case string.Decimal => Decimal(_)
    case string.Null => Null(_)
    case string.ToString => ToString(_)
    case structural.MakeArray => MakeArray(_)
    case structural.Meta => Meta(_)
  }

  def translateBinaryMapping[T[_[_]], A]
      : BinaryFunc => (A, A) => MapFunc[T, A] = {
    // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
    //     about the second. And it shouldn’t even have two in LP.
    case array.ArrayLength => (a, b) => Length(a)
    case math.Add => Add(_, _)
    case math.Multiply => Multiply(_, _)
    case math.Subtract => Subtract(_, _)
    case math.Divide => Divide(_, _)
    case math.Modulo => Modulo(_, _)
    case math.Power => Power(_, _)
    case relations.Eq => Eq(_, _)
    case relations.Neq => Neq(_, _)
    case relations.Lt => Lt(_, _)
    case relations.Lte => Lte(_, _)
    case relations.Gt => Gt(_, _)
    case relations.Gte => Gte(_, _)
    case relations.IfUndefined => IfUndefined(_, _)
    case relations.And => And(_, _)
    case relations.Or => Or(_, _)
    case set.Within => Within(_, _)
    case structural.MakeObject => MakeMap(_, _)
    case structural.ObjectConcat => ConcatMaps(_, _)
    case structural.ArrayProject => ProjectIndex(_, _)
    case structural.ObjectProject => ProjectField(_, _)
    case structural.DeleteField => DeleteField(_, _)
    case string.Concat
       | structural.ArrayConcat
       | structural.ConcatOp => ConcatArrays(_, _)
  }

  def translateTernaryMapping[T[_[_]], A]
      : TernaryFunc => (A, A, A) => MapFunc[T, A] = {
    case relations.Between => Between(_, _, _)
    case relations.Cond    => Cond(_, _, _)
    case string.Search     => Search(_, _, _)
    case string.Substring  => Substring(_, _, _)
  }
}

// TODO we should statically verify that these have a `DimensionalEffect` of `Mapping`
object MapFuncs {
  // nullary
  /** A value that is statically known.
    */
  @Lenses final case class Constant[T[_[_]], A](ejson: T[EJson]) extends Nullary[T, A]
  /** A value that doesn’t exist. Most operations on `Undefined` should evaluate
    * to `Undefined`. See [[IfUndefined]] for the exception.
    */
  @Lenses final case class Undefined[T[_[_]], A]() extends Nullary[T, A]

  // array
  @Lenses final case class Length[T[_[_]], A](a1: A) extends Unary[T, A]

  // date
  // See https://www.postgresql.org/docs/9.2/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
  @Lenses final case class ExtractCentury[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractDayOfMonth[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractDecade[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractDayOfWeek[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractDayOfYear[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractEpoch[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractHour[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractIsoDayOfWeek[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractIsoYear[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractMicroseconds[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractMillennium[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractMilliseconds[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractMinute[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractMonth[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractQuarter[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractSecond[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractTimezone[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractTimezoneHour[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractTimezoneMinute[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractWeek[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ExtractYear[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Date[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Time[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Timestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Interval[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class StartOfDay[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class TemporalTrunc[T[_[_]], A](part: TemporalPart, a1: A) extends Unary[T, A]
  @Lenses final case class TimeOfDay[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ToTimestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  /** Fetches the [[quasar.Type.Timestamp]] for the current instant in time. */
  @Lenses final case class Now[T[_[_]], A]() extends Nullary[T, A]

  // identity
  /** Returns a string describing the type of the value. If the value has a
    * metadata map containing an "_ejson.type" entry, that value is returned.
    * Otherwise, it returns a string naming a [[quasar.common.PrimaryType]].
    */
  @Lenses final case class TypeOf[T[_[_]], A](a1: A) extends Unary[T, A]

  // math
  @Lenses final case class Negate[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Add[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Multiply[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Subtract[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Divide[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Modulo[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Power[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // relations
  @Lenses final case class Not[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Eq[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Neq[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Lt[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Lte[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Gt[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Gte[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  /** This “catches” [[Undefined]] values and replaces them with a value.
    */
  @Lenses final case class IfUndefined[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class And[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Or[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class Between[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]
  @Lenses final case class Cond[T[_[_]], A](cond: A, then_ : A, else_ : A) extends Ternary[T, A] {
    def a1 = cond
    def a2 = then_
    def a3 = else_
  }

  // set
  @Lenses final case class Within[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]

  // string
  @Lenses final case class Lower[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Upper[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Bool[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Integer[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Decimal[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Null[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ToString[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Search[T[_[_]], A](a1: A, a2: A, a3: A) extends Ternary[T, A]
  @Lenses final case class Substring[T[_[_]], A](string: A, from: A, count: A) extends Ternary[T, A] {
    def a1 = string
    def a2 = from
    def a3 = count
  }

  // structural

  /** Makes a single-element [[ejson.Arr]] containing `a1`.
    */
  @Lenses final case class MakeArray[T[_[_]], A](a1: A) extends Unary[T, A]
  /** Makes a single-element [[ejson.Map]] with key `key` and value `value`.
    */
  @Lenses final case class MakeMap[T[_[_]], A](key: A, value: A) extends Binary[T, A] {
    def a1 = key
    def a2 = value
  }
  @Lenses final case class ConcatArrays[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class ConcatMaps[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  @Lenses final case class ProjectIndex[T[_[_]], A](src: A, index: A) extends Binary[T, A] {
    def a1 = src
    def a2 = index
  }
  @Lenses final case class ProjectField[T[_[_]], A](src: A, field: A) extends Binary[T, A] {
    def a1 = src
    def a2 = field
  }
  @Lenses final case class DeleteField[T[_[_]], A](src: A, field: A) extends Binary[T, A] {
    def a1 = src
    def a2 = field
  }
  @Lenses final case class Meta[T[_[_]], A](a1: A) extends Unary[T, A]

  @Lenses final case class Range[T[_[_]], A](from: A, to: A) extends Binary[T, A] {
    def a1 = from
    def a2 = to
  }

  /** A conditional specifically for checking that `a1` satisfies `pattern`.
    */
  @Lenses final case class Guard[T[_[_]], A](a1: A, pattern: Type, a2: A, a3: A)
      extends Ternary[T, A]

  object NullLit {
    def apply[T[_[_]]: CorecursiveT, A](): FreeMapA[T, A] =
      Free.roll(Constant[T, FreeMapA[T, A]](EJson.fromCommon[T[EJson]].apply(ejson.Null[T[EJson]]())))

    def unapply[T[_[_]]: RecursiveT, A](mf: FreeMapA[T, A]): Boolean = mf.resume.fold ({
      case Constant(ej) => EJson.isNull(ej)
      case _ => false
    }, _ => false)
  }

  object BoolLit {
    def apply[T[_[_]]: CorecursiveT, A](b: Boolean): FreeMapA[T, A] =
      Free.roll(Constant[T, FreeMapA[T, A]](EJson.fromCommon[T[EJson]].apply(ejson.Bool[T[EJson]](b))))

    def unapply[T[_[_]]: RecursiveT, A](mf: FreeMapA[T, A]): Option[Boolean] = mf.resume.fold ({
      case Constant(ej) => CommonEJson.prj(ej.project).flatMap {
        case ejson.Bool(b) => b.some
        case _ => None
      }
      case _ => None
    }, _ => None)
  }

  object DecLit {
    def apply[T[_[_]]: CorecursiveT, A](d: BigDecimal): FreeMapA[T, A] =
      Free.roll(Constant[T, FreeMapA[T, A]](EJson.fromCommon[T[EJson]].apply(ejson.Dec[T[EJson]](d))))
  }

  object IntLit {
    def apply[T[_[_]]: CorecursiveT, A](i: BigInt): FreeMapA[T, A] =
      Free.roll(Constant[T, FreeMapA[T, A]](EJson.fromExt[T[EJson]].apply(ejson.Int[T[EJson]](i))))

    def unapply[T[_[_]]: RecursiveT, A](mf: FreeMapA[T, A]): Option[BigInt] =
      mf.resume.fold(IntLitMapFunc.unapply(_), _ => None)
  }

  object IntLitMapFunc {
    def unapply[T[_[_]]: RecursiveT, A](mf: MapFunc[T, A]): Option[BigInt] = mf match {
      case Constant(ej) => ExtEJson.prj(ej.project).flatMap {
        case ejson.Int(i) => i.some
        case _ => None
      }
      case _ => None
    }
  }

  object StrLit {
    def apply[T[_[_]]: CorecursiveT, A](str: String): FreeMapA[T, A] =
      Free.roll(Constant[T, FreeMapA[T, A]](EJson.fromCommon[T[EJson]].apply(ejson.Str[T[EJson]](str))))

    def unapply[T[_[_]]: RecursiveT, A](mf: FreeMapA[T, A]):
        Option[String] =
      mf.resume.fold({
        case Constant(ej) => CommonEJson.prj(ej.project).flatMap {
          case ejson.Str(str) => str.some
          case _ => None
        }
        case _ => None
      }, {
        _ => None
      })
  }
}
