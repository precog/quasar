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

import quasar._
import quasar.ejson._
import quasar.Predef._
import quasar.fp._
import quasar.std.StdLib._

import matryoshka._, Recursive.ops._
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

  /** Returns a List that maps element-by-element to a MapFunc array. If we
    * can’t statically determine _all_ of the elements, it doesn’t match.
    */
  object StaticArray {
    private implicit def implicitPrio[F[_], G[_]]: Inject[G, Coproduct[F, G, ?]] = Inject.rightInjectInstance

    def unapply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Recursive, A](
      mf: CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        Option[List[T[CoEnv[A, MapFunc[T2, ?], ?]]]] =
      mf match {
        case ConcatArraysN(as) =>
          as.foldRightM[Option, List[T[CoEnv[A, MapFunc[T2, ?], ?]]]](
            Nil)(
            (mf, acc) => (mf.project.run.toOption >>=
              {
                case MakeArray(value) => (value :: acc).some
                case Constant(Embed(Inj(ejson.Arr(values)))) =>
                  (values.map(v => CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]](Constant(v).right).embed) ++ acc).some
                case _ => None
              }))
        case _ => None
      }
  }

  /** Like `StaticArray`, but returns as much of the array as can be statically
    * determined. Useful if you just want to statically lookup into an array if
    * possible, and punt otherwise.
    */
  object StaticArrayPrefix {
    private implicit def implicitPrio[F[_], G[_]]: Inject[G, Coproduct[F, G, ?]] = Inject.rightInjectInstance

    def unapply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Recursive, A](
      mf: CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        Option[List[T[CoEnv[A, MapFunc[T2, ?], ?]]]] =
      mf match {
        case ConcatArraysN(as) =>
          as.foldRightM[List[T[CoEnv[A, MapFunc[T2, ?], ?]]] \/ ?, List[T[CoEnv[A, MapFunc[T2, ?], ?]]]](
            Nil)(
            (mf, acc) => mf.project.run.fold(
              κ(acc.left),
              _ match {
                case MakeArray(value) => (value :: acc).right
                case Constant(Embed(Inj(ejson.Arr(values)))) =>
                  (values.map(v => CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]](Constant(v).right).embed) ++ acc).right
                case _ => acc.left
              })).merge.some
        case _ => None
      }
  }

  // TODO: subtyping is preventing embedding of MapFuncs
  /** This returns the set of expressions that are concatenated together. It can
    * include statically known pieces, like `MakeArray` and `Constant(Arr)`, but
    * also arbitrary expressions that may evaluate to an array of any size.
    */
  object ConcatArraysN {
    private implicit def implicitPrio[F[_], G[_]]: Inject[G, Coproduct[F, G, ?]] = Inject.rightInjectInstance

    def apply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Corecursive, A](args: List[Free[MapFunc[T2, ?], A]]):
        Free[MapFunc[T2, ?], A] =
      apply(args.map(_.toCoEnv[T])).embed.fromCoEnv

    def apply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Corecursive, A](args: List[T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]] = {
      args.toList match {
        case h :: t => t.foldLeft(h)((a, b) => CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]((ConcatArrays(a, b): MapFunc[T2, T[CoEnv[A, MapFunc[T2, ?], ?]]]).right).embed).project
        case Nil    => CoEnv(\/-(Constant[T2, T[CoEnv[A, MapFunc[T2, ?], ?]]](EJson.fromCommon[T2].apply(ejson.Arr[T2[EJson]](Nil)))))
      }
    }

    def unapply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Recursive, A](
      mf: CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        Option[List[T[CoEnv[A, MapFunc[T2, ?], ?]]]] =
      mf.run.fold(
        κ(None),
        {
          case MakeArray(_) | Constant(Embed(Inj(ejson.Arr(_)))) =>
            List(mf.embed).some
          case ConcatArrays(h, t) =>
            (unapply(h.project).getOrElse(List(h)) ++
              unapply(t.project).getOrElse(List(t))).some
          case _ => None
        })

  }

  type CoMF[T[_[_]], A, B] = CoEnv[A, MapFunc[T, ?], B]
  type CoMFR[T[_[_]], A] = CoMF[T, A, T[CoMF[T, A, ?]]]

  // TODO subtyping is preventing embeding of MapFuncs
  object ConcatMapsN {
    private implicit def implicitPrio[F[_], G[_]]: Inject[F, Coproduct[F, G, ?]] = Inject.leftInjectInstance

    def apply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Corecursive, A](args: List[T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]] = {
      args.toList match {
        case h :: t => t.foldLeft(h)((a, b) => CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]((ConcatMaps(a, b): MapFunc[T2, T[CoEnv[A, MapFunc[T2, ?], ?]]]).right).embed).project
        case Nil    => CoEnv(\/-(Constant[T2, T[CoEnv[A, MapFunc[T2, ?], ?]]](EJson.fromCommon[T2].apply(ejson.Arr[T2[EJson]](Nil)))))
      }
    }

    def unapply[T[_[_]]: Recursive: Corecursive, T2[_[_]]: Recursive, A](
      mf: CoEnv[A, MapFunc[T2, ?], T[CoEnv[A, MapFunc[T2, ?], ?]]]):
        Option[List[T[CoEnv[A, MapFunc[T2, ?], ?]]]] =
      mf.run.fold(
        κ(None),
        {
          case MakeMap(_, _) | Constant(Embed(Inj(ejson.Map(_)))) =>
            List(mf.embed).some
          case ConcatMaps(h, t) =>
            (unapply(h.project).getOrElse(List(h)) ++
              unapply(t.project).getOrElse(List(t))).some
          case _ => None
        })
  }

  // Transform effectively constant `MapFunc` into a `Constant` value.
  // This is a mini-evaluator for constant qscript values.
  def foldConstant[T[_[_]]: Recursive: Corecursive, A]: CoMFR[T, A] => Option[CoMFR[T, A]] = {
    def exprCommon(result: ejson.Common[T[EJson]]): Option[CoMFR[T, A]] =
      Some(CoEnv(Constant(EJson.fromCommon.apply(result)).right))

    def exprExt(result: ejson.Extension[T[EJson]]): Option[CoMFR[T, A]] =
      Some(CoEnv(Constant(EJson.fromExt.apply(result)).right))

    object EjConstCommon {
      def unapply[B](tco: T[CoMF[T, B, ?]]): Option[ejson.Common[T[EJson]]] = tco match {
        case Embed(CoEnv(\/-(Constant(Embed(ejson.Common(v)))))) => Some(v)
        case _                                                   => None
      }
    }

    object EjConstExtension {
      def unapply[B](tco: T[CoMF[T, B, ?]]): Option[ejson.Extension[T[EJson]]] = tco match {
        case Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(v)))))) => Some(v)
        case _                                                      => None
      }
    }

    _.run.fold(
      κ(None),
      {
        // relations
        case And(EjConstCommon(ejson.Bool(v1)), EjConstCommon(ejson.Bool(v2))) => exprCommon(ejson.Bool(v1 && v2))
        case Or(EjConstCommon(ejson.Bool(v1)), EjConstCommon(ejson.Bool(v2)))  => exprCommon(ejson.Bool(v1 || v2))
        case Not(EjConstCommon(ejson.Bool(v1)))                                => exprCommon(ejson.Bool(!v1))

        // string
        case Lower(EjConstCommon(ejson.Str(v1))) => exprCommon(ejson.Str(v1.toLowerCase))
        case Upper(EjConstCommon(ejson.Str(v1))) => exprCommon(ejson.Str(v1.toUpperCase))

        // structural
        case MakeArray(Embed(CoEnv(\/-(Constant(v1)))))                               => exprCommon(ejson.Arr(List(v1)))
        case MakeMap(EjConstCommon(ejson.Str(v1)), Embed(CoEnv(\/-(Constant(v2)))))   => exprExt(ejson.Map(List(EJson.fromCommon.apply(ejson.Str(v1)) -> v2)))
        case ConcatArrays(EjConstCommon(ejson.Arr(v1)), EjConstCommon(ejson.Arr(v2))) => exprCommon(ejson.Arr(v1 ++ v2))

        case _ => None
      })
  }

  // TODO: This could be split up as it is in LP, with each function containing
  //       its own normalization.
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def normalize[T[_[_]]: Recursive: Corecursive: EqualT, A]:
      CoMFR[T, A] => Option[CoMFR[T, A]] = {
    _.run.fold(
      κ(None),
      {
        case Eq(Embed(CoEnv(\/-(Constant(v1)))), Embed(CoEnv(\/-(Constant(v2))))) =>
          CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](
            Constant[T, T[CoEnv[A, MapFunc[T, ?], ?]]](EJson.fromCommon[T].apply(
              ejson.Bool[T[EJson]](v1 ≟ v2))).right).some

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
              }.map(p => CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](Constant[T, T[CoEnv[A, MapFunc[T, ?], ?]]](p._2).right)).get
          }

        // elide Nil array on the left
        case ConcatArrays(
          Embed(CoEnv(\/-(Constant(Embed(ejson.Common(ejson.Arr(Nil))))))),
          Embed(CoEnv(\/-(rhs)))) =>
            CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](rhs.right[A]).some

        // elide Nil array on the right
        case ConcatArrays(
          Embed(CoEnv(\/-(lhs))),
          Embed(CoEnv(\/-(Constant(Embed(ejson.Common(ejson.Arr(Nil)))))))) =>
            CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](lhs.right[A]).some

        // elide Nil map on the left
        case ConcatMaps(
          Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Map(Nil))))))),
          Embed(CoEnv(\/-(rhs)))) =>
            CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](rhs.right[A]).some

        // elide Nil map on the right
        case ConcatMaps(
          Embed(CoEnv(\/-(lhs))),
          Embed(CoEnv(\/-(Constant(Embed(ejson.Extension(ejson.Map(Nil)))))))) =>
            CoEnv[A, MapFunc[T, ?], T[CoEnv[A, MapFunc[T, ?], ?]]](lhs.right[A]).some

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
        case Date(a1) => f(a1) ∘ (Date(_))
        case Time(a1) => f(a1) ∘ (Time(_))
        case Timestamp(a1) => f(a1) ∘ (Timestamp(_))
        case Interval(a1) => f(a1) ∘ (Interval(_))
        case TimeOfDay(a1) => f(a1) ∘ (TimeOfDay(_))
        case ToTimestamp(a1) => f(a1) ∘ (ToTimestamp(_))
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
        case DupArrayIndices(a1) => f(a1) ∘ (DupArrayIndices(_))
        case DupMapKeys(a1) => f(a1) ∘ (DupMapKeys(_))
        case ZipArrayIndices(a1) => f(a1) ∘ (ZipArrayIndices(_))
        case ZipMapKeys(a1) => f(a1) ∘ (ZipMapKeys(_))

        // binary
        case Extract(a1, a2) => (f(a1) ⊛ f(a2))(Extract(_, _))
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
        case Coalesce(a1, a2) => (f(a1) ⊛ f(a2))(Coalesce(_, _))
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
        case (Date(a1), Date(b1)) => in.equal(a1, b1)
        case (Time(a1), Time(b1)) => in.equal(a1, b1)
        case (Timestamp(a1), Timestamp(b1)) => in.equal(a1, b1)
        case (Interval(a1), Interval(b1)) => in.equal(a1, b1)
        case (TimeOfDay(a1), TimeOfDay(b1)) => in.equal(a1, b1)
        case (ToTimestamp(a1), ToTimestamp(b1)) => in.equal(a1, b1)
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
        case (DupArrayIndices(a1), DupArrayIndices(b1)) => in.equal(a1, b1)
        case (DupMapKeys(a1), DupMapKeys(b1)) => in.equal(a1, b1)
        case (ZipArrayIndices(a1), ZipArrayIndices(b1)) => in.equal(a1, b1)
        case (ZipMapKeys(a1), ZipMapKeys(b1)) => in.equal(a1, b1)

        case (Extract(a1, a2), Extract(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
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
        case (Coalesce(a1, a2), Coalesce(b1, b2)) => in.equal(a1, b1) && in.equal(a2, b2)
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
      def apply[A](sh: Show[A]): Show[MapFunc[T, A]] = Show.show {
        // nullary
        case Constant(v) => Cord("Constant(") ++ v.show ++ Cord(")")
        case Undefined() => Cord("Undefined()")
        case Now() => Cord("Now()")

        // unary
        case Date(a1) => Cord("Date(") ++ sh.show(a1) ++ Cord(")")
        case Time(a1) => Cord("Time(") ++ sh.show(a1) ++ Cord(")")
        case Timestamp(a1) => Cord("Timestamp(") ++ sh.show(a1) ++ Cord(")")
        case Interval(a1) => Cord("Interval(") ++ sh.show(a1) ++ Cord(")")
        case TimeOfDay(a1) => Cord("TimeOfDay(") ++ sh.show(a1) ++ Cord(")")
        case ToTimestamp(a1) => Cord("ToTimestamp(") ++ sh.show(a1) ++ Cord(")")
        case Negate(a1) => Cord("Negate(") ++ sh.show(a1) ++ Cord(")")
        case Not(a1) => Cord("Not(") ++ sh.show(a1) ++ Cord(")")
        case Length(a1) => Cord("Length(") ++ sh.show(a1) ++ Cord(")")
        case Lower(a1) => Cord("Lower(") ++ sh.show(a1) ++ Cord(")")
        case Upper(a1) => Cord("Upper(") ++ sh.show(a1) ++ Cord(")")
        case Bool(a1) => Cord("Bool(") ++ sh.show(a1) ++ Cord(")")
        case Integer(a1) => Cord("Integer(") ++ sh.show(a1) ++ Cord(")")
        case Decimal(a1) => Cord("Decimal(") ++ sh.show(a1) ++ Cord(")")
        case Null(a1) => Cord("Null(") ++ sh.show(a1) ++ Cord(")")
        case ToString(a1) => Cord("ToString(") ++ sh.show(a1) ++ Cord(")")
        case MakeArray(a1) => Cord("MakeArray(") ++ sh.show(a1) ++ Cord(")")
        case DupArrayIndices(a1) => Cord("DupArrayIndices(") ++ sh.show(a1) ++ Cord(")")
        case DupMapKeys(a1) => Cord("DupMapKeys(") ++ sh.show(a1) ++ Cord(")")
        case ZipArrayIndices(a1) => Cord("ZipArrayIndices(") ++ sh.show(a1) ++ Cord(")")
        case ZipMapKeys(a1) => Cord("ZipMapKeys(") ++ sh.show(a1) ++ Cord(")")

        // binary
        case Extract(a1, a2) => Cord("Extract(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Add(a1, a2) => Cord("Add(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Multiply(a1, a2) => Cord("Multiply(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Subtract(a1, a2) => Cord("Subtract(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Divide(a1, a2) => Cord("Divide(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Modulo(a1, a2) => Cord("Modulo(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Power(a1, a2) => Cord("Power(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Eq(a1, a2) => Cord("Eq(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Neq(a1, a2) => Cord("Neq(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Lt(a1, a2) => Cord("Lt(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Lte(a1, a2) => Cord("Lte(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Gt(a1, a2) => Cord("Gt(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Gte(a1, a2) => Cord("Gte(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case IfUndefined(a1, a2) => Cord("IfUndefined(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case And(a1, a2) => Cord("And(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Or(a1, a2) => Cord("Or(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Coalesce(a1, a2) => Cord("Coalesce(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Within(a1, a2) => Cord("Within(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case MakeMap(a1, a2) => Cord("MakeMap(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ConcatMaps(a1, a2) => Cord("ConcatMaps(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ProjectIndex(a1, a2) => Cord("ProjectIndex(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ProjectField(a1, a2) => Cord("ProjectField(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case DeleteField(a1, a2) => Cord("DeleteField(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case ConcatArrays(a1, a2) => Cord("ConcatArrays(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")
        case Range(a1, a2) => Cord("Range(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2)  ++ Cord(")")

        //  ternary
        case Between(a1, a2, a3) => Cord("Between(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2) ++ Cord(", ") ++ sh.show(a3) ++ Cord(")")
        case Cond(a1, a2, a3) => Cord("Cond(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2) ++ Cord(", ") ++ sh.show(a3) ++ Cord(")")
        case Search(a1, a2, a3) => Cord("Search(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2) ++ Cord(", ") ++ sh.show(a3) ++ Cord(")")
        case Substring(a1, a2, a3) => Cord("Substring(") ++ sh.show(a1) ++ Cord(", ") ++ sh.show(a2) ++ Cord(", ") ++ sh.show(a3) ++ Cord(")")
        case Guard(a1, tpe, a2, a3) => Cord("Guard(") ++ sh.show(a1) ++ Cord(", ") ++ tpe.show ++ Cord(", ") ++ sh.show(a2) ++ Cord(", ") ++ sh.show(a3) ++ Cord(")")
      }
    }

  def translateUnaryMapping[T[_[_]], A]: UnaryFunc => A => MapFunc[T, A] = {
    {
      case date.Date => Date(_)
      case date.Time => Time(_)
      case date.Timestamp => Timestamp(_)
      case date.Interval => Interval(_)
      case date.TimeOfDay => TimeOfDay(_)
      case date.ToTimestamp => ToTimestamp(_)
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
    }
  }

  def translateBinaryMapping[T[_[_]], A]: BinaryFunc => (A, A) => MapFunc[T, A] = {
    {
      // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
      //     about the second. And it shouldn’t even have two in LP.
      case array.ArrayLength => (a, b) => Length(a)
      case date.Extract => Extract(_, _)
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
      case relations.Coalesce => Coalesce(_, _)
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
  }

  def translateTernaryMapping[T[_[_]], A]:
      TernaryFunc => (A, A, A) => MapFunc[T, A] = {
    {
      case relations.Between => Between(_, _, _)
      case relations.Cond    => Cond(_, _, _)
      case string.Search     => Search(_, _, _)
      case string.Substring  => Substring(_, _, _)
    }
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
  @Lenses final case class Date[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Time[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Timestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Interval[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class TimeOfDay[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class ToTimestamp[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Extract[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
  /** Fetches the [[quasar.Type.Timestamp]] for the current instant in time.
    */
  @Lenses final case class Now[T[_[_]], A]() extends Nullary[T, A]


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
  @Lenses final case class Coalesce[T[_[_]], A](a1: A, a2: A) extends Binary[T, A]
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
  @Lenses final case class MakeArray[T[_[_]], A](a1: A) extends Unary[T, A]
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

  // helpers & QScript-specific
  /** Turns a map of `{ k1: v1, k2: v2, ...}` into a map of
    * `{ k1: k1, k2: k2, ...}`.
    */
  @Lenses final case class DupMapKeys[T[_[_]], A](a1: A) extends Unary[T, A]
  /** Turns an array of `[v1, v2, ...]` into an array of `[0, 1, ...]`.
    */
  @Lenses final case class DupArrayIndices[T[_[_]], A](a1: A) extends Unary[T, A]
  /** Turns a map of `{ k1: v1, k2: v2, ...}` into a map of
    * `{ k1: [k1, v1], k2: [k2, v2], ...}`.
    */
  @Lenses final case class ZipMapKeys[T[_[_]], A](a1: A) extends Unary[T, A]
  /** Turns an array of `[v1, v2, ...]` into an array of
    * `[[0, v1], [1, v2], ...]`.
    */
  @Lenses final case class ZipArrayIndices[T[_[_]], A](a1: A) extends Unary[T, A]
  @Lenses final case class Range[T[_[_]], A](from: A, to: A) extends Binary[T, A] {
    def a1 = from
    def a2 = to
  }
  /** A conditional specifically for checking that `a1` satisfies `pattern`.
    */
  @Lenses final case class Guard[T[_[_]], A](a1: A, pattern: Type, a2: A, a3: A)
      extends Ternary[T, A]

  object NullLit {
    def apply[T[_[_]]: Corecursive, A](): Free[MapFunc[T, ?], A] =
      Free.roll(Constant[T, Free[MapFunc[T, ?], A]](EJson.fromCommon[T].apply(ejson.Null[T[EJson]]())))

    def unapply[T[_[_]]: Recursive, A](mf: Free[MapFunc[T, ?], A]): Boolean = mf.resume.fold ({
      case Constant(ej) => CommonEJson.prj(ej.project).fold(false) {
        case ejson.Null() => true
        case _ => false
      }
      case _ => false
    }, _ => false)
  }

  object BoolLit {
    def apply[T[_[_]]: Corecursive, A](b: Boolean): Free[MapFunc[T, ?], A] =
      Free.roll(Constant[T, Free[MapFunc[T, ?], A]](EJson.fromCommon[T].apply(ejson.Bool[T[EJson]](b))))

    def unapply[T[_[_]]: Recursive, A](mf: Free[MapFunc[T, ?], A]): Option[Boolean] = mf.resume.fold ({
      case Constant(ej) => CommonEJson.prj(ej.project).flatMap {
        case ejson.Bool(b) => b.some
        case _ => None
      }
      case _ => None
    }, _ => None)
  }

  object IntLit {
    def apply[T[_[_]]: Corecursive, A](i: BigInt): Free[MapFunc[T, ?], A] =
      Free.roll(Constant[T, Free[MapFunc[T, ?], A]](EJson.fromExt[T].apply(ejson.Int[T[EJson]](i))))

    def unapply[T[_[_]]: Recursive, A](mf: Free[MapFunc[T, ?], A]): Option[BigInt] = mf.resume.fold ({
      case Constant(ej) => ExtEJson.prj(ej.project).flatMap {
        case ejson.Int(i) => i.some
        case _ => None
      }
      case _ => None
    }, _ => None)
  }

  object StrLit {
    def apply[T[_[_]]: Corecursive, A](str: String): Free[MapFunc[T, ?], A] =
      Free.roll(Constant[T, Free[MapFunc[T, ?], A]](EJson.fromCommon[T].apply(ejson.Str[T[EJson]](str))))

    def unapply[T[_[_]]: Recursive, A, B](mf: CoEnv[A, MapFunc[T, ?], B]):
        Option[String] =
      mf.run.fold({
        _ => None
      }, {
        case Constant(ej) => CommonEJson.prj(ej.project).flatMap {
          case ejson.Str(str) => str.some
          case _ => None
        }
        case _ => None
      })

    def unapply[T[_[_]]: Recursive, A](mf: Free[MapFunc[T, ?], A]):
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

