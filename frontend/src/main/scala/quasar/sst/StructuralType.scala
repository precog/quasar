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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, EncodeEJson, EncodeEJsonK}
import quasar.fp.ski.κ
import quasar.tpe._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import monocle.Lens
import scalaz._, Scalaz._

/** A measure annotated Type focused on structure over semantics.
  *
  * @tparam L the type of literals
  * @tparam V the measure the type is annotated with
  */
final case class StructuralType[L, V](toCofree: Cofree[TypeF[L, ?], V]) extends scala.AnyVal {
  def cobind[W](f: StructuralType[L, V] => W): StructuralType[L, W] =
    StructuralType(toCofree.cobind(c => f(StructuralType(c))))

  def cojoin: StructuralType[L, StructuralType[L, V]] =
    StructuralType(toCofree.duplicate map (StructuralType(_)))

  def copoint: V =
    toCofree.head

  def map[W](f: V => W): StructuralType[L, W] =
    StructuralType(toCofree map f)

  /** Convert to a type, forgetting annotations. */
  def toType[T](implicit T: Corecursive.Aux[T, TypeF[L, ?]]): T =
    forgetAnnotation[Cofree[TypeF[L, ?], V], T, TypeF[L, ?], V](toCofree)
}

object StructuralType extends StructuralTypeInstances {
  /** Lens targeting the top-level annotation of a structural type. */
  def measure[L, V]: Lens[StructuralType[L, V], V] =
    Lens[StructuralType[L, V], V](_.copoint)(v => st => envT(v, st.project.lower).embed)

  /** The structural type representing the given EJson value, annotated with
    * the result of `measure`.
    */
  object fromEJson {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[V](measure: Algebra[TypeF[J, ?], V], ejson: J)(
        implicit
        J : Order[J],
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): StructuralType[J, V] = {
        type T = Fix[TypeF[J, ?]]

        val attrV =
          attributeAlgebra[TypeF[J, ?], V](measure)

        val normAndAttr =
          ((_: T).cata[Cofree[TypeF[J, ?], V]](attrV)) <<<
          ((_: TypeF[J, T]).embed)                     <<<
          normalization.lowerConst[J, T]

        StructuralType(normAndAttr(TypeF.const[J, T](ejson)))
      }
    }
  }

  /** The structural type representing the given EJson value annotated with
    * the given measure.
    */
  object fromEJsonK {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[V](measure: V, ejson: J)(
        implicit
        J : Order[J],
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): StructuralType[J, V] =
        fromEJson[J](κ(measure), ejson)
    }
  }

  /** Unfold a pair of structural types into their deep structural merge. */
  def mergeƒ[L: Order, V: Monoid, T](
    implicit
    TR: Recursive.Aux[T, EnvT[V, TypeF[L, ?], ?]],
    TC: Corecursive.Aux[T, EnvT[V, TypeF[L, ?], ?]]
  ): ElgotCoalgebra[T \/ ?, EnvT[V, TypeF[L, ?], ?], (T, T)] = {
    import TypeF._
    type TT = (T, T)
    val ⊥ = envT(∅[V], bottom[L, T]()).embed

    def mergeThese[F[_]: Align](xs: F[T], ys: F[T]): F[TT] =
      xs.alignWith(ys)(_.fold((_, ⊥), (_, ⊥), (_, _)))

    def mergeArr(l: (V, IList[T] \/ T), r: (V, IList[T] \/ T)): TypeF[L, T] \/ TypeF[L, TT] =
      (l, r) match {
        case ((_, -\/(              xs)), (_, -\/(              ys))) => arr[L, TT](mergeThese(xs, ys).left).right
        case ((_, \/-(               x)), (_, \/-(               y))) => arr[L, TT]((x, y).right).right
        case ((_, -\/(          INil())), (_, y @ \/-(           _))) => arr[L,  T](y).left
        case ((_, x @ \/-(           _)), (_, -\/(          INil()))) => arr[L,  T](x).left
        case ((_, -\/(ICons(x, INil()))), (_, \/-(               y))) => arr[L, TT]((x, y).right).right
        case ((_, \/-(               x)), (_, -\/(ICons(y, INil())))) => arr[L, TT]((x, y).right).right

        case ((v, -\/(ICons(a, ICons(b, cs)))), (_, \/-(y))) =>
          arr[L, TT]((envT(v, union[L, T](a, b, cs)).embed, y).right).right

        case ((_, \/-(x)), (v, -\/(ICons(a, ICons(b, cs))))) =>
          arr[L, TT]((x, envT(v, union[L, T](a, b, cs)).embed).right).right
      }

    def mergeUnk(xu: Option[TT], yu: Option[TT]): Option[(TT, TT)] =
      (xu, yu) match {
        case (Some((xk, xv)), Some((yk, yv))) => some(((xk, yk), (xv, yv)))
        case (Some((xk, xv)),           None) => some(((xk,  ⊥), (xv,  ⊥)))
        case (          None, Some((yk, yv))) => some(((⊥ , yk), (⊥ , yv)))
        case (          None,           None) => none
      }

    def mergeable(x: T, y: T): Boolean =
      (x, y).umap(_.project.lower) match {
        case (    Top(),     Top()) => true
        case (Simple(a), Simple(b)) => a ≟ b
        case ( Const(a),  Const(b)) => a ≟ b
        case (   Arr(_),    Arr(_)) => true
        case (Map(_, _), Map(_, _)) => true
        case _                      => false
      }

    def mergeUnions(v: V, xs: NonEmptyList[T], ys: NonEmptyList[T]): T \/ EnvT[V, TypeF[L, ?], TT] =
      xs.foldLeft(ys.toZipper map (_.right[TT]))((z, x) =>
        // Pair with merge candidate if found, otherwise add to the union.
        z.findZ(_ exists (mergeable(x, _))).cata(
          _.modify(_ >>= ((x, _).left[T])),
          z.insert((x, ⊥).left[T])))
        .map(_.swap valueOr ((⊥, _)))
        .toNel match {
          case NonEmptyList(a, ICons(b, cs)) => envT(v, union[L, TT](a, b, cs)).right
          case NonEmptyList(a,       INil()) => envT(v, coproduct[L, T](a)).embed.left
        }

    _.umap(_.project) match {
      case (                     x, EnvT((_,    Bottom())))            => x.embed.left
      case (EnvT((_,    Bottom())),                      y)            => y.embed.left
      case (EnvT((v,       Top())), EnvT((w,       Top())))            => envT(v |+| w, top[L, T]()).embed.left
      case (EnvT((v,   Simple(x))), EnvT((w,   Simple(y)))) if (x ≟ y) => envT(v |+| w, simple[L, T](x)).embed.left
      case (EnvT((v,    Const(x))), EnvT((w,    Const(y)))) if (x ≟ y) => envT(v |+| w, const[L, T](x)).embed.left
      case (EnvT((v, Unioned(xs))), EnvT((w, Unioned(ys))))            => mergeUnions(v |+| w, xs, ys)
      case (x @ EnvT((v,       _)), EnvT((w, Unioned(ys))))            => mergeUnions(v |+| w, x.embed.wrapNel, ys)
      case (EnvT((v, Unioned(xs))), y @ EnvT((w,       _)))            => mergeUnions(v |+| w, xs, y.embed.wrapNel)

      case (EnvT((v, Arr(x))), EnvT((w, Arr(y)))) =>
        val sum = v |+| w
        mergeArr((v, x), (w, y)).bimap(envT(sum, _).embed, envT(sum, _))

      case (EnvT((v, Map(xs, xunk))), EnvT((w, Map(ys, yunk)))) =>
        envT(v |+| w, map[L, TT](mergeThese(xs, ys), mergeUnk(xunk, yunk))).right

      case (x @ EnvT((v, _)), y @ EnvT((w, _))) =>
        envT(v |+| w, coproduct[L, T](x.embed, y.embed)).embed.left
    }
  }
}

sealed abstract class StructuralTypeInstances extends StructuralTypeInstances0 {
  implicit def encodeEJson[L: EncodeEJson, V: EncodeEJson]: EncodeEJson[StructuralType[L, V]] = {
    implicit val encodeEnvT = EncodeEJsonK.envT[V, TypeF[L, ?]]("measure", "structure")
    EncodeEJson.encodeEJsonR[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]]
  }

  implicit def corecursive[L, V]: Corecursive.Aux[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]] =
    new Corecursive[StructuralType[L, V]] {
      type Base[B] = EnvT[V, TypeF[L, ?], B]

      def embed(st: EnvT[V, TypeF[L, ?], StructuralType[L, V]])(implicit BF: Functor[EnvT[V, TypeF[L, ?], ?]]) =
        StructuralType(st.map(_.toCofree).embed)
    }

  implicit def recursive[L, V]: Recursive.Aux[StructuralType[L, V], EnvT[V, TypeF[L, ?], ?]] =
    new Recursive[StructuralType[L, V]] {
      type Base[B] = EnvT[V, TypeF[L, ?], B]

      def project(st: StructuralType[L, V])(implicit BF: Functor[EnvT[V, TypeF[L, ?], ?]]) =
        st.toCofree.project map (StructuralType(_))
    }

  implicit def monoid[L: Order, V: Monoid]: Monoid[StructuralType[L, V]] =
    new Monoid[StructuralType[L, V]] {
      type T = Cofree[TypeF[L, ?], V]
      val zero = StructuralType(Cofree(∅[V], TypeF.bottom[L, T]()))
      def append(x: StructuralType[L, V], y: => StructuralType[L, V]) =
        StructuralType((x.toCofree, y.toCofree).elgotApo[T](StructuralType.mergeƒ[L, V, T]))
    }

  implicit def comonad[L]: Comonad[StructuralType[L, ?]] =
    new Comonad[StructuralType[L, ?]] {
      def copoint[A](st: StructuralType[L, A]) =
        st.copoint

      override def cojoin[A](st: StructuralType[L, A]) =
        st.cojoin

      override final def map[A, B](st: StructuralType[L, A])(f: A => B) =
        st map f

      def cobind[A, B](st: StructuralType[L, A])(f: StructuralType[L, A] => B) =
        st cobind f
    }

  implicit def equal[L: Equal, V: Equal]: Equal[StructuralType[L, V]] = {
    implicit val eqlTypeF = TypeF.structuralEqual[L]
    Equal.equalBy(_.toCofree)
  }

  implicit def show[L: Show, V: Show]: Show[StructuralType[L, V]] =
    Show.show(st => Cord("StructuralType") ++ st.toCofree.show)
}

sealed abstract class StructuralTypeInstances0 {
  implicit def traverse1[L]: Traverse1[StructuralType[L, ?]] =
    new Traverse1[StructuralType[L, ?]] {
      override def map[A, B](st: StructuralType[L, A])(f: A => B): StructuralType[L, B] =
        StructuralType(st.toCofree map f)

      override final def foldMap[A, B: Monoid](st: StructuralType[L, A])(f: A => B): B =
        st.toCofree.foldMap(f)

      override final def foldRight[A, B](st: StructuralType[L, A], z: => B)(f: (A, => B) => B): B =
        st.toCofree.foldRight(z)(f)

      override final def foldLeft[A, B](st: StructuralType[L, A], z: B)(f: (B, A) => B): B =
        st.toCofree.foldLeft(z)(f)

      override final def foldMapLeft1[A, B](st: StructuralType[L, A])(z: A => B)(f: (B, A) => B): B =
        st.toCofree.foldMapLeft1(z)(f)

      override def foldMapRight1[A, B](st: StructuralType[L, A])(z: A => B)(f: (A, => B) => B): B =
        st.toCofree.foldMapRight1(z)(f)

      override def foldMap1[A, B](st: StructuralType[L, A])(f: A => B)(implicit S: Semigroup[B]): B =
        st.toCofree.foldMap1(f)

      override def traverseImpl[G[_]: Applicative, A, B](st: StructuralType[L, A])(f: A => G[B]): G[StructuralType[L, B]] =
        st.toCofree.traverse(f) map (StructuralType(_))

      def traverse1Impl[G[_]: Apply, A, B](st: StructuralType[L, A])(f: A => G[B]): G[StructuralType[L, B]] =
        st.toCofree.traverse1(f) map (StructuralType(_))
    }
}
