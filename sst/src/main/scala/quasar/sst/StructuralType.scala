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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.zipper._
import quasar.ejson
import quasar.ejson.{
  Decoded,
  DecodeEJsonK,
  DecodeEJson,
  EJson,
  EncodeEJson,
  EncodeEJsonK,
  ExtEJson => E,
  Type => EType,
  TypeTag
}
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkShow, copkTraverse}
import quasar.contrib.iota.{:<<:, ACopK}
import quasar.fp.ski.κ
import quasar.tpe._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import monocle.Lens
import scalaz._, Scalaz._
import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

/** A measure annotated Type focused on structure over semantics.
  *
  * @tparam L the type of literals
  * @tparam V the measure the type is annotated with
  */
final case class StructuralType[L, V](toCofree: Cofree[StructuralType.ST[L, ?], V]) extends scala.AnyVal {
  def cobind[W](f: StructuralType[L, V] => W): StructuralType[L, W] =
    StructuralType(toCofree.cobind(c => f(StructuralType(c))))

  def cojoin: StructuralType[L, StructuralType[L, V]] =
    StructuralType(toCofree.duplicate map (StructuralType(_)))

  def copoint: V =
    toCofree.head

  def map[W](f: V => W): StructuralType[L, W] =
    StructuralType(toCofree map f)
}

object StructuralType extends StructuralTypeInstances {
  type ST[L, A]     = CopK[TypeF[L, ?] ::: Tagged ::: TNilK, A]
  type STF[L, V, A] = EnvT[V, ST[L, ?], A]

  object TypeST {
    def apply[L, A](tf: TypeF[L, A]): ST[L, A] =
      CopK.Inject[TypeF[L, ?], ST[L, ?]].inj(tf)

    def unapply[L, A](st: ST[L, A]): Option[TypeF[L, A]] =
      CopK.Inject[TypeF[L, ?], ST[L, ?]].prj(st)
  }

  object TagST {
    def apply[L] = new PartiallyApplied[L]

    final class PartiallyApplied[L] {
      def apply[A](tg: Tagged[A]): ST[L, A] =
        CopK.Inject[Tagged, ST[L, ?]].inj(tg)
    }

    def unapply[L, A](st: ST[L, A]): Option[Tagged[A]] =
      CopK.Inject[Tagged, ST[L, ?]].prj(st)
  }

  object ConstST {
    def unapply[L, V](stf: STF[L, V, StructuralType[L, V]]): Option[(Option[(V, TypeTag)], V, L)] =
      stf match {
        case EnvT((v, TypeST(TypeF.Const(j)))) =>
          some((none, v, j))

        case EnvT((v1, TagST(Tagged(t, Embed(EnvT((v2, TypeST(TypeF.Const(j))))))))) =>
          some((some((v1, t)), v2, j))

        case _ => none
      }
  }

  /** Lens targeting the top-level annotation of a structural type. */
  def measure[L, V]: Lens[StructuralType[L, V], V] =
    Lens[StructuralType[L, V], V](_.copoint)(v => st => envT(v, st.project.lower).embed)

  // TODO{matryoshka}: Define over EnvT once generalized attribute* are available.
  /** Lift an algebra over types to an algebra annotating a structural type. */
  def attributeSTƒ[J, V](measure: Algebra[TypeF[J, ?], V]): Algebra[ST[J, ?], Cofree[ST[J, ?], V]] =
    attributeAlgebra[ST[J, ?], V] {
      case TypeST(tf)          => measure(tf)
      case TagST(Tagged(_, v)) => v
    }

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
        StructuralType(
          TypeF.const[J, T](ejson).embed
            .hylo(attributeSTƒ(measure), fromTypeƒ[J, T]))
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

  /** Whether the StructuralType is constant. */
  def isConst[L, V](st: StructuralType[L, V]): Boolean =
    ConstST.unapply(st.project).isDefined

  /** Unfold a pair of structural types into their deep structural merge. */
  def mergeƒ[L, V: Semigroup, F[a] <: ACopK[a]: Functor, T](
    implicit
    I: TypeF[L, ?] :<<: F,
    F: StructuralMerge[F],
    TC: Corecursive.Aux[T, EnvT[V, F, ?]],
    TR: Recursive.Aux[T, EnvT[V, F, ?]]
  ): GCoalgebra[T \/ ?, EnvT[V, F, ?], (T, T)] =
    tt => {
      val pp = tt.umap(_.project)
      F.merge[V, T](pp) getOrElse (pp match {
        case (EnvT((v, I(TypeF.Unioned(xs)))), y @ EnvT((w, _))) =>
          envT(v |+| w, I(TypeF.union[L, T](y.embed, xs.head, xs.tail)) map (_.left))

        case (x @ EnvT((v, _)), EnvT((w, I(TypeF.Unioned(ys))))) =>
          envT(v |+| w, I(TypeF.union[L, T](x.embed, ys.head, ys.tail)) map (_.left))

        case (EnvT((v, _)), EnvT((w, _))) =>
          envT(v |+| w, I(TypeF.coproduct[L, T](tt) map (_.left)))
      })
    }

  /** A transform ensuring unions are disjoint by merging their members. */
  def disjoinUnionsƒ[L, V: Semigroup, F[a] <: ACopK[a]: Functor, T](
    implicit
    I: TypeF[L, ?] :<<: F,
    F: StructuralMerge[F],
    TC: Corecursive.Aux[T, EnvT[V, F, ?]],
    TR: Recursive.Aux[T, EnvT[V, F, ?]]
  ): EnvT[V, F, T] => EnvT[V, F, T] = {
    def strictMerge(x: T, y: T): Option[T] =
      some((x, y).apo[T](mergeƒ[L, V, F, T])) filter { t =>
        I.prj(t.project.lower) all TypeF.union[L, T].isEmpty
      }

    orOriginal(ft => I.prj(ft.lower) collect {
      case TypeF.Unioned(NonEmptyList(h, t)) =>
        val disjoint = t.foldLeft(h.wrapNel.toZipper) { (z, t) =>
          z.findMap(strictMerge(t, _)) getOrElse z.insert(t)
        }

        disjoint.toNel match {
          case NonEmptyList(a, INil()) =>
            a.project

          case NonEmptyList(a, ICons(b, cs)) =>
            envT(ft.ask, I(TypeF.union[L, T](a, b, cs)))
        }
    })
  }

  /** Unfold a StructuralType from a Type. */
  def fromTypeƒ[J: Order, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    TR: Recursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): Coalgebra[ST[J, ?], T] =
    _.project match {
      case TypeF.Const(Embed(E(ejson.Meta(j, Embed(EType(t)))))) =>
        TagST[J](Tagged(t, TypeF.const[J, T](j).embed))

      case other =>
        TypeST(normalization.lowerConst[J, T].apply(other))
    }
}

sealed abstract class StructuralTypeInstances extends StructuralTypeInstances0 {
  import StructuralType.{ST, STF, TypeST, TagST}

  private val MeasureKey = "measure"
  private val StructureKey = "structure"
  private val TagKey = "tag"

  // We can't use final here due to SI-4440 - it results in warning
  private case class TTags[L, A](tags: List[TypeTag], tpe: TypeF[L, A])

  private object TTags {
    def collectTags[V, L]: Transform[Cofree[TTags[L, ?], V], STF[L, V, ?], EnvT[V, TTags[L, ?], ?]] = {
      case EnvT((v, TypeST(tpe))) =>
        envT(v, TTags(Nil, tpe))

      case EnvT((_, TagST(Tagged(t, Embed(EnvT((v, TTags(ts, tpe)))))))) =>
        envT(v, TTags(t :: ts, tpe))
    }

    def disperseTags[V, L]: Transform[Cofree[TTags[L, ?], V], EnvT[V, TTags[L, ?], ?], STF[L, V, ?]] = {
      case EnvT((v, TTags(Nil, tpe))) =>
        envT(v, TypeST(tpe))

      case EnvT((v, TTags(t :: ts, tpe))) =>
        envT(v, TagST[L](Tagged(t, envT(v, TTags(ts, tpe)).embed)))
    }

    implicit def traverse[L]: Traverse[TTags[L, ?]] =
      new Traverse[TTags[L, ?]] {
        def traverseImpl[F[_]: Applicative, A, B](fa: TTags[L, A])(f: A => F[B]) =
          fa.tpe.traverse(f) map (TTags(fa.tags, _))
      }

    implicit def decodeEJsonK[L: DecodeEJson: Order]: DecodeEJsonK[TTags[L, ?]] =
      new DecodeEJsonK[TTags[L, ?]] {
        val decType = DecodeEJsonK[TypeF[L, ?]]

        def decodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]) =
          j => {
            val tags = j.keyS(TagKey).cata(
              tj => tj.decodeAs[TypeTag].map(List(_)) orElse tj.decodeAs[List[TypeTag]],
              List().point[Decoded])

            (tags |@| decType.decodeK[J].apply(j))(TTags(_, _))
          }
      }

    // TODO{ejson}: Lift tags back to metadata once we're using EJson everywhere.
    implicit def encodeEJsonK[L: EncodeEJson]: EncodeEJsonK[TTags[L, ?]] =
      new EncodeEJsonK[TTags[L, ?]] {
        def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]) = {
          case TTags(     Nil, j) => j.asEJsonK
          case TTags(t :: Nil, j) => tagEjs(t.asEJson[J], j.asEJsonK)
          case TTags(     ts , j) => tagEjs(ejson.Fixed[J].arr(ts map (_.asEJson[J])), j.asEJsonK)
        }

        val encType = EncodeEJsonK[TypeF[L, ?]]

        def tagEjs[J](tejs: J, v: J)(
          implicit
          JC: Corecursive.Aux[J, EJson],
          JR: Recursive.Aux[J, EJson]
        ): J = {
          val j = ejson.Fixed[J]
          j.map.modifyOption((j.str(TagKey), tejs) :: _)(v) | j.arr(List(tejs, v))
        }
      }
  }

  implicit def decodeEJson[L: DecodeEJson: Order, V: DecodeEJson]: DecodeEJson[StructuralType[L, V]] =
    new DecodeEJson[StructuralType[L, V]] {
      implicit val decodeEnvT = DecodeEJsonK.envT[V, TTags[L, ?]](MeasureKey, StructureKey)

      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]) =
        j.anaM[Cofree[TTags[L, ?], V]](decodeEnvT.decodeK[J])
          .map(_.transAna[StructuralType[L, V]](TTags.disperseTags))
    }

  implicit def encodeEJson[L: EncodeEJson, V: EncodeEJson]: EncodeEJson[StructuralType[L, V]] =
    new EncodeEJson[StructuralType[L, V]] {
      implicit val encodeEnvT = EncodeEJsonK.envT[V, TTags[L, ?]](MeasureKey, StructureKey)

      def encode[J](st: StructuralType[L, V])(
        implicit
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): J =
        st.transCata[Cofree[TTags[L, ?], V]](TTags.collectTags).cata[J](encodeEnvT.encodeK)
    }

  implicit def corecursive[L, V]: Corecursive.Aux[StructuralType[L, V], STF[L, V, ?]] =
    new Corecursive[StructuralType[L, V]] {
      type Base[B] = STF[L, V, B]

      def embed(st: STF[L, V, StructuralType[L, V]])(implicit BF: Functor[STF[L, V, ?]]) =
        StructuralType(st.map(_.toCofree).embed)
    }

  implicit def recursive[L, V]: Recursive.Aux[StructuralType[L, V], STF[L, V, ?]] =
    new Recursive[StructuralType[L, V]] {
      type Base[B] = STF[L, V, B]

      def project(st: StructuralType[L, V])(implicit BF: Functor[STF[L, V, ?]]) =
        st.toCofree.project map (StructuralType(_))
    }

  implicit def semigroup[L: Order, V: Semigroup]: Semigroup[StructuralType[L, V]] =
    new Semigroup[StructuralType[L, V]] {
      type F[A] = ST[L, A]
      type T    = Cofree[F, V]
      def append(x: StructuralType[L, V], y: => StructuralType[L, V]) =
        StructuralType((x.toCofree, y.toCofree)
          .apo[T](StructuralType.mergeƒ[L, V, F, T])
          .transAna[T](StructuralType.disjoinUnionsƒ[L, V, F, T]))
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
