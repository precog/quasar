/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu.mra

import slamdata.Predef.{None, Option, Some}

import quasar.RenderTree, RenderTree.ops._
import quasar.contrib.cats.boolean._
import quasar.qscript.OnUndefined

import cats.{Applicative, Order, Semigroup, Show}
import cats.data.{Ior, NonEmptyList}
import cats.implicits._

import monocle.Traversal

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

trait ProvImpl[S, V, T] extends Provenance[S, V, T] {

  import ProvImpl.Vecs

  implicit def sOrder: Order[S]
  implicit def vOrder: Order[V]
  implicit def tOrder: Order[T]

  type D = Dim[S, V, T]
  private val D = Dim.Optics[S, V, T]

  type P = Uop[Vecs[D]]

  def autojoin(l: P, r: P): AutoJoin[S, V] = {
    import JoinKeys._
    import shims.monoidToCats

    def join(x: Dim[S, V, T], y: Dim[S, V, T]): Option[JoinKeys[S, V] @@ Conjunction] =
      Conjunction.subst((x, y) match {
        case (D.inflate(xv, xt), D.inflate(yv, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.dynamic(xv, yv)))

        case (D.inflate(xv, xt), D.project(ys, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.staticR(xv, ys)))

        case (D.project(xs, xt), D.inflate(yv, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.staticL(xs, yv)))

        case (xp @ D.project(_, _), yp @ D.project(_, _)) if xp === yp =>
          Some(JoinKeys.empty)

        case _ => None
      })

    val joins = for {
      lids <- l.toSortedSet
      rids <- r.toSortedSet
    } yield lids.united.zipWithDefined(rids.united)(join)

    val (Disjunction(keys), Disjunction(shouldEmit)) =
      joins unorderedFoldMap {
        case Conjunction(jks) => (Disjunction(jks), Disjunction(jks.isEmpty))
      }

    if (shouldEmit && !keys.isEmpty)
      AutoJoin(keys, OnUndefined.Emit)
    else
      AutoJoin(keys, OnUndefined.Omit)
  }

  def and(l: P, r: P): P =
    l ∧ r

  def empty: P =
    Uop.empty

  def inflateConjoin(vectorId: V, sort: T, p: P): P =
    p.map(_.mapActive(_ :≻ D.inflate(vectorId, sort)))

  def inflateExtend(vectorId: V, sort: T, p: P): P = {
    val n = D.inflate(vectorId, sort)

    if (p.isEmpty)
      Uop.one(Vecs.active(Identities(n)))
    else
      p.map(_.mapActive(_ :+ n))
  }

  def inflateSubmerge(vectorId: V, sort: T, p: P): P =
    p.map(_.mapActive(_.submerge(D.inflate(vectorId, sort))))

  def injectDynamic(p: P): P =
    createOrConj(D.fresh(), p)

  def injectStatic(scalarId: S, sort: T, p: P): P =
    createOrConj(D.inject(scalarId, sort), p)

  def or(l: P, r: P): P =
    l ∨ r

  def projectDynamic(p: P): P =
    createOrConj(D.fresh(), p)

  def projectStatic(scalarId: S, sort: T, p: P): P = {
    def applyVec(v: NonEmptyList[NonEmptyList[Dim[S, V, T]]]): Option[Vecs[D]] = {
      val r = v.reverse
      val rh = r.head.reverse

      val (newh, isActive) =
        if (rh.head === D.inject(scalarId, sort))
          (rh.tail, true)
        else if (D.inject.isEmpty(rh.head))
          (D.project(scalarId, sort) :: rh.toList, true)
        else
          (rh.toList.dropWhile(D.inject.nonEmpty), false)

      val vecs =
        newh.reverse.toNel match {
          case Some(h) => Some(NonEmptyList(h, r.tail).reverse)
          case None => r.tail.reverse.toNel
        }

      vecs map { v =>
        val ids = Identities.collapsed[cats.Id, Dim[S, V, T]](v)

        if (isActive)
          Vecs.active(ids)
        else
          Vecs.terminal(ids)
      }
    }

    def applyVecs(vecs: Vecs[D]): Option[Vecs[D]] =
      vecs.active match {
        case Some(a) if a.lastValues.forall(D.inject.isEmpty) =>
          Some(vecs.setActive(a :≻ D.project(scalarId, sort)))

        case Some(a) =>
          a.expanded.toList
            .foldMap(applyVec(_))
            .combine(vecs.terminal.map(Vecs.terminal(_)))

        case None =>
          Some(vecs)
      }

    if (p.isEmpty)
      Uop.one(Vecs.active(Identities(D.project(scalarId, sort))))
    else
      Uop.fromFoldable(p.toList.flatMap(applyVecs(_).toList))
  }

  def reduce(p: P): P =
    Uop.fromFoldable(p.toList.flatMap(_.toIor match {
      case Ior.Left(t) =>
        t.initRegions.map(Vecs.terminal(_)).toList

      case Ior.Right(a) =>
        a.initRegions.map(Vecs.active(_)).toList

      case Ior.Both(t, a) =>
        (
          t.initRegions.map(Vecs.terminal(_)) |+|
          a.initRegions.map(Vecs.active(_))
        ).toList
    }))

  def squash(p: P): P =
    p.map(Vecs.identities.modify(_.squash))

  def traverseComponents[F[_]: Applicative](f: P => F[P])(p: P): F[P] = {
    import Uop._
    p.toList
      .traverse(ids => f(Uop.one(ids)))
      .map(ps => Disjunction.subst(ps).combineAll.unwrap)
  }

  def traverseScalarIds[F[_]: Applicative](f: (S, T) => F[(S, T)])(p: P): F[P] = {
    import shims.applicativeToScalaz
    scalarIds.modifyF[F](f.tupled)(p)
  }

  def traverseVectorIds[F[_]: Applicative](f: (V, T) => F[(V, T)])(p: P): F[P] = {
    import shims.applicativeToScalaz
    vectorIds.modifyF[F](f.tupled)(p)
  }

  ////

  // Not totally lawful, so private
  private lazy val UopT =
    new Traversal[P, Vecs[D]] {
      import shims.applicativeToCats
      def modifyF[F[_]: scalaz.Applicative](f: Vecs[D] => F[Vecs[D]])(p: P): F[P] =
        p.toList.traverse(f).map(Uop.fromFoldable(_))
    }

  private lazy val scalarIds: Traversal[P, (S, T)] =
    UopT composeTraversal Vecs.identities composeTraversal Identities.values composeTraversal D.scalar

  private lazy val vectorIds: Traversal[P, (V, T)] =
    UopT composeTraversal Vecs.identities composeTraversal Identities.values composePrism D.inflate

  private def createOrConj(d: Dim[S, V, T], p: P): P =
    if (p.isEmpty)
      Uop.one(Vecs.active(Identities(d)))
    else
      p.map(_.mapActive(_ :≻ d))
}

object ProvImpl {
  def apply[S, V, T](implicit sord: Order[S], vord: Order[V], tord: Order[T])
      : Provenance.Aux[S, V, T, Uop[Vecs[Dim[S, V, T]]]] =
    new ProvImpl[S, V, T] {
      val sOrder = sord
      val vOrder = vord
      val tOrder = tord
    }

  final case class Vecs[A](toIor: Ior[Identities[A], Identities[A]]) extends scala.AnyVal {
    def active: Option[Identities[A]] =
      toIor.right

    def combine(other: Vecs[A])(implicit A: Order[A]): Vecs[A] =
      Vecs(toIor |+| other.toIor)

    def mapActive(f: Identities[A] => Identities[A]): Vecs[A] =
      Vecs(toIor map f)

    def mapTerminal(f: Identities[A] => Identities[A]): Vecs[A] =
      Vecs(toIor leftMap f)

    def setActive(ids: Identities[A]): Vecs[A] =
      Vecs(toIor.putRight(ids))

    def setTerminal(ids: Identities[A]): Vecs[A] =
      Vecs(toIor.putLeft(ids))

    def terminal: Option[Identities[A]] =
      toIor.left

    def united(implicit A: Order[A]): Identities[A] =
      toIor.merge
  }

  object Vecs {
    def active[A](ids: Identities[A]): Vecs[A] =
      Vecs(Ior.Right(ids))

    def terminal[A](ids: Identities[A]): Vecs[A] =
      Vecs(Ior.Left(ids))

    def identities[A]: Traversal[Vecs[A], Identities[A]] =
      new Traversal[Vecs[A], Identities[A]] {
        import shims.applicativeToCats
        def modifyF[F[_]: scalaz.Applicative](f: Identities[A] => F[Identities[A]])(v: Vecs[A]): F[Vecs[A]] =
          v.toIor.bitraverse(f, f).map(Vecs(_))
      }

    implicit def order[A: Order]: Order[Vecs[A]] =
      Order.by(_.united)

    implicit def renderTree[A: Order: Show]: RenderTree[Vecs[A]] =
      RenderTree.make(_.united.render)

    implicit def semigroup[A: Order]: Semigroup[Vecs[A]] =
      Semigroup.instance(_ combine _)

    implicit def show[A: Order: Show]: Show[Vecs[A]] =
      Show[Identities[A]].contramap(_.united)
  }
}
