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

package quasar.qsu.mra

import slamdata.Predef.{None, Option, Some}

import quasar.contrib.cats.boolean._
import quasar.qscript.OnUndefined

import cats.{Applicative, Order}
import cats.data.NonEmptyList
import cats.instances.list._
import cats.instances.set._
import cats.instances.tuple._
import cats.syntax.eq._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.list._
import cats.syntax.traverse._

import monocle.Traversal

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

trait ProvImpl[S, V, T] extends Provenance[S, V, T] {

  implicit def sOrder: Order[S]
  implicit def vOrder: Order[V]
  implicit def tOrder: Order[T]

  private val D = Dim.Optics[S, V, T]

  type P = Uop[Identities[Dim[S, V, T]]]

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
    } yield lids.zipWithDefined(rids)(join)

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
    p.map(_ :≻ D.inflate(vectorId, sort))

  def inflateExtend(vectorId: V, sort: T, p: P): P = {
    val n = D.inflate(vectorId, sort)

    if (p.isEmpty)
      Uop.one(Identities(n))
    else
      p.map(_ :+ n)
  }

  def inflateSubmerge(vectorId: V, sort: T, p: P): P =
    p.map(_.submerge(D.inflate(vectorId, sort)))

  def injectDynamic(p: P): P =
    createOrConj(D.fresh(), p)

  def injectStatic(scalarId: S, sort: T, p: P): P =
    createOrConj(D.inject(scalarId, sort), p)

  def or(l: P, r: P): P =
    l ∨ r

  def projectDynamic(p: P): P =
    createOrConj(D.fresh(), p)

  def projectStatic(scalarId: S, sort: T, p: P): P = {
    def applyVec(v: NonEmptyList[NonEmptyList[Dim[S, V, T]]]): Option[NonEmptyList[NonEmptyList[Dim[S, V, T]]]] = {
      val r = v.reverse
      val rh = r.head.reverse

      val newh =
        if (rh.head === D.inject(scalarId, sort))
          rh.tail
        else
          rh.toList.dropWhile(D.inject.nonEmpty)

      newh.reverse.toNel match {
        case Some(h) => Some(NonEmptyList(h, r.tail).reverse)
        case None => r.tail.reverse.toNel
      }
    }

    def applyIds(ids: Identities[Dim[S, V, T]]): Option[Identities[Dim[S, V, T]]] =
      if (ids.lastValues.forall(D.inject.isEmpty))
        Some(ids :≻ D.project(scalarId, sort))
      else
        ids.expanded.toList
          .flatMap(applyVec(_).toList)
          .toNel
          .map(Identities.collapsed(_))

    if (p.isEmpty)
      Uop.one(Identities(D.project(scalarId, sort)))
    else
      Uop.fromFoldable(p.toList.flatMap(applyIds(_).toList))
  }

  def reduce(p: P): P =
    Uop.fromFoldable(p.toList.flatMap(_.initRegions.toList))

  def squash(p: P): P =
    p.map(_.squash)

  def traverseComponents[F[_]: Applicative](f: P => F[P])(p: P): F[P] = {
    import Uop._
    p.toList
      .traverse(ids => f(Uop.one(ids)))
      .map(ps => Disjunction.subst(ps).combineAll.unwrap)
  }

  def traverseScalarIds[F[_]: Applicative](f: (S, T) => F[(S, T)])(p: P): F[P] = {
    import shims._
    scalarIds.modifyF[F](f.tupled)(p)
  }

  def traverseVectorIds[F[_]: Applicative](f: (V, T) => F[(V, T)])(p: P): F[P] = {
    import shims._
    vectorIds.modifyF[F](f.tupled)(p)
  }

  ////

  private type J = Identities[Dim[S, V, T]]

  // Not totally lawful, so private
  private lazy val UopT =
    new Traversal[P, J] {
      import shims._
      def modifyF[F[_]: scalaz.Applicative](f: J => F[J])(p: P): F[P] =
        p.toList.traverse(f).map(Uop.fromFoldable(_))
    }

  private lazy val scalarIds: Traversal[P, (S, T)] =
    UopT composeTraversal Identities.values composeTraversal D.scalar

  private lazy val vectorIds: Traversal[P, (V, T)] =
    UopT composeTraversal Identities.values composePrism D.inflate

  private def createOrConj(d: Dim[S, V, T], p: P): P =
    if (p.isEmpty)
      Uop.one(Identities(d))
    else
      p.map(_ :≻ d)
}

object ProvImpl {
  def apply[S, V, T](implicit sord: Order[S], vord: Order[V], tord: Order[T])
      : Provenance.Aux[S, V, T, Uop[Identities[Dim[S, V, T]]]] =
    new ProvImpl[S, V, T] {
      val sOrder = sord
      val vOrder = vord
      val tOrder = tord
    }
}
