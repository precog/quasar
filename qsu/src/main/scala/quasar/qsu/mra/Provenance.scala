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

import slamdata.Predef.{Boolean, Vector}

import cats.{Applicative, Eq, Monoid, Reducible}
import cats.data.{Const, NonEmptyVector}
import cats.kernel.{BoundedSemilattice, CommutativeMonoid}
import cats.instances.vector._
import cats.syntax.apply._
import cats.syntax.eq._
import cats.syntax.reducible._

import scalaz.@@
import scalaz.Tags.{Disjunction, Conjunction}
import scalaz.syntax.tag._

trait Provenance[S, V, T] { self =>
  type P

  /** Apply `f` to cross product of the independent components of `ps`,
    * 'or'-ing the results.
    *
    * Each argument to `f` will have the same size as `ps`, if any `ps` are
    * empty, their corresponsing position in the `NonEmptyVector` will also be
    * empty.
    */
  def applyComponentsN[F[_]: Applicative, G[_]: Reducible](
      f: NonEmptyVector[P] => F[P])(
      ps: G[P])
      : F[P] = {

    def nev(p: P): NonEmptyVector[P] =
      NonEmptyVector.fromVector(foldMapComponents(Vector(_))(p))
        .getOrElse(NonEmptyVector.one(empty))

    def cross(in: NonEmptyVector[NonEmptyVector[P]], p: P)
        : NonEmptyVector[NonEmptyVector[P]] =
      nev(p).flatMap(p => in.map(_ :+ p))

    val crossed =
      ps.reduceLeftTo(p => nev(p).map(NonEmptyVector.one(_)))(cross)

    crossed.reduceLeftTo(f)((x, y) => (x, f(y)).mapN(or))
  }

  /** The set of identity comparisons describing an autojoin of `l` and `r`. */
  def autojoin(l: P, r: P): AutoJoin[S, V]

  /** The conjunction of two provenance, representing a dataset where values
    * have identities from both inputs.
    */
  def and(l: P, r: P): P

  /** Provenance having a dimensionality of zero. */
  def empty: P

  /** Convert each component into `A` and combine using its monoid. */
  def foldMapComponents[A: Monoid](f: P => A)(p: P): A =
    traverseComponents[Const[A, ?]](p => Const(f(p)))(p).getConst

  /** Convert each scalar id into `A` and combine using its monoid. */
  def foldMapScalarIds[A: Monoid](f: (S, T) => A)(p: P): A =
    traverseScalarIds[Const[A, ?]]((s, t) => Const(f(s, t)))(p).getConst

  /** Convert each vector id into `A` and combine using its monoid. */
  def foldMapVectorIds[A: Monoid](f: (V, T) => A)(p: P): A =
    traverseVectorIds[Const[A, ?]]((v, t) => Const(f(v, t)))(p).getConst

  /** Append an identity, maintaining current dimensionality. */
  def inflateConjoin(vectorId: V, sort: T, p: P): P

  /** Append an identity, increasing dimensionality by 1. */
  def inflateExtend(vectorId: V, sort: T, p: P): P

  /** "Submerge" an identity, making it the second-highest dimension,
    * increasing dimensionality by 1.
    */
  def inflateSubmerge(vectorId: V, sort: T, p: P): P

  /** Inject into a structure at an unknown field, maintains dimensionality. */
  def injectDynamic(p: P): P

  /** Inject into a structure at the given field, maintains dimensionality. */
  def injectStatic(scalarId: S, sort: T, p: P): P

  /** The disjunction of two provenance, representing a dataset where values
    * have identities from either of the inputs.
    */
  def or(l: P, r: P): P

  /** Project an unknown field, maintains dimensionality. */
  def projectDynamic(p: P): P

  /** Project a statically known field, maintains dimensionality. */
  def projectStatic(scalarId: S, sort: T, p: P): P

  /** Discard the highest dimension, reducing dimensionality by 1. */
  def reduce(p: P): P

  /** Conjoin all dimensions into a single one. */
  def squash(p: P): P

  /** Apply `f` to each independent component of `p`. */
  def traverseComponents[F[_]: Applicative](f: P => F[P])(p: P): F[P]

  /** Apply `f` to each scalar id of `p`. */
  def traverseScalarIds[F[_]: Applicative](f: (S, T) => F[(S, T)])(p: P): F[P]

  /** Apply `f` to each vector id of `p`. */
  def traverseVectorIds[F[_]: Applicative](f: (V, T) => F[(V, T)])(p: P): F[P]

  object instances {
    implicit val pConjunctionCommutativeMonoid: CommutativeMonoid[P @@ Conjunction] =
      new CommutativeMonoid[P @@ Conjunction] {
        val empty = Conjunction(self.empty)

        def combine(x: P @@ Conjunction, y: P @@ Conjunction) =
          Conjunction(and(x.unwrap, y.unwrap))
      }

    implicit val pDisjunctionSemilattice: BoundedSemilattice[P @@ Disjunction] =
      new BoundedSemilattice[P @@ Disjunction] {
        val empty = Disjunction(self.empty)

        def combine(x: P @@ Disjunction, y: P @@ Disjunction) =
          Disjunction(or(x.unwrap, y.unwrap))
      }
  }

  object syntax {
    implicit final class ProvenanceOps(p: P) {
      def ⋈ (that: P): AutoJoin[S, V] =
        self.autojoin(p, that)

      def ∧ (that: P): P =
        self.and(p, that)

      def ∨ (that: P): P =
        self.or(p, that)

      def foldMapComponents[A: Monoid](f: P => A): A =
        self.foldMapComponents(f)(p)

      def foldMapScalarIds[A: Monoid](f: (S, T) => A): A =
        self.foldMapScalarIds(f)(p)

      def foldMapVectorIds[A: Monoid](f: (V, T) => A): A =
        self.foldMapVectorIds(f)(p)

      def inflateConjoin(vectorId: V, sort: T): P =
        self.inflateConjoin(vectorId, sort, p)

      def inflateExtend(vectorId: V, sort: T): P =
        self.inflateExtend(vectorId, sort, p)

      def inflateSubmerge(vectorId: V, sort: T): P =
        self.inflateSubmerge(vectorId, sort, p)

      def injectDynamic: P =
        self.injectDynamic(p)

      def injectStatic(scalarId: S, sort: T): P =
        self.injectStatic(scalarId, sort, p)

      def isEmpty(implicit P: Eq[P]): Boolean =
        p === self.empty

      def projectDynamic: P =
        self.projectDynamic(p)

      def projectStatic(scalarId: S, sort: T): P =
        self.projectStatic(scalarId, sort, p)

      def reduce: P =
        self.reduce(p)

      def squash: P =
        self.squash(p)

      def traverseComponents[F[_]: Applicative](f: P => F[P]): F[P] =
        self.traverseComponents(f)(p)

      def traverseScalarIds[F[_]: Applicative](f: (S, T) => F[(S, T)]): F[P] =
        self.traverseScalarIds(f)(p)

      def traverseVectorIds[F[_]: Applicative](f: (V, T) => F[(V, T)]): F[P] =
        self.traverseVectorIds(f)(p)
    }
  }
}

object Provenance {
  type Aux[S, V, T, P0] = Provenance[S, V, T] { type P = P0 }
}
