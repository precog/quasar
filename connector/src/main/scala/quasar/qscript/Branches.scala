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

package quasar.qscript

import quasar.fp.ski.ι

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import simulacrum.typeclass

import scalaz._

// TODO Generalize to a Monocle Traversal, for example:
//
// ```
// trait Branches[F[_]] {
//   def branches[T[_[_]], A]: Traversal[F[A], FreeQS[T]]
// }
//
// Branches[F].branches[T, A].modify(_.transCata(mapBeforeSortCoEnv[T, QScriptTotal[T, ?]])
// ```
//
// This gives us a generic way to target all the branches and all the map funcs.
// Additionally we can compose it with other optics and use other functions on Traversal.
@typeclass trait Branches[F[_]] {
  type IT[F[_]]

  // TODO abstract `Hole`
  type CoEnvF[F[_]] = CoEnv[Hole, F, Free[F, Hole]]

  def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]]): F[A] => F[A]
}

object Branches {

  type Aux[T[_[_]], F[_]] = Branches[F] { type IT[F[_]] = T[F] }

  type CoEnvF[F[_]] = CoEnv[Hole, F, Free[F, Hole]]

  def applyToFreeQS[T[_[_]]](
      alg: CoEnvF[QScriptTotal[T, ?]] => CoEnvF[QScriptTotal[T, ?]]): FreeQS[T] => FreeQS[T] =
    _.transCata[FreeQS[T]](alg)

  implicit def const[T[_[_]], A]: Branches.Aux[T, Const[A, ?]] =
    new Branches[Const[A, ?]] {
      type IT[F[_]] = T[F]

      def run[B](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : Const[A, B] => Const[A, B] = ι
    }

  implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Branches.Aux[T, F],
      G: Branches.Aux[T, G]): Branches.Aux[T, Coproduct[F, G, ?]] =
    new Branches[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : Coproduct[F, G, A] => Coproduct[F, G, A] =
        cp => Coproduct(cp.run.bimap(F.run(alg)(_), G.run(alg)(_)))
    }

  implicit def qscriptCore[T[_[_]]]: Branches.Aux[T, QScriptCore[T, ?]] =
    new Branches[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : QScriptCore[IT, A] => QScriptCore[IT, A] = _ match {
        case Union(src, left, right) =>
          Union(src, applyToFreeQS[IT](alg)(left), applyToFreeQS[IT](alg)(right))
        case Subset(src, from, op, count) =>
          Subset(src, applyToFreeQS[IT](alg)(from), op, applyToFreeQS[IT](alg)(count))
        case qs => qs
      }
    }

  implicit def projectBucket[T[_[_]]]: Branches.Aux[T, ProjectBucket[T, ?]] =
    new Branches[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : ProjectBucket[IT, A] => ProjectBucket[IT, A] = ι
    }

  implicit def thetaJoin[T[_[_]]]: Branches.Aux[T, ThetaJoin[T, ?]] =
    new Branches[ThetaJoin[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : ThetaJoin[IT, A] => ThetaJoin[IT, A] = {
        case ThetaJoin(src, left, right, key, func, combine) =>
          ThetaJoin(
            src,
            applyToFreeQS[IT](alg)(left),
            applyToFreeQS[IT](alg)(right),
            key,
            func,
            combine)
      }
    }

  implicit def equiJoin[T[_[_]]]: Branches.Aux[T, EquiJoin[T, ?]] =
    new Branches[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
        : EquiJoin[IT, A] => EquiJoin[IT, A] = {
        case EquiJoin(src, left, right, key, func, combine) =>
          EquiJoin(
            src,
            applyToFreeQS[IT](alg)(left),
            applyToFreeQS[IT](alg)(right),
            key,
            func,
            combine)
      }
    }
}
