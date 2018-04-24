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

package quasar.qscript

import matryoshka._
import matryoshka.patterns._

import monocle._
import scalaz._, Scalaz._

trait Branches[T[_[_]], IN[_]] {
  def branches[A]: Traversal[IN[A], FreeQS[T]]
}

object Branches {

  implicit def const[T[_[_]], C]: Branches[T, Const[C, ?]] =
    new Branches[T, Const[C, ?]] {
      def branches[A]: Traversal[Const[C, A], FreeQS[T]] =
        new Traversal[Const[C, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: Const[C, A]): F[Const[C, A]] =
            Applicative[F].pure(s)
        }
    }

  implicit def coproduct[T[_[_]], G[_], H[_]]
    (implicit G: Branches[T, G], H: Branches[T, H])
      : Branches[T, Coproduct[G, H, ?]] =
    new Branches[T, Coproduct[G, H, ?]] {
      def branches[A]: Traversal[Coproduct[G, H, A], FreeQS[T]] =
        new Traversal[Coproduct[G, H, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: Coproduct[G, H, A]): F[Coproduct[G, H, A]] = {
            s.run.bitraverse[F, G[A], H[A]](
              G.branches.modifyF(f),
              H.branches.modifyF(f)
            ).map(Coproduct(_))
          }
        }
    }

  implicit def qscriptCore[T[_[_]]]: Branches[T, QScriptCore[T, ?]] =
    new Branches[T, QScriptCore[T, ?]] {
      def branches[A]: Traversal[QScriptCore[T, A], FreeQS[T]] =
        new Traversal[QScriptCore[T, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: QScriptCore[T, A]): F[QScriptCore[T, A]] =
            s match {
              case Union(src, left, right) =>
                (f(left) |@| f(right))(Union(src, _, _))
              case Subset(src, from, op, count) =>
                (f(from) |@| f(count))(Subset(src, _, op, _))
              case qs => Applicative[F].pure(qs)
            }
        }
    }

  implicit def projectBucket[T[_[_]]]: Branches[T, ProjectBucket[T, ?]] =
    new Branches[T, ProjectBucket[T, ?]] {
      def branches[A]: Traversal[ProjectBucket[T, A], FreeQS[T]] =
        new Traversal[ProjectBucket[T, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: ProjectBucket[T, A]): F[ProjectBucket[T, A]] =
            Applicative[F].pure(s)
        }
    }

  implicit def thetaJoin[T[_[_]]]: Branches[T, ThetaJoin[T, ?]] =
    new Branches[T, ThetaJoin[T, ?]] {
      def branches[A]:Traversal[ThetaJoin[T, A], FreeQS[T]] =
        new Traversal[ThetaJoin[T, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: ThetaJoin[T, A]): F[ThetaJoin[T, A]] =
            s match {
              case ThetaJoin(src, left, right, key, func, combine) =>
                (f(left) |@| f(right))(ThetaJoin(src, _, _, key, func, combine))
            }
        }
    }

  implicit def equiJoin[T[_[_]]]: Branches[T, EquiJoin[T, ?]] =
    new Branches[T, EquiJoin[T, ?]] {
      def branches[A]: Traversal[EquiJoin[T, A], FreeQS[T]] =
        new Traversal[EquiJoin[T, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: EquiJoin[T, A]): F[EquiJoin[T, A]] = {
            s match {
              case EquiJoin(src, left, right, key, func, combine) =>
                (f(left) |@| f(right))(EquiJoin(src, _, _, key, func, combine))
            }
          }
        }
    }

  implicit def coEnv[T[_[_]], E, G[_]](implicit G: Branches[T, G]): Branches[T, CoEnv[E, G, ?]] =
    new Branches[T, CoEnv[E, G, ?]] {
      def branches[A]: Traversal[CoEnv[E, G, A], FreeQS[T]] =
        new Traversal[CoEnv[E, G, A], FreeQS[T]] {
          def modifyF[F[_]: Applicative](f: FreeQS[T] => F[FreeQS[T]])(s: CoEnv[E, G, A]): F[CoEnv[E, G, A]] =
            s.run.traverse(G.branches.modifyF(f)).map(CoEnv(_))
        }
    }

  def apply[T[_[_]], F[_]](implicit ev: Branches[T, F]): Branches[T, F] = ev
}
