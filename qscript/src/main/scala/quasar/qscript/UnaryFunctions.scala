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
import quasar.qscript.RecFreeS.RecOps

import monocle._
import scalaz._, Scalaz._

// TODO look into traversing FreeQS as well to simplify rewrites like
// assumeReadType
trait UnaryFunctions[T[_[_]], IN[_]] {
  def unaryFunctions[A]: Traversal[IN[A], FreeMap[T]]
}

object UnaryFunctions {

  implicit def const[T[_[_]], C]: UnaryFunctions[T, Const[C, ?]] =
    new UnaryFunctions[T, Const[C, ?]] {
      def unaryFunctions[A]: Traversal[Const[C, A], FreeMap[T]] =
        new Traversal[Const[C, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: Const[C, A]): F[Const[C, A]] =
            Applicative[F].pure(s)
        }
    }

  implicit def coproduct[T[_[_]], G[_], H[_]]
    (implicit G: UnaryFunctions[T, G], H: UnaryFunctions[T, H])
      : UnaryFunctions[T, Coproduct[G, H, ?]] =
    new UnaryFunctions[T, Coproduct[G, H, ?]] {
      def unaryFunctions[A]: Traversal[Coproduct[G, H, A], FreeMap[T]] =
        new Traversal[Coproduct[G, H, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: Coproduct[G, H, A]): F[Coproduct[G, H, A]] = {
            s.run.bitraverse[F, G[A], H[A]](
              G.unaryFunctions.modifyF(f),
              H.unaryFunctions.modifyF(f)
            ).map(Coproduct(_))
          }
        }
    }

  implicit def qscriptCore[T[_[_]]]: UnaryFunctions[T, QScriptCore[T, ?]] =
    new UnaryFunctions[T, QScriptCore[T, ?]] {
      def unaryFunctions[A]: Traversal[QScriptCore[T, A], FreeMap[T]] =
        new Traversal[QScriptCore[T, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: QScriptCore[T, A]): F[QScriptCore[T, A]] =
            s match {
              case Map(src, fm) =>
                (f(fm.linearize)).map(_.asRec).map(Map(src, _))
              case LeftShift(src, struct, id, stpe, undef, repair) =>
                (f(struct.linearize).map(_.asRec)).map(LeftShift(src, _, id, stpe, undef, repair))
              case Reduce(src, bucket, red, repair) =>
                (bucket.traverse(f) |@| red.traverse(_.traverse(f)))(Reduce(src, _, _, repair))
              case Sort(src, bucket, order) =>
                (bucket.traverse(f) |@| order.traverse(t => f(t._1).map(x => (x, t._2))))(Sort(src, _, _))
              case Union(src, lBranch, rBranch) =>
                Applicative[F].pure(s)
              case Filter(src, fm) =>
                (f(fm.linearize)).map(_.asRec).map(Filter(src, _))
              case Subset(src, from, sel, count) =>
                Applicative[F].pure(s)
              case Unreferenced() =>
                Applicative[F].pure(s)
            }
        }
    }

  implicit def projectBucket[T[_[_]]]: UnaryFunctions[T, ProjectBucket[T, ?]] =
    new UnaryFunctions[T, ProjectBucket[T, ?]] {
      def unaryFunctions[A]: Traversal[ProjectBucket[T, A], FreeMap[T]] =
        new Traversal[ProjectBucket[T, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: ProjectBucket[T, A]): F[ProjectBucket[T, A]] =
            s match {
              case BucketKey(src, value, name) =>
                (f(value) |@| f(name))(BucketKey(src, _, _))
              case BucketIndex(src, value, index) =>
                (f(value) |@| f(index))(BucketIndex(src, _, _))
            }
        }
    }

  implicit def shiftedRead[T[_[_]]]: UnaryFunctions[T, ShiftedRead] =
    new UnaryFunctions[T,  ShiftedRead] {
      def unaryFunctions[A]: Traversal[ShiftedRead[A], FreeMap[T]] =
        new Traversal[ShiftedRead[A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: ShiftedRead[A]): F[ShiftedRead[A]] =
            Applicative[F].pure(s)
        }
    }

  implicit def thetaJoin[T[_[_]]]: UnaryFunctions[T, ThetaJoin[T, ?]] =
    new UnaryFunctions[T, ThetaJoin[T, ?]] {
      def unaryFunctions[A]:Traversal[ThetaJoin[T, A], FreeMap[T]] =
        new Traversal[ThetaJoin[T, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: ThetaJoin[T, A]): F[ThetaJoin[T, A]] =
            s match {
              case ThetaJoin(src, left, right, key, func, combine) =>
                Applicative[F].pure(s)
            }
        }
    }

  implicit def equiJoin[T[_[_]]]: UnaryFunctions[T, EquiJoin[T, ?]] =
    new UnaryFunctions[T, EquiJoin[T, ?]] {
      def unaryFunctions[A]: Traversal[EquiJoin[T, A], FreeMap[T]] =
        new Traversal[EquiJoin[T, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: EquiJoin[T, A]): F[EquiJoin[T, A]] = {
            s match {
              case EquiJoin(src, left, right, key, func, combine) =>
                Applicative[F].pure(s)
            }
          }
        }
    }

  implicit def coEnv[T[_[_]], E, G[_]](implicit G: UnaryFunctions[T, G]): UnaryFunctions[T, CoEnv[E, G, ?]] =
    new UnaryFunctions[T, CoEnv[E, G, ?]] {
      def unaryFunctions[A]: Traversal[CoEnv[E, G, A], FreeMap[T]] =
        new Traversal[CoEnv[E, G, A], FreeMap[T]] {
          def modifyF[F[_]: Applicative](f: FreeMap[T] => F[FreeMap[T]])(s: CoEnv[E, G, A]): F[CoEnv[E, G, A]] =
            s.run.traverse(G.unaryFunctions.modifyF(f)).map(CoEnv(_))
        }
    }

  def apply[T[_[_]], F[_]](implicit ev: UnaryFunctions[T, F]): UnaryFunctions[T, F] = ev
}
