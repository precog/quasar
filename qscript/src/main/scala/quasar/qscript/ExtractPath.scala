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

import slamdata.Predef._
import quasar.fp.ski.κ

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import shapeless.Lazy

/** Extracts paths of particular type from QScript, collecting them in the
  * provided `ApplicativePlus`.
  */
trait ExtractPath[F[_], P] {
  def extractPath[G[_]: ApplicativePlus]: Algebra[F, G[P]]
}

object ExtractPath extends ExtractPathInstances {
  def apply[F[_], P](implicit instance: ExtractPath[F, P]): ExtractPath[F, P] = instance
}

sealed abstract class ExtractPathInstances extends ExtractPathInstances0 {
  implicit def coproduct[F[_], G[_], P](
    implicit
    F: Lazy[ExtractPath[F, P]],
    G: Lazy[ExtractPath[G, P]]
  ): ExtractPath[Coproduct[F, G, ?], P] =
    new ExtractPath[Coproduct[F, G, ?], P] {
      def extractPath[H[_]: ApplicativePlus]: Algebra[Coproduct[F, G, ?], H[P]] =
        _.run.fold(F.value.extractPath[H], G.value.extractPath[H])
    }

  implicit def constRead[A, P >: A]: ExtractPath[Const[Read[A], ?], P] =
    new ExtractPath[Const[Read[A], ?], P] {
      def extractPath[G[_]: ApplicativePlus] = {
        case Const(Read(a)) => (a: P).point[G]
      }
    }

  implicit def constShiftedRead[A, P >: A]: ExtractPath[Const[ShiftedRead[A], ?], P] =
    new ExtractPath[Const[ShiftedRead[A], ?], P] {
      def extractPath[G[_]: ApplicativePlus] = {
        case Const(ShiftedRead(a, _)) => (a: P).point[G]
      }
    }

  implicit def equiJoin[T[_[_]]: RecursiveT, P](
    implicit QST: Lazy[ExtractPath[QScriptTotal[T, ?], P]]
  ): ExtractPath[EquiJoin[T, ?], P] =
    new ExtractPath[EquiJoin[T, ?], P] {
      def extractPath[G[_]: ApplicativePlus] = {
        case EquiJoin(paths, l, r, _, _, _) =>
          extractBranch[T, G, P](l) <+> extractBranch[T, G, P](r) <+> paths
      }
    }

  implicit def qScriptCore[T[_[_]]: RecursiveT, P](
    implicit QST: Lazy[ExtractPath[QScriptTotal[T, ?], P]]
  ): ExtractPath[QScriptCore[T, ?], P] =
    new ExtractPath[QScriptCore[T, ?], P] {
      def extractPath[G[_]: ApplicativePlus] = {
        case Filter(paths, _)                => paths
        case LeftShift(paths, _, _, _, _, _) => paths
        case Map(paths, _)                   => paths
        case Reduce(paths, _, _, _)          => paths
        case Sort(paths, _, _)               => paths
        case Subset(paths, fm, _, ct)        => extractBranch[T, G, P](fm) <+> extractBranch[T, G, P](ct) <+> paths
        case Union(paths, l, r)              => extractBranch[T, G, P](l)  <+> extractBranch[T, G, P](r)  <+> paths
        case Unreferenced()                  => mempty[G, P]
      }
    }

  implicit def thetaJoin[T[_[_]]: RecursiveT, P](
    implicit QST: Lazy[ExtractPath[QScriptTotal[T, ?], P]]
  ): ExtractPath[ThetaJoin[T, ?], P] =
    new ExtractPath[ThetaJoin[T, ?], P] {
      def extractPath[G[_]: ApplicativePlus] = {
        case ThetaJoin(paths, l, r, _, _, _) =>
          extractBranch[T, G, P](l) <+> extractBranch[T, G, P](r) <+> paths
      }
    }

  ////

  private def extractBranch[T[_[_]]: RecursiveT, G[_]: ApplicativePlus, P](
    branch: FreeQS[T]
  )(implicit
    QST: Lazy[ExtractPath[QScriptTotal[T, ?], P]]
  ): G[P] = branch.cata(interpret(κ(mempty[G, P]), QST.value.extractPath[G]))
}

sealed abstract class ExtractPathInstances0 {
  import quasar.contrib.pathy.APath

  // NB: This is explicit here so we can hide the instances for `DeadEnd` and
  //     `ProjectBucket` as we don't want to be able to extract paths from
  //     QScript if it includes them, however QScriptTotal Knows All, so we
  //     must define them for it.
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def qScriptTotalPath[T[_[_]]: RecursiveT]: Lazy[ExtractPath[QScriptTotal[T, ?], APath]] = {
    implicit val constDeadEnd: ExtractPath[Const[DeadEnd, ?], APath] =
      new ExtractPath[Const[DeadEnd, ?], APath] {
        def extractPath[G[_]: ApplicativePlus] = {
          case Const(Root) => mempty[G, APath]
        }
      }

    implicit val projectBucket: ExtractPath[ProjectBucket[T, ?], APath] =
      new ExtractPath[ProjectBucket[T, ?], APath] {
        def extractPath[G[_]: ApplicativePlus] = {
          case BucketKey(paths, _, _) => paths
          case BucketIndex(paths, _, _) => paths
        }
      }

    Lazy(ExtractPath[QScriptTotal[T, ?], APath])
  }
}
