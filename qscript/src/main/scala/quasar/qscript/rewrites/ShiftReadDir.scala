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

package quasar.qscript.rewrites

import slamdata.Predef._


import quasar.contrib.pathy.AFile
import quasar.contrib.iota.{copkTraverse, mkInject}
import quasar.fp.{coenvPrism, :<<:, ACopK}
import quasar.qscript._
import quasar.qscript.rewrites.ShiftRead.FixFreeH

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** Like [[ShiftRead]], but only applies when the path is a directory, leaving
  * file reads untouched.
  */
trait ShiftReadDir[F[_]] {
  type G[A]
  def shiftReadDir[H[_]](GtoH: G ~> H): F ~> FixFreeH[H, ?]
}

object ShiftReadDir extends ShiftReadDirInstances {
  type Aux[T[_[_]], F[_], H[_]] = ShiftReadDir[F] { type G[A] = H[A] }

  def apply[T[_[_]], F[_], G[_]](implicit ev: Aux[T, F, G]) = ev
}

sealed abstract class ShiftReadDirInstances extends ShiftReadDirInstances0 {
  implicit def readFile[T[_[_]], F[a] <: ACopK[a]](
    implicit RF: Const[Read[AFile], ?] :<<: F
  ): ShiftReadDir.Aux[T, Const[Read[AFile], ?], F] =
    new ShiftReadDir[Const[Read[AFile], ?]] {
      type G[A] = F[A]
      def shiftReadDir[H[_]](GtoH: G ~> H) =
        λ[Const[Read[AFile], ?] ~> FixFreeH[H, ?]](fa => GtoH(RF inj (fa map Free.point)))
    }

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](
    implicit QC: QScriptCore[T, ?] :<<: F
  ): ShiftReadDir.Aux[T, QScriptCore[T, ?], F] =
    new ShiftReadDir[QScriptCore[T, ?]] {
      type G[A] = F[A]
      def shiftReadDir[H[_]](GtoH: G ~> H) = λ[QScriptCore[T, ?] ~> FixFreeH[H, ?]](qc =>
        GtoH(QC.inj(qc match {
          case Union(src, lb, rb) =>
            Union(Free.point(src), applyToBranch(lb), applyToBranch(rb))
          case Subset(src, lb, sel, rb) =>
            Subset(Free.point(src), applyToBranch(lb), sel, applyToBranch(rb))
          case _ => qc.map(Free.point)
        }))
      )
    }

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](
    implicit TJ: ThetaJoin[T, ?] :<<: F
  ): ShiftReadDir.Aux[T, ThetaJoin[T, ?], F] =
    new ShiftReadDir[ThetaJoin[T, ?]] {
      type G[A] = F[A]
      def shiftReadDir[H[_]](GtoH: G ~> H) = λ[ThetaJoin[T, ?] ~> FixFreeH[H, ?]](tj =>
        GtoH(TJ.inj(ThetaJoin(
          Free.point(tj.src),
          applyToBranch(tj.lBranch),
          applyToBranch(tj.rBranch),
          tj.on,
          tj.f,
          tj.combine))))
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[a] <: ACopK[a]](
    implicit EJ: EquiJoin[T, ?] :<<: F
  ): ShiftReadDir.Aux[T, EquiJoin[T, ?], F] =
    new ShiftReadDir[EquiJoin[T, ?]] {
      type G[A] = F[A]
      def shiftReadDir[H[_]](GtoH: G ~> H) = λ[EquiJoin[T, ?] ~> FixFreeH[H, ?]](ej =>
        GtoH(EJ.inj(EquiJoin(
          Free.point(ej.src),
          applyToBranch(ej.lBranch),
          applyToBranch(ej.rBranch),
          ej.key,
          ej.f,
          ej.combine))))
    }

  implicit def copk[T[_[_]], LL <: TListK, M[_]](implicit M: Materializer[T, LL, M]): ShiftReadDir.Aux[T, CopK[LL, ?], M] =
    M.materialize(offset = 0)
  
  sealed trait Materializer[T[_[_]], LL <: TListK, M[_]] {
    def materialize(offset: Int): ShiftReadDir.Aux[T, CopK[LL, ?], M]
  }
  
  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T[_[_]], F[_], M[_]](
      implicit
      F: ShiftReadDir.Aux[T, F, M]
    ): Materializer[T, F ::: TNilK, M] = new Materializer[T, F ::: TNilK, M] {
      override def materialize(offset: Int): ShiftReadDir.Aux[T, CopK[F ::: TNilK, ?], M] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new ShiftReadDir[CopK[F ::: TNilK, ?]] {
          type G[A] = M[A]
          def shiftReadDir[H[_]](GtoH: G ~> H) = new (CopK[F ::: TNilK, ?] ~> FixFreeH[H, ?]) {
            override def apply[A](cfa: CopK[F ::: TNilK, A]): FixFreeH[H, A] = cfa match {
              case I(fa) => F.shiftReadDir(GtoH)(fa)
            }
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T[_[_]], F[_], LL <: TListK, M[_]](
      implicit
      F: ShiftReadDir.Aux[T, F, M],
      LL: Materializer[T, LL, M]
    ): Materializer[T, F ::: LL, M] = new Materializer[T, F ::: LL, M] {
      override def materialize(offset: Int): ShiftReadDir.Aux[T, CopK[F ::: LL, ?], M] = {
        val I = mkInject[F, F ::: LL](offset)
        new ShiftReadDir[CopK[F ::: LL, ?]] {
          type G[A] = M[A]
          def shiftReadDir[H[_]](GtoH: G ~> H) = new (CopK[F ::: LL, ?] ~> FixFreeH[H, ?]) {
            override def apply[A](cfa: CopK[F ::: LL, A]): FixFreeH[H, A] = cfa match {
              case I(fa) => F.shiftReadDir(GtoH)(fa)
              case other => LL.materialize(offset + 1).shiftReadDir(GtoH)(other.asInstanceOf[CopK[LL, A]])
            }
          }
        }
      }
    }
  }

  ////

  private def applyToBranch[T[_[_]]: BirecursiveT](branch: FreeQS[T]): FreeQS[T] =
    branch.futu[FreeQS[T]](_.project.run.fold(
      h => CoEnv(h.left[QScriptTotal[T, Free[CoEnvQS[T, ?], FreeQS[T]]]]),
      shiftTotal.shiftReadDir(coenvPrism[QScriptTotal[T, ?], Hole].reverseGet)(_)))

  private def shiftTotal[T[_[_]]: BirecursiveT]: ShiftReadDir.Aux[T, QScriptTotal[T, ?], QScriptTotal[T, ?]] =
    ShiftReadDir[T, QScriptTotal[T, ?], QScriptTotal[T, ?]]
}

sealed abstract class ShiftReadDirInstances0 {
  implicit def fromShiftRead[T[_[_]], F[_], I[_]](implicit SR: ShiftRead.Aux[T, F, I]): ShiftReadDir.Aux[T, F, I] =
    new ShiftReadDir[F] {
      type G[A] = I[A]
      def shiftReadDir[H[_]](GtoH: G ~> H) = SR.shiftRead(GtoH)
    }
}
