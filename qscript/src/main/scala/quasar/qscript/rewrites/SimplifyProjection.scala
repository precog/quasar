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

import slamdata.Predef.{Map => _, _}

import quasar.fp.EndoK
import quasar.contrib.iota.{:<<:, ACopK}
import quasar.contrib.iota.mkInject
import quasar.qscript._
import scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::

/** This optional transformation changes the semantics of [[Read]]. The default
  * semantics return a single value, whereas the transformed version has an
  * implied [[LeftShift]] and therefore returns a set of values, which more
  * closely matches the way many data stores behave.
  */
trait SimplifyProjection[F[_]] {
  type H[A]

  def simplifyProjection: F ~> H
}

object SimplifyProjection {
  type Aux[F[_], G[_]] = SimplifyProjection[F] { type H[A] = G[A] }

  def apply[F[_], G[_]](implicit ev: Aux[F, G]): Aux[F, G] = ev
  def default[F[_]: Traverse, G[a] <: ACopK[a]](implicit F: F :<<: G): Aux[F, G] = make[F, G](F.inj)

  def make[F[_], G[_]](f: F ~> G): Aux[F, G] = new SimplifyProjection[F] {
    type H[A] = G[A]
    val simplifyProjection = f
  }

  // FIXME: if these are only used implicitly they should have names less likely to collide.
  implicit def projectBucket[T[_[_]], G[a] <: ACopK[a]](implicit QC: QScriptCore[T, ?] :<<: G): Aux[ProjectBucket[T, ?], G] =
    SimplifiableProjection[T].ProjectBucket[G]
  implicit def qscriptCore[T[_[_]], G[a] <: ACopK[a]](implicit QC: QScriptCore[T, ?] :<<: G): Aux[QScriptCore[T, ?], G] =
    SimplifiableProjection[T].QScriptCore[G]
  implicit def thetaJoin[T[_[_]], G[a] <: ACopK[a]](implicit QC: ThetaJoin[T, ?] :<<: G): Aux[ThetaJoin[T, ?], G] =
    SimplifiableProjection[T].ThetaJoin[G]
  implicit def equiJoin[T[_[_]], G[a] <: ACopK[a]](implicit QC: EquiJoin[T, ?] :<<: G): Aux[EquiJoin[T, ?], G] =
    SimplifiableProjection[T].EquiJoin[G]
  implicit def deadEnd[F[a] <: ACopK[a]](implicit DE: Const[DeadEnd, ?] :<<: F): Aux[Const[DeadEnd, ?], F] =
    default[Const[DeadEnd, ?], F]
  implicit def read[F[a] <: ACopK[a], A](implicit R: Const[Read[A], ?] :<<: F): Aux[Const[Read[A], ?], F] =
    default[Const[Read[A], ?], F]
  implicit def shiftedRead[F[a] <: ACopK[a], A](implicit SR: Const[ShiftedRead[A], ?] :<<: F): Aux[Const[ShiftedRead[A], ?], F] =
    default[Const[ShiftedRead[A], ?], F]

  implicit def copk[G[_], LL <: TListK](implicit M: Materializer[G, LL]): Aux[CopK[LL, ?], G] =
    M.materialize(offset = 0)

  sealed trait Materializer[G[_], LL <: TListK] {
    def materialize(offset: Int): Aux[CopK[LL, ?], G]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[G[_], F[_]](
      implicit
      F: Aux[F, G]
    ): Materializer[G, F ::: TNilK] = new Materializer[G, F ::: TNilK] {
      override def materialize(offset: Int): Aux[CopK[F ::: TNilK, ?], G] = {
        val I = mkInject[F, F ::: TNilK](offset)
        make(new (CopK[F ::: TNilK, ?] ~> G) {
          override def apply[A](cfa: CopK[F ::: TNilK, A]): G[A] = {
            cfa match {
              case I(fa) => F.simplifyProjection(fa)
            }
          }
        })
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[G[_], F[_], LL <: TListK](
      implicit
      F: Aux[F, G],
      LL: Materializer[G, LL]
    ): Materializer[G, F ::: LL] = new Materializer[G, F ::: LL] {
      override def materialize(offset: Int): Aux[CopK[F ::: LL, ?], G] = {
        val I = mkInject[F, F ::: LL](offset)
        make(new (CopK[F ::: LL, ?] ~> G) {
          override def apply[A](cfa: CopK[F ::: LL, A]): G[A] = {
            cfa match {
              case I(fa) => F.simplifyProjection(fa)
              case other => LL.materialize(offset + 1).simplifyProjection(other.asInstanceOf[CopK[LL, A]])
            }
          }
        })
      }
    }
  }

  // This assembles the coproduct out of the individual implicits.
  def simplifyQScriptTotal[T[_[_]]]: Aux[QScriptTotal[T, ?], QScriptTotal[T, ?]] = apply

}


class SimplifiableProjectionT[T[_[_]]] extends TTypes[T] {
  import SimplifyProjection._

  private lazy val simplify: EndoK[QScriptTotal] = simplifyQScriptTotal[T].simplifyProjection
  private def applyToBranch(branch: FreeQS): FreeQS = branch mapSuspension simplify

  def ProjectBucket[G[a] <: ACopK[a]](implicit QC: QScriptCore :<<: G) = make(
    λ[ProjectBucket ~> G] {
      case BucketKey(src, value, key) =>
        QC.inj(Map(src, RecFreeS.fromFree(Free.roll(MFC(MapFuncsCore.ProjectKey(value, key))))))
      case BucketIndex(src, value, index) =>
        QC.inj(Map(src, RecFreeS.fromFree(Free.roll(MFC(MapFuncsCore.ProjectIndex(value, index))))))
    })

  def QScriptCore[G[a] <: ACopK[a]](implicit QC: QScriptCore :<<: G) = make(
    λ[QScriptCore ~> G](fa =>
      QC inj (fa match {
        case Union(src, lb, rb) =>
          Union(src, applyToBranch(lb), applyToBranch(rb))
        case Subset(src, lb, sel, rb) =>
          Subset(src, applyToBranch(lb), sel, applyToBranch(rb))
        case _ => fa
      })))

  def ThetaJoin[G[a] <: ACopK[a]](implicit TJ: ThetaJoin :<<: G) = make(
    λ[ThetaJoin ~> G](tj =>
      TJ inj quasar.qscript.ThetaJoin(
        tj.src,
        applyToBranch(tj.lBranch),
        applyToBranch(tj.rBranch),
        tj.on,
        tj.f,
        tj.combine)))

  def EquiJoin[G[a] <: ACopK[a]](implicit EJ: EquiJoin :<<: G) = make(
    λ[EquiJoin ~> G](ej =>
      EJ inj quasar.qscript.EquiJoin(
        ej.src,
        applyToBranch(ej.lBranch),
        applyToBranch(ej.rBranch),
        ej.key,
        ej.f,
        ej.combine)))
}

object SimplifiableProjection {
  def apply[T[_[_]]] = new SimplifiableProjectionT[T]
}
