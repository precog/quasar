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

import slamdata.Predef._
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait Normalizable[F[_]] {
  def normalizeF: NTComp[F, Option]
}

// it would be nice to use the `NTComp` alias here, but it cannot compile
trait NormalizableInstances {
  import Normalizable._

  def normalizable[T[_[_]]: BirecursiveT: EqualT: ShowT] =
    new NormalizableT[T]

  implicit def const[A]: Normalizable[Const[A, ?]] =
    make(λ[Const[A, ?] ~> (Option ∘ Const[A, ?])#λ](_ => None))

  implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : Normalizable[QScriptCore[T, ?]] =
    normalizable[T].QScriptCore

  implicit def projectBucket[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : Normalizable[ProjectBucket[T, ?]] =
    normalizable[T].ProjectBucket

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : Normalizable[ThetaJoin[T, ?]] =
    normalizable[T].ThetaJoin

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : Normalizable[EquiJoin[T, ?]] =
    normalizable[T].EquiJoin

  implicit def coproduct[F[_], G[_]]
    (implicit F: Normalizable[F], G: Normalizable[G])
      : Normalizable[Coproduct[F, G, ?]] =
    new Normalizable[Coproduct[F, G, ?]] {
      def normalizeF =
        λ[Coproduct[F, G, ?] ~> (Option ∘ Coproduct[F, G, ?])#λ](
          _.run.bitraverse(F.normalizeF(_), G.normalizeF(_)) ∘ (Coproduct(_)))
  }
}

class NormalizableT[T[_[_]]: BirecursiveT : EqualT : ShowT] extends TTypes[T] {
  import Normalizable._
  lazy val rewrite = new Rewrite[T]

  def freeTC(free: FreeQS): FreeQS =
    free.transCata[FreeQS](liftCo(rewrite.normalizeCoEnv[QScriptTotal]))

  def freeTCEq(free: FreeQS): Option[FreeQS] = {
    val freeNormalized = freeTC(free)
    (free ≠ freeNormalized).option(freeNormalized)
  }

  def freeMFEq[A: Equal: Show](fm: Free[MapFunc, A]): Option[Free[MapFunc, A]] = {
    val fmNormalized = freeMF[A](fm)
    (fm ≠ fmNormalized).option(fmNormalized)
  }

  def freeMF[A: Show](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    fm.transCata[Free[MapFunc, A]](MapFuncCore.normalize[T, A])

  def makeNorm[A, B, C](
    lOrig: A, rOrig: B)(
    left: A => Option[A], right: B => Option[B])(
    f: (A, B) => C):
      Option[C] =
    (left(lOrig), right(rOrig)) match {
      case (None, None) => None
      case (l, r)       => f(l.getOrElse(lOrig), r.getOrElse(rOrig)).some
    }

  def EquiJoin = make(
    λ[EquiJoin ~> (Option ∘ EquiJoin)#λ](ej =>
      (freeTCEq(ej.lBranch), freeTCEq(ej.rBranch), freeMFEq(ej.lKey), freeMFEq(ej.rKey), freeMFEq(ej.combine)) match {
        case (None, None, None, None, None) => None
        case (lBranchNorm, rBranchNorm, lKeyNorm, rKeyNorm, combineNorm) =>
          quasar.qscript.EquiJoin(
            ej.src,
            lBranchNorm.getOrElse(ej.lBranch),
            rBranchNorm.getOrElse(ej.rBranch),
            lKeyNorm.getOrElse(ej.lKey),
            rKeyNorm.getOrElse(ej.rKey),
            ej.f,
            combineNorm.getOrElse(ej.combine)).some
      }))

  def ThetaJoin = make(
    λ[ThetaJoin ~> (Option ∘ ThetaJoin)#λ](tj =>
      (freeTCEq(tj.lBranch), freeTCEq(tj.rBranch), freeMFEq(tj.on), freeMFEq(tj.combine)) match {
        case (None, None, None, None) => None
        case (lBranchNorm, rBranchNorm, onNorm, combineNorm) =>
          quasar.qscript.ThetaJoin(
            tj.src,
            lBranchNorm.getOrElse(tj.lBranch),
            rBranchNorm.getOrElse(tj.rBranch),
            onNorm.getOrElse(tj.on),
            tj.f,
            combineNorm.getOrElse(tj.combine)).some
      }))

  def QScriptCore = {
    make(λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
      case Reduce(src, bucket, reducers, repair) => {
        val reducersOpt: List[Option[ReduceFunc[FreeMap]]] =
          reducers.map(_.traverse(freeMFEq[Hole](_)))

        val reducersNormOpt: Option[List[ReduceFunc[FreeMap]]] =
          reducersOpt.exists(_.nonEmpty).option(
            Zip[List].zipWith(reducersOpt, reducers)(_.getOrElse(_)))

        val bucketNormOpt: Option[FreeMap] = freeMFEq(bucket)

        val bucketNormConst: Option[FreeMap] =
          bucketNormOpt.getOrElse(bucket).resume.fold({
            case MFC(MapFuncsCore.Constant(ej)) =>
              (!EJson.isNull(ej)).option(MapFuncsCore.NullLit[T, Hole]())
            case _ => bucketNormOpt
          }, κ(bucketNormOpt))

        (bucketNormConst, reducersNormOpt, freeMFEq(repair)) match {
          case (None, None, None) =>
            None
          case (bucketNorm, reducersNorm, repairNorm)  =>
            Reduce(
              src,
              bucketNorm.getOrElse(bucket),
              reducersNorm.getOrElse(reducers),
              repairNorm.getOrElse(repair)).some
        }
      }

      case Sort(src, bucket, order) =>
        val orderOpt: NonEmptyList[Option[(FreeMap, SortDir)]] =
          order.map { case (fm, dir) => freeMFEq(fm) strengthR dir }

        val orderNormOpt: Option[NonEmptyList[(FreeMap, SortDir)]] =
          orderOpt any (_.nonEmpty) option orderOpt.fzipWith(order)(_ | _)

        makeNorm(bucket, order)(freeMFEq(_), _ => orderNormOpt)(Sort(src, _, _))

      case Map(src, f)             => freeMFEq(f).map(Map(src, _))
      case LeftShift(src, s, i, r) => makeNorm(s, r)(freeMFEq(_), freeMFEq(_))(LeftShift(src, _, i, _))
      case Union(src, l, r)        => makeNorm(l, r)(freeTCEq(_), freeTCEq(_))(Union(src, _, _))
      case Filter(src, f)          => freeMFEq(f).map(Filter(src, _))
      case Subset(src, from, sel, count) => makeNorm(from, count)(freeTCEq(_), freeTCEq(_))(Subset(src, _, sel, _))
      case Unreferenced()          => None
    })
  }

  def ProjectBucket = make(
    λ[ProjectBucket ~> (Option ∘ ProjectBucket)#λ] {
      case BucketField(a, v, f) => makeNorm(v, f)(freeMFEq(_), freeMFEq(_))(BucketField(a, _, _))
      case BucketIndex(a, v, i) => makeNorm(v, i)(freeMFEq(_), freeMFEq(_))(BucketIndex(a, _, _))
    }
  )
}

object Normalizable extends NormalizableInstances {
  def make[F[_]](f: NTComp[F, Option]): Normalizable[F] = new Normalizable[F] { val normalizeF = f }
}
