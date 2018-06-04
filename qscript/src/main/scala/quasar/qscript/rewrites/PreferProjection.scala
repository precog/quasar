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

import quasar.ejson
import quasar.ejson.{EJson, ExtEJson}
import quasar.contrib.iota.{copkTraverse, mkInject}
import quasar.fp.{:<<:, ACopK, PrismNT}
import quasar.fp.ski.κ
import quasar.qscript._
import quasar.qscript.analysis.Outline
import quasar.qscript.RecFreeS._

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import iotaz.{TListK, CopK, TNilK}
import iotaz.TListK.:::


/** A rewrite that, where possible, replaces map key deletion with construction
  * of a new map containing the keys in the complement of the singleton set consisting
  * of the deleted key.
  *
  * Only applies to maps where all keys are known statically.
  */
trait PreferProjection[F[_], T, B[_]] {
  def preferProjectionƒ(BtoF: PrismNT[B, F]): F[(Outline.Shape, T)] => B[T]
}

object PreferProjection extends PreferProjectionInstances {

  /** Rewrites map key deletion into construction of a new map containing the
    * keys in the complement of the singleton set consisting of the deleted key.
    */
  object preferProjection {
    def apply[F[_]] = new PartiallyApplied[F]
    final class PartiallyApplied[F[_]] {
      def apply[T](t: T)(
          implicit
          TC: Corecursive.Aux[T, F],
          TR: Recursive.Aux[T, F],
          F: Functor[F],
          P: PreferProjection[F, T, F],
          O: Outline[F])
          : T =
        t.zygo(O.outlineƒ, P.preferProjectionƒ(PrismNT.id[F]) >>> (_.embed))
      }
  }

  def preferProjectionF[F[_]: Functor, A]
      (fa: Free[F, A])
      (f: A => Outline.Shape)
      (implicit P: PreferProjection[F, Free[F, A], CoEnv[A, F, ?]], O: Outline[F])
      : Free[F, A] = {

    val ƒ: GAlgebra[(Outline.Shape, ?), CoEnv[A, F, ?], Free[F, A]] = {
      case CoEnv(\/-(fu)) =>
        P.preferProjectionƒ(PrismNT.coEnv[F, A])(fu).embed

      case coEnv => coEnv.map(_._2).embed
    }

    fa.zygo(interpret(f, O.outlineƒ), ƒ)
  }

  def projectComplement[T[_[_]]: BirecursiveT: EqualT, A: Equal]
      (fm: FreeMapA[T, A])
      (f: A => Outline.Shape)
      : FreeMapA[T, A] =
    fm.elgotZygo(
        interpret(f, Outline[MapFunc[T, ?]].outlineƒ),
        projectComplementƒ[T, MapFunc[T, ?], A])
      .transCata[FreeMapA[T, A]](MapFuncCore.normalize)

  /** Replaces key deletion of a map having statically known structure with a
    * projection of the complement of the deleted key.
    */
  def projectComplementƒ[T[_[_]]: BirecursiveT, F[a] <: ACopK[a] : Functor, A]
      (implicit I: MapFuncCore[T, ?] :<<: F)
      : ElgotAlgebra[(Outline.Shape, ?), CoEnv[A, F, ?], Free[F, A]] = {

    import MapFuncsCore._

    type U = Free[F, A]
    val P = PrismNT.injectCopK[MapFuncCore[T, ?], F] compose PrismNT.coEnv[F, A]

    {
      case (shape, mfc @ P(DeleteKey(src, _))) =>
        val projections = shape.resume.swap.toOption collect {
          case ExtEJson(ejson.Map(kvs)) =>
            kvs.traverse { case (k, _) => asEjs[T](k) } map { keys =>
              val maps = keys.map { k =>
                val c = P(Constant[T, U](k)).embed
                P(MakeMap[T, U](c, P(ProjectKey[T, U](src, c)).embed)).embed
              }

              maps.foldLeft1Opt((a, b) => P(ConcatMaps(a, b)).embed) | P(MapFuncCore.EmptyMap[T, U]).embed
            }
        }

        projections.join getOrElse mfc.embed

      case (_, coEnv) => coEnv.embed
    }
  }

  ////

  private object asEjs {
    def apply[T[_[_]]] = new PartiallyApplied[T]
    final class PartiallyApplied[T[_[_]]] {
      def apply[A](fa: Free[EJson, A])(implicit T: CorecursiveT[T]): Option[T[EJson]] =
        fa.transCataM[Option, T[EJson], EJson](_.run.toOption)
    }
  }
}

sealed abstract class PreferProjectionInstances {
  import PreferProjection.projectComplement

  implicit def copk[T, B[_], LL <: TListK](implicit M: Materializer[T, B, LL]): PreferProjection[CopK[LL, ?], T, B] =
    M.materialize(offset = 0)

  sealed trait Materializer[T, B[_], LL <: TListK] {
    def materialize(offset: Int): PreferProjection[CopK[LL, ?], T, B]
  }

  object Materializer {
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def base[T, B[_], F[_]](
      implicit
      F: PreferProjection[F, T, B]
    ): Materializer[T, B, F ::: TNilK] = new Materializer[T, B, F ::: TNilK] {
      override def materialize(offset: Int): PreferProjection[CopK[F ::: TNilK, ?], T, B] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new PreferProjection[CopK[F ::: TNilK, ?], T, B] {
          type C[A] = CopK[F ::: TNilK, A]
          type D[A] = CopK[TNilK, A]

          def preferProjectionƒ(BtoC: PrismNT[B, C]): C[(Outline.Shape, T)] => B[T] = {
            case I(fa) => F.preferProjectionƒ(BtoC andThen PrismNT.injectCopK[F, C](I))(fa)
          }

        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[T, B[_], F[_], LL <: TListK](
      implicit
      F: PreferProjection[F, T, B],
      LL: Materializer[T, B, LL]
    ): Materializer[T, B, F ::: LL] = new Materializer[T, B, F ::: LL] {
      override def materialize(offset: Int): PreferProjection[CopK[F ::: LL, ?], T, B] = {
        val I = mkInject[F, F ::: LL](offset)
        new PreferProjection[CopK[F ::: LL, ?], T, B] {
          type C[A] = CopK[F ::: LL, A]
          type D[A] = CopK[LL, A]

          def preferProjectionƒ(BtoC: PrismNT[B, C]): C[(Outline.Shape, T)] => B[T] = {
            case I(fa) => F.preferProjectionƒ(BtoC andThen PrismNT.injectCopK[F, C](I))(fa)
            case other => LL.materialize(offset + 1).preferProjectionƒ(BtoC.asInstanceOf[PrismNT[B, D]])(other.asInstanceOf[D[(Outline.Shape, T)]])
          }

        }
      }
    }
  }

  implicit def qScriptCore[T[_[_]]: BirecursiveT: EqualT, U, B[_]: Functor](
      implicit
      UC: Corecursive.Aux[U, B],
      UR: Recursive.Aux[U, B],
      O: Outline[QScriptCore[T, ?]])
      : PreferProjection[QScriptCore[T, ?], U, B] =
    new PreferProjection[QScriptCore[T, ?], U, B] {
      def preferProjectionƒ(BtoF: PrismNT[B, QScriptCore[T, ?]]) = {
        case Map((srcShape, u), f) =>
          BtoF(Map(u, RecFreeS.fromFree(prjFreeMap(srcShape, f.linearize))))

        case LeftShift((srcShape, u), struct, idStatus, shiftType, undef, repair) =>
          // NB: This is necessary as srcShape gives us the input shape to the
          //     LeftShift, which is what we need to rewrite `struct`, however
          //     to rewrite `repair` we need any modifications that `LeftShift`
          //     makes to the shape, excluding `repair`'s own modifications.
          val prjRepair = projectComplement(repair) { joinSide =>
            O.outlineƒ(LeftShift(srcShape, struct, idStatus, shiftType, undef, Free.point(joinSide)))
          }

          BtoF(LeftShift(u, RecFreeS.fromFree(prjFreeMap(srcShape, struct.linearize)), idStatus, shiftType, undef, prjRepair))

        case Reduce((srcShape, u), buckets, reducers, repair) =>
          val prjBuckets = buckets map (prjFreeMap(srcShape, _))
          val prjReducers = Functor[List].compose[ReduceFunc].map(reducers)(prjFreeMap(srcShape, _))
          val prjRepair = projectComplement(repair) { ridx =>
            O.outlineƒ(Reduce(srcShape, buckets, reducers, Free.point(ridx)))
          }

          BtoF(Reduce(u, prjBuckets, prjReducers, prjRepair))

        case Union((_, u), l, r) => BtoF(Union(u, l, r))

        case Sort((srcShape, u), buckets, order) =>
          val prjBuckets = buckets map (prjFreeMap(srcShape, _))
          val prjOrder = order map (_ leftMap (prjFreeMap(srcShape, _)))
          BtoF(Sort(u, prjBuckets, prjOrder))

        case Filter((srcShape, u), pred) =>
          BtoF(Filter(u, prjFreeMap(srcShape, pred.linearize).asRec))

        case Subset((srcShape, u), from, op, count) =>
          BtoF(Subset(u, prjFreeQS(srcShape, from), op, prjFreeQS(srcShape, count)))

        case Unreferenced() => BtoF(Unreferenced())
      }
    }

  implicit def thetaJoin[T[_[_]]: BirecursiveT: EqualT, U, B[_]: Functor](
      implicit
      UC: Corecursive.Aux[U, B],
      UR: Recursive.Aux[U, B],
      O: Outline[ThetaJoin[T, ?]])
      : PreferProjection[ThetaJoin[T, ?], U, B] =
    new PreferProjection[ThetaJoin[T, ?], U, B] {
      def preferProjectionƒ(BtoF: PrismNT[B, ThetaJoin[T, ?]]) = {
        case ThetaJoin((srcShape, u), l, r, on, jtype, combine) =>
          val prjL = prjFreeQS(srcShape, l)
          val prjR = prjFreeQS(srcShape, r)
          val prjOn = projectComplement(on) { jside =>
            O.outlineƒ(ThetaJoin(srcShape, l, r, on, jtype, Free.point(jside)))
          }
          val prjCombine = projectComplement(combine) { jside =>
            O.outlineƒ(ThetaJoin(srcShape, l, r, on, jtype, Free.point(jside)))
          }

          BtoF(ThetaJoin(u, prjL, prjR, prjOn, jtype, prjCombine))
      }
    }

  implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT, U, B[_]: Functor](
      implicit
      UC: Corecursive.Aux[U, B],
      UR: Recursive.Aux[U, B],
      O: Outline[EquiJoin[T, ?]])
      : PreferProjection[EquiJoin[T, ?], U, B] =
    new PreferProjection[EquiJoin[T, ?], U, B] {
      def preferProjectionƒ(BtoF: PrismNT[B, EquiJoin[T, ?]]) = {
        case EquiJoin((srcShape, u), l, r, key, jtype, combine) =>
          val prjL = prjFreeQS(srcShape, l)
          val prjR = prjFreeQS(srcShape, r)
          val prjKey = key map (_ umap (prjFreeMap(srcShape, _)))
          val prjCombine = projectComplement(combine) { jside =>
            O.outlineƒ(EquiJoin(srcShape, l, r, key, jtype, Free.point(jside)))
          }

          BtoF(EquiJoin(u, prjL, prjR, prjKey, jtype, prjCombine))
      }
    }

  // Identity Instances

  implicit def projectBucket[T[_[_]], U, B[_]: Functor]: PreferProjection[ProjectBucket[T, ?], U, B] =
    identityInstance

  implicit def constRead[U, B[_]: Functor, A]: PreferProjection[Const[Read[A], ?], U, B] =
    identityInstance

  implicit def constShiftedRead[U, B[_]: Functor, A]: PreferProjection[Const[ShiftedRead[A], ?], U, B] =
    identityInstance

  implicit def constDeadEnd[U, B[_]: Functor]: PreferProjection[Const[DeadEnd, ?], U, B] =
    identityInstance

  ////

  private def prjFreeQS[T[_[_]]: BirecursiveT: EqualT](
      srcShape: Outline.Shape,
      fqs: FreeQS[T])
      : FreeQS[T] =
    PreferProjection.preferProjectionF(fqs)(κ(srcShape))

  private def prjFreeMap[T[_[_]]: BirecursiveT: EqualT](
      srcShape: Outline.Shape,
      fm: FreeMap[T])
      : FreeMap[T] =
    projectComplement(fm)(κ(srcShape))

  private def identityInstance[F[_], U, B[_]: Functor]: PreferProjection[F, U, B] =
    new PreferProjection[F, U, B] {
      def preferProjectionƒ(BtoF: PrismNT[B, F]) =
        f => BtoF(f) map (_._2)
    }
}
