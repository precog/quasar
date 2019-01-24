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
import quasar.RenderTreeT
import quasar.contrib.iota._
import quasar.fp.PrismNT
import quasar.qscript._

import iotaz.CopK
import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import scalaz.{~>, Functor}
import scalaz.syntax.functor._

class NormalizeQScriptFreeMap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    extends Normalize[T] {

  val QSC = CopK.Inject[QScriptCore[T, ?], QSNorm]
  val EJ = CopK.Inject[EquiJoin[T, ?], QSNorm]

  def norm[F[_], G[_]: Functor](
      FToNorm: F ~> OptNorm,
      NormToG: QSNorm ~> G,
      prismGF: PrismNT[G, F])
      : F[T[G]] => G[T[G]] =
    ftg => FToNorm(ftg) match {
      case Some(QSC(value)) => value match {
        case Map(src, fm) =>
          NormToG(QSC.inj(Map(src, recNorm(fm))))

        case LeftShift(src, struct, status, tpe, undef, repair) =>
          NormToG(QSC.inj(LeftShift(
            src,
            recNorm(struct),
            status,
            tpe,
            undef,
            MapFuncCore.normalized(repair))))

        case Reduce(src, bucket, reducers, repair) =>
          NormToG(QSC.inj(Reduce(
            src,
            bucket.map(MapFuncCore.normalized(_)),
            reducers.map(_.map(MapFuncCore.normalized(_))),
            MapFuncCore.normalized(repair))))

        case Sort(src, bucket, order) =>
          NormToG(QSC.inj(Sort(
            src,
            bucket.map(MapFuncCore.normalized(_)),
            order map { case (fm, dir) => (MapFuncCore.normalized(fm), dir) })))

        case Union(src, lBranch, rBranch) =>
          NormToG(QSC.inj(Union(
            src,
            branchNorm(lBranch),
            branchNorm(rBranch))))

        case Filter(src, fm) =>
          NormToG(QSC.inj(Filter(src, recNorm(fm))))

        case Subset(src, from, op, count) =>
          NormToG(QSC.inj(Subset(
            src,
            branchNorm(from),
            op,
            branchNorm(count))))

        case Unreferenced() =>
          prismGF(ftg)
      }

      case Some(EJ(EquiJoin(src, lBranch, rBranch, key, tpe, combine))) =>
        NormToG(EJ.inj(EquiJoin(
          src,
          branchNorm(lBranch),
          branchNorm(rBranch),
          key map { case (l, r) => (MapFuncCore.normalized(l), MapFuncCore.normalized(r)) },
          tpe,
          MapFuncCore.normalized(combine))))

      case _ => prismGF(ftg)
    }

  private def recNorm(fm: RecFreeMap[T]): RecFreeMap[T] =
    RecFreeS.fromFree(MapFuncCore.normalized(fm.linearize))
}

object NormalizeQScriptFreeMap {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qs: T[QScriptNormalized[T, ?]])
      : T[QScriptNormalized[T, ?]] = {
    val N = new NormalizeQScriptFreeMap[T]
    qs.transCata[T[QScriptNormalized[T, ?]]](N.normQS)
  }
}
