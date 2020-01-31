/*
 * Copyright 2014–2020 SlamData Inc.
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
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{~>, Functor}
import scalaz.syntax.equal._
import scalaz.syntax.monad._

private class NormalizeQScript[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
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
        // no-op Map
        case Map(src, mf) if mf === HoleR => src.project

        case Map(outerSrc, outerMF) =>
          prismGF.unapply(outerSrc.project) match {
            case Some(fa) => FToNorm(fa) match {
              // Map . Map
              case Some(QSC(Map(innerSrc, innerMF))) =>
                NormToG(QSC.inj(Map(innerSrc, outerMF >> innerMF)))

              // Map . LeftShift
              case Some(QSC(LeftShift(innerSrc, struct, status, tpe, undef, repair))) =>
                NormToG(QSC.inj(LeftShift(
                  innerSrc, struct, status, tpe, undef, outerMF.linearize >> repair)))

              case _ => prismGF(ftg)
            }

            case None => prismGF(ftg)
          }

        // branch recursion
        case Union(src, lBranch, rBranch) =>
          NormToG(QSC.inj(Union(src, branchNorm(lBranch), branchNorm(rBranch))))

        // branch recursion
        case Subset(src, from, op, count) =>
          NormToG(QSC.inj(Subset(src, branchNorm(from), op, branchNorm(count))))

        case qs => NormToG(QSC.inj(qs))
      }

      // branch recursion
      case Some(EJ(EquiJoin(src, lBranch, rBranch, key, f, combine))) =>
        NormToG(EJ.inj(EquiJoin(src, branchNorm(lBranch), branchNorm(rBranch), key, f, combine)))

      case _ => prismGF(ftg)
    }
}

object NormalizeQScript {

  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qs: T[QScriptNormalized[T, ?]])
      : T[QScriptNormalized[T, ?]] = {
    val N = new NormalizeQScript[T]
    qs.transCata[T[QScriptNormalized[T, ?]]](N.normQS)
  }
}
