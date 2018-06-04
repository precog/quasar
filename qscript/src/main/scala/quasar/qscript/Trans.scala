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
import quasar.contrib.matryoshka.transHyloM
import quasar.fp._
import quasar.contrib.iota._

import matryoshka._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._

trait Trans[F[_], M[_]] {
  def trans[T, G[_]: Functor]
      (GtoF: PrismNT[G, F])
      (implicit TC: Corecursive.Aux[T, G], TR: Recursive.Aux[T, G])
      : F[T] => M[G[T]]
}

object Trans {

  def apply[T[_[_]]: BirecursiveT, F[_], G[a] <: ACopK[a]: Traverse, M[_]: Monad]
      (trans: Trans[F, M], t: T[G])
      (implicit FG: F :<<: G, FT: Injectable[G, QScriptTotal[T, ?]], B: Branches[T, G])
      : M[T[G]] =
    applyTrans[T, F, G, M](trans, PrismNT.injectCopK)(t)

  def applyTrans[T[_[_]]: BirecursiveT, F[_], G[_]: Traverse, M[_]: Monad]
      (trans: Trans[F, M], GtoF: PrismNT[G, F])
      (t: T[G])
      (implicit G: Injectable[G, QScriptTotal[T, ?]], BR: Branches[T, G])
      : M[T[G]] = {

    val transG: G[T[G]] => M[G[T[G]]] =
      gtg => GtoF.get(gtg).fold(gtg.point[M])(trans.trans[T[G], G](GtoF))

    val prismT: PrismNT[QScriptTotal[T, ?], F] =
      PrismNT(
        λ[QScriptTotal[T, ?] ~> (Option ∘ F)#λ](x => G.project(x).flatMap(GtoF.get.apply)),
        G.inject compose GtoF.reverseGet)

    def tb[A]: G[A] => M[G[A]] = BR.branches.modifyF[M](transBranches[T, F, M](trans, prismT))

    transHyloM[T[G], G, G, T[G], G, M](t)(tb[T[G]], transG)
  }

  ////

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def transBranches[T[_[_]]: BirecursiveT, F[_], M[_]: Monad]
      (trans: Trans[F, M], QTtoF: PrismNT[QScriptTotal[T, ?], F])
      (fqs: FreeQS[T])
      : M[FreeQS[T]] = {

    type G[A] = CoEnv[Hole, QScriptTotal[T, ?], A]

    val p: PrismNT[G, F] = QTtoF.compose(PrismNT.coEnv)

    val transT: G[FreeQS[T]] => M[G[FreeQS[T]]] =
      co => p.get(co).fold(co.point[M])(trans.trans[FreeQS[T], G](p))

    def tb[A]: G[A] => M[G[A]] = Branches.coEnv[T, Hole, QScriptTotal[T, ?]].branches.modifyF(transBranches[T, F, M](trans, QTtoF))

    transHyloM[FreeQS[T], G, G, FreeQS[T], G, M](fqs)(tb[FreeQS[T]], transT)
  }
}
