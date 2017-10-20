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
import quasar.fp._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._

trait Trans[F[_]] {
  def trans[T, G[_]: Functor]
      (GtoF: PrismNT[G, F])
      (implicit TC: Corecursive.Aux[T, G], TR: Recursive.Aux[T, G])
      : F[T] => G[T]
}

object Trans {
  def apply[T[_[_]]: BirecursiveT, F[_], G[_]: Functor]
      (trans: Trans[F], t: T[G])
      (implicit FG: F :<: G, FT: Injectable.Aux[G, QScriptTotal[T, ?]], B: Branches[T, G])
      : T[G] =
    applyTrans(trans, PrismNT.inject)(t)

  def applyTrans[T[_[_]]: BirecursiveT, F[_], G[_]: Functor]
      (trans: Trans[F], GtoF: PrismNT[G, F])
      (t: T[G])
      (implicit G: Injectable.Aux[G, QScriptTotal[T, ?]], BR: Branches[T, G])
      : T[G] = {

    val transG: G[T[G]] => G[T[G]] =
      gtg => GtoF.get(gtg).fold(gtg)(trans.trans[T[G], G](GtoF))

    val prismT: PrismNT[QScriptTotal[T, ?], F] =
      PrismNT(
        λ[QScriptTotal[T, ?] ~> (Option ∘ F)#λ](x => G.project(x).flatMap(GtoF.get.apply)),
        G.inject compose GtoF.reverseGet)

    def tb[A]: G[A] => G[A] = BR.branches.modify(transBranches[T, F](trans, prismT))
    t.transHylo(tb[T[G]], transG)
  }

  ////

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def transBranches[T[_[_]]: BirecursiveT, F[_]]
      (trans: Trans[F], QTtoF: PrismNT[QScriptTotal[T, ?], F])
      (fqs: FreeQS[T])
      : FreeQS[T] = {

    type G[A] = CoEnv[Hole, QScriptTotal[T, ?], A]

    val p: PrismNT[G, F] = QTtoF.compose(PrismNT.coEnv)

    val transT: G[FreeQS[T]] => G[FreeQS[T]] =
      co => p.get(co).fold(co)(trans.trans[FreeQS[T], G](p))

    def tb[A]: G[A] => G[A] = Branches.coEnv[T, Hole, QScriptTotal[T, ?]].branches.modify(transBranches[T, F](trans, QTtoF))
    fqs.transHylo(tb[FreeQS[T]], transT)
  }
}
