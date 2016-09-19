/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.qscript

import matryoshka.AlgebraM
import scalaz._

trait Planner[F[_], QS[_], A] {
  def plan: AlgebraM[F, QS, A]
}

object Planner {
  def apply[F[_], QS[_], A](implicit ev: Planner[F, QS, A]): Planner[F, QS, A] = ev

  implicit def coproduct[M[_], F[_], G[_], A](implicit F: Planner[M, F, A], G: Planner[M, G, A]): Planner[M, Coproduct[F, G, ?], A] =
    new Planner[M, Coproduct[F, G, ?], A] {
      val plan: AlgebraM[M, Coproduct[F, G, ?], A] = _.run.fold(F.plan, G.plan)
    }
}
