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

package quasar.qsu

import slamdata.Predef._
import quasar.RenderTreeT
import quasar.fs.Planner.PlannerErrorME
import quasar.effect.NameGenerator
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.{delayShow, showTShow, BirecursiveT, EqualT, ShowT}
import scalaz.{Applicative, Cord, Functor, Kleisli => K, Monad, Show}
import scalaz.syntax.applicative._
import scalaz.syntax.show._

final class LPtoQS[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends QSUTTypes[T] {
  import LPtoQS.MapSyntax

  def apply[F[_]: Monad: PlannerErrorME: NameGenerator](lp: T[LogicalPlan])
      : F[T[QScriptEducated]] = {

    val agraph =
      ApplyProvenance.AuthenticatedQSU.graph[T]

    val lpToQs =
      K(ReadLP[T, F])                        >==>
      debug("ReadLP: ")                      >==>
      RewriteGroupByArrays[T, F]             >==>
      debug("RewriteGroupByArrays: ")        >-
      EliminateUnary[T]                      >==>
      debug("EliminateUnary: ")              >-
      RecognizeDistinct[T]                   >==>
      debug("RecognizeDistinct: ")           >==>
      ExtractFreeMap[T, F]                   >==>
      debug("ExtractFreeMap: ")              >==>
      ApplyProvenance[T, F]                  >==>
      debug("ApplyProv: ")                   >==>
      ReifyBuckets[T, F]                     >==>
      debug("ReifyBuckets: ")                >==>
      MinimizeAutoJoins[T, F]                >==>
      debug("MinimizeAutoJoins: ")           >==>
      ReifyAutoJoins[T, F]                   >==>
      debug("ReifyAutoJoins: ")              >==>
      ExpandShifts[T, F]                     >==>
      debug("ExpandShifts: ")                >-
      agraph.modify(ResolveOwnIdentities[T]) >==>
      debug("ResolveOwnIdentities: ")        >==>
      ReifyIdentities[T, F]                  >==>
      debug("ReifyIdentities: ")             >==>
      Graduate[T, F]

    lpToQs(lp)
  }

  private def debug[F[_]: Applicative, A: Show](prefix: String): A => F[A] =
    // uh... yeah do better
    a => a.point[F].map { i =>
      maybePrint((Cord("\n\n") ++ Cord(prefix) ++ a.show).shows)
      i
    }

  private def maybePrint(str: => String): Unit = {
    // println(str)

    ()
  }
}

object LPtoQS {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: LPtoQS[T] = new LPtoQS[T]

  final implicit class MapSyntax[F[_], A](val self: F[A]) extends AnyVal {
    def >-[B](f: A => B)(implicit F: Functor[F]): F[B] =
      F.map(self)(f)
  }
}
