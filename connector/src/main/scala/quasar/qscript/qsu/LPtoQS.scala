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

package quasar.qscript.qsu

import slamdata.Predef._
import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{Applicative, Functor, Kleisli => K, Monad}
import scalaz.syntax.applicative._

final class LPtoQS[T[_[_]]: BirecursiveT: EqualT: ShowT] extends QSUTTypes[T] {
  import LPtoQS.MapSyntax
  import ApplyProvenance.AuthenticatedQSU

  def apply[F[_]: Monad: PlannerErrorME: NameGenerator](lp: T[LogicalPlan])
      : F[T[QScriptEducated]] = {

    val lpToQs =
      K(ReadLP[T].apply[F])          >=>
      debugG("ReadLP: ")             >-
      EliminateUnary[T].apply        >=>
      debugG("EliminateUnary: ")     >-
      RecognizeDistinct[T].apply     >=>
      debugG("RecognizeDistinct: ")  >==>
      ExtractFreeMap[T, F]           >=>
      debugG("ExtractFM: ")          >==>
      MinimizeAutoJoins[T].apply[F]  >=>
      debugG("MinimizeAJ: ")         >==>
      ApplyProvenance[T].apply[F]    >=>
      debugAG("ApplyProv: ")         >-
      ReifyBuckets[T]                >=>
      debugAG("ReifyBuckets: ")      >==>
      ReifyAutoJoins[T].apply[F]     >=>
      debugAG("ReifyAutoJoins: ")    >-
      (_.graph)                      >==>
      ReifyIdentities[T].apply[F]    >==>
      Graduate[T].apply[F]

    lpToQs(lp)
  }

  // import quasar.fp.{coproductShow, symbolShow}
  // import matryoshka.data._
  // import matryoshka.{delayShow, showTShow}
  // import scalaz.std.map._
  // import scalaz.syntax.show._

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def debugG[F[_]: Applicative](prefix: String): K[F, QSUGraph, QSUGraph] =
    K { g =>
      // println("\n\n" + prefix + g.shows)    // uh... yeah do better
      g.point[F]
    }

  private def debugAG[F[_]: Applicative](prefix: String): K[F, AuthenticatedQSU[T], AuthenticatedQSU[T]] =
    K { aqsu =>
      // println("\n\n" + prefix + aqsu.graph.shows + "\n" + aqsu.dims.shows)
      aqsu.point[F]
    }
}

object LPtoQS {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT]: LPtoQS[T] = new LPtoQS[T]

  final implicit class MapSyntax[F[_], A](val self: F[A]) extends AnyVal {
    def >-[B](f: A => B)(implicit F: Functor[F]): F[B] =
      F.map(self)(f)
  }
}
