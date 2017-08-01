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

package quasar.physical.marklogic.qscript

import quasar.ejson.EJson
import quasar.physical.marklogic.DocType
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.qscript._

import slamdata.Predef._
import simulacrum.typeclass
import matryoshka._
import scalaz._, Scalaz._

@typeclass
trait FormatFilterPlanner[A] {
  def plan[
    F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
    FMT: SearchOptions,
    T[_[_]]: BirecursiveT: ShowT, Q](src: Search[Q], f: FreeMap[T])(
    implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]],
             SP: StructuralPlanner[F, FMT]
  ): F[Option[Search[Q]]]
}

object FormatFilterPlanner {
  implicit val xmlFilterPlanner: FormatFilterPlanner[DocType.Xml] = new FormatFilterPlanner[DocType.Xml] {
    def plan[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
      FMT: SearchOptions,
      T[_[_]]: BirecursiveT: ShowT, Q](src: Search[Q], f: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]],
               SP: StructuralPlanner[F, FMT]
    ): F[Option[Search[Q]]] = {
      val planner = new FilterPlanner[F, FMT, T]

      lazy val pathQuery    = planner.validSearch(planner.PathIndexPlanner(src, f))
      lazy val starQuery    = planner.validSearch(planner.StarIndexPlanner(src, f))
      lazy val elementQuery = planner.validSearch(planner.ElementIndexPlanner(src, f))

      (pathQuery |@| starQuery |@| elementQuery) {
        case (Some(q), _, _)    => q.some.point[F]
        case (_, Some(q), _)    => q.some.point[F]
        case (_, _, Some(q))    => q.some.point[F]
        case (None, None, None) => none[Search[Q]].point[F]
      }.join
    }
  }

  implicit val jsonFilterPlanner: FormatFilterPlanner[DocType.Json] = new FormatFilterPlanner[DocType.Json] {
    def plan[F[_]: Monad: QNameGenerator: PrologW: MonadPlanErr: Xcc,
      FMT: SearchOptions,
      T[_[_]]: BirecursiveT: ShowT, Q](src: Search[Q], f: FreeMap[T])(
      implicit Q: Birecursive.Aux[Q, Query[T[EJson], ?]],
               SP: StructuralPlanner[F, FMT]
    ): F[Option[Search[Q]]] = {
      val planner = new FilterPlanner[F, FMT, T]

      lazy val pathQuery    = planner.validSearch(planner.PathIndexPlanner(src, f))
      lazy val elementQuery = planner.validSearch(planner.ElementIndexPlanner(src, f))

      (pathQuery |@| elementQuery) {
        case (Some(q), _) => q.some.point[F]
        case (_, Some(q)) => q.some.point[F]
        case (None, None) => none[Search[Q]].point[F]
      }.join
    }
  }

}
