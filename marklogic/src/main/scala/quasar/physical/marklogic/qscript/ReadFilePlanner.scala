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

import quasar.contrib.pathy.{AFile, UriPathCodec}
import quasar.physical.marklogic.cts._
import quasar.physical.marklogic.xquery._
import quasar.qscript._

import eu.timepit.refined.auto._
import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class ReadFilePlanner[M[_]: Applicative: MonadPlanErr, FMT]
    extends Planner[M, FMT, Const[Read[AFile], ?]] {

  import MarkLogicPlannerError._

  def plan[Q, V](implicit Q: Birecursive.Aux[Q, Query[V, ?]]): AlgebraM[M, Const[Read[AFile], ?], Search[Q] \/ XQuery] = {
    case Const(Read(file)) =>
      val fileUri = UriPathCodec.printPath(file)

      Uri.getOption(fileUri).cata(uri =>
        Search(
          Q.embed(Query.Document[V, Q](IList(uri))),
          ExcludeId
        ).left[XQuery].point[M],
        MonadPlanErr[M].raiseError(invalidUri(fileUri)))
  }
}
