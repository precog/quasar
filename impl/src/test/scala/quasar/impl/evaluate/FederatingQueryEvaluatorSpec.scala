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

package quasar.impl.evaluate

import slamdata.Predef._
import quasar.{IdStatus, Qspec, TreeMatchers}
import quasar.api.resource._
import quasar.connector.ResourceError
import quasar.contrib.pathy.AFile
import quasar.contrib.iota.{copkTraverse, copkEqual}
import quasar.fp.constEqual
import quasar.qscript._

import matryoshka._
import matryoshka.data.Fix
import pathy.Path._
import scalaz.{\/, IMap, Show, Tree}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.either._

final class FederatingQueryEvaluatorSpec extends Qspec with TreeMatchers {
  import ResourceError._
  import IdStatus.ExcludeId

  implicit val showTree: Show[Tree[ResourceName]] =
    Show.shows(_.drawTree)

  val abs = ResourcePath.root() / ResourceName("resource") / ResourceName("abs")
  val xys = ResourcePath.root() / ResourceName("resource") / ResourceName("xys")

  val srcs = IMap(
    abs -> Source(abs, 1),
    xys -> Source(xys, 2))

  val qfed = new QueryFederation[Fix, ResourceError \/ ?, Int, FederatedQuery[Fix, Int]] {
    def evaluateFederated(q: FederatedQuery[Fix, Int]) = q.right[ResourceError]
  }

  val fqe =
    FederatingQueryEvaluator[Fix, ResourceError \/ ?, Int, FederatedQuery[Fix, Int]](
      qfed, f => srcs.lookup(ResourcePath.leaf(f)).right[ResourceError])

  "evaluate" >> {
    val qs = construction.mkDefaults[Fix, QScriptEducated[Fix, ?]]

    "returns NAR for root" >> {
      val query =
        qs.fix.Map(
          qs.fix.Read[ResourcePath](ResourcePath.Root, ExcludeId),
          qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

      fqe.evaluate(query).swap.toOption must_= Some(notAResource(ResourcePath.root()))
    }

    "returns PNF when no source" >> {
      val query =
        qs.fix.Map(
          qs.fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("foo") </> file("bar")), ExcludeId),
          qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

      val rp =
        ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.evaluate(query).swap.toOption must_= Some(pathNotFound(rp))
    }

    "returns PNF when no source in branch" >> {
      val query =
        qs.fix.Union(
          qs.fix.Unreferenced,
          qs.free.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("abs") </> file("a")), ExcludeId),
          qs.free.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("foo") </> file("bar")), ExcludeId))

      val rp =
        ResourcePath.root() / ResourceName("abs") / ResourceName("a")

      fqe.evaluate(query).swap.toOption must_= Some(pathNotFound(rp))
    }

    "builds federated query when all sources found" >> {
      val absf: AFile =
        rootDir </> dir("resource") </> file("abs")

      val xysf: AFile =
        rootDir </> dir("resource") </> file("xys")

      val query =
        qs.fix.Filter(
          qs.fix.Union(
            qs.fix.Unreferenced,
            qs.free.Read[ResourcePath](ResourcePath.leaf(absf), ExcludeId),
            qs.free.Read[ResourcePath](ResourcePath.leaf(xysf), ExcludeId)),
          qs.recFunc.Gt(
            qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
            qs.recFunc.Now))

      fqe.evaluate(query) map { fq =>
        fq.query must beTreeEqual(query)
        fq.sources(absf) must_= Some(Source(ResourcePath.leaf(absf), 1))
        fq.sources(xysf) must_= Some(Source(ResourcePath.leaf(xysf), 2))
      } getOrElse ko("Unexpected evaluate failure.")
    }
  }
}
