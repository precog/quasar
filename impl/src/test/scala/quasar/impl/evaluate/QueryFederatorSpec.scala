/*
 * Copyright 2020 Precog Data
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
import quasar.api.DataPathSegment
import quasar.api.datasource.DatasourceType
import quasar.api.push.{OffsetPath, InternalKey}
import quasar.api.resource._
import quasar.connector._
import quasar.connector.evaluate._
import quasar.contrib.pathy.AFile
import quasar.contrib.iota.{copkTraverse, copkEqual}
import quasar.fp.constEqual
import quasar.impl.{EmptyDatasource, QuasarDatasource}
import quasar.qscript._

import scala.collection.immutable.SortedMap

import cats.{Order, Show}
import cats.data.NonEmptyList
import cats.implicits._

import fs2.Stream

import matryoshka._
import matryoshka.data.Fix

import pathy.Path._

import scalaz.Tree

import shims.{eqToScalaz, equalToCats, orderToCats, showToCats, showToScalaz}

import spire.math.Real

import skolems.∃

final class QueryFederatorSpec extends Qspec with TreeMatchers {
  import ResourceError._
  import IdStatus.ExcludeId

  implicit val showTree: Show[Tree[ResourceName]] =
    Show.show(_.drawTree)

  type M[A] = Either[ResourceError, A]
  type Q = InterpretedRead[ResourcePath]

  val fedType = DatasourceType("federator", 37)

  val abs = ResourcePath.root() / ResourceName("resource") / ResourceName("abs")
  val xys = ResourcePath.root() / ResourceName("resource") / ResourceName("xys")

  val absSrc =
    EmptyDatasource[M, Stream[M, ?], Q, Int, ResourcePathType](fedType, 1, supportsSeek = false)

  val xysSrc =
    EmptyDatasource[M, Stream[M, ?], Q, Int, ResourcePathType](fedType, 2, supportsSeek = true)

  val srcs = SortedMap(
    abs -> Source(abs, QuasarDatasource.lightweight[Fix](absSrc)),
    xys -> Source(xys, QuasarDatasource.lightweight[Fix](xysSrc)))(
    Order[ResourcePath].toOrdering)

  val federator = QueryFederator { f =>
    srcs.get(ResourcePath.leaf(f)).asRight[ResourceError]
  }

  val qs = construction.mkDefaults[Fix, QScriptEducated[Fix, ?]]

  def offset(r: Real): Offset =
    Offset.Internal(OffsetPath(DataPathSegment.Field("ts")), ∃(InternalKey.Actual.real(r)))

  "returns 'not a resource' for root" >> {
    val query =
      qs.fix.Map(
        qs.fix.Read[ResourcePath](ResourcePath.Root, ExcludeId),
        qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

    federator((query, None)).swap.toOption must_= Some(notAResource(ResourcePath.root()))
  }

  "returns 'path not found' when no source" >> {
    val query =
      qs.fix.Map(
        qs.fix.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("foo") </> file("bar")), ExcludeId),
        qs.recFunc.MakeMapS("value", qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "value")))

    val rp =
      ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

    federator((query, None)).swap.toOption must_= Some(pathNotFound(rp))
  }

  "returns 'path not found' when no source in branch" >> {
    val query =
      qs.fix.Union(
        qs.fix.Unreferenced,
        qs.free.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("abs") </> file("a")), ExcludeId),
        qs.free.Read[ResourcePath](ResourcePath.leaf(rootDir </> dir("foo") </> file("bar")), ExcludeId))

    val rp =
      ResourcePath.root() / ResourceName("abs") / ResourceName("a")

    federator((query, None)).swap.toOption must_= Some(pathNotFound(rp))
  }

  "returns 'too many sources' when offset query references more than one source" >> {
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

    federator((query, Some(offset(16)))) must beLike {
      case Left(ResourceError.TooManyResources(ps, _)) =>
        ps must_= NonEmptyList.of(absf, xysf).map(ResourcePath.leaf(_))
    }
  }

  "returns 'seek unsupported' when offset query refers to a source that cannot seek" >> {
    val absf: AFile =
      rootDir </> dir("resource") </> file("abs")

    val query =
      qs.fix.Filter(
        qs.fix.Read[ResourcePath](ResourcePath.leaf(absf), ExcludeId),
        qs.recFunc.Gt(
          qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
          qs.recFunc.Now))

    federator((query, Some(offset(100)))) must beLike {
      case Left(ResourceError.SeekUnsupported(p)) => p must_= abs
    }
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

    federator((query, None)) map { fq =>
      fq.query must beTreeEqual(query)
      fq.sources(absf).map(_.path) must_= Some(ResourcePath.leaf(absf))
      fq.sources(xysf).map(_.path) must_= Some(ResourcePath.leaf(xysf))
    } getOrElse ko("Unexpected evaluate failure.")
  }

  "builds offset query when single, seek-supporting, source exists" >> {
    val xysf: AFile =
      rootDir </> dir("resource") </> file("xys")

    val query =
      qs.fix.Filter(
        qs.fix.Union(
          qs.fix.Unreferenced,
          qs.free.Read[ResourcePath](ResourcePath.leaf(xysf), ExcludeId),
          qs.free.Read[ResourcePath](ResourcePath.leaf(xysf), ExcludeId)),
        qs.recFunc.Gt(
          qs.recFunc.ProjectKeyS(qs.recFunc.Hole, "ts"),
          qs.recFunc.Now))

    federator((query, Some(offset(42)))) map { fq =>
      fq.query must beTreeEqual(query)
      fq.sources(xysf).map(_.path) must_= Some(ResourcePath.leaf(xysf))
    } getOrElse ko("Unexpected evaluate failure.")
  }
}
