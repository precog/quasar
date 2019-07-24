/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.datasource.local

import slamdata.Predef._

import cats.effect.IO
import fs2.Stream
import java.nio.file.Paths
import scala.concurrent.ExecutionContext
import quasar.ScalarStages
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.common.data.RValue
import quasar.concurrent.BlockingContext
import quasar.connector._, ParsableType.JsonVariant
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.κ
import quasar.qscript.InterpretedRead

import shims._

abstract class LocalDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val tmr = IO.timer(ExecutionContext.Implicits.global)

  override def datasource
      : Datasource[IO, Stream[IO, ?], InterpretedRead[ResourcePath], QueryResult[IO], ResourcePathType.Physical]

  val nonExistentPath =
    ResourcePath.root() / ResourceName("non") / ResourceName("existent")

  def gatherMultiple[A](g: Stream[IO, A]) = g.compile.toList

  "directory jail" >> {
    val tio = ResourcePath.root() / ResourceName("..") / ResourceName("scala")

    "prevents escaping root directory during discovery" >>* {
      datasource.prefixedChildPaths(tio).map(_ must beNone)
    }

    "prevents escaping root directory during evaluation" >>* {
      MonadResourceErr[IO]
        .attempt(datasource.evaluate(InterpretedRead(tio, ScalarStages.Id)))
        .map(_.toEither must beLeft.like {
          case ResourceError.PathNotFound(p) => p must equal(tio)
        })
    }
  }

  "returns data from a nonempty file" >>* {
    datasource
      .evaluate(InterpretedRead(ResourcePath.root() / ResourceName("smallZips.ldjson"), ScalarStages.Id))
      .flatMap(_.data.foldMap(κ(1)).compile.lastOrError)
      .map(_ must be_>(0))
  }
}

object LocalDatasourceSpec extends LocalDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-datasource-spec")

  def datasource =
    LocalDatasource[IO](
      Paths.get("./impl/src/test/resources"),
      1024,
      ParsableType.json(JsonVariant.LineDelimited, true),
      None,
      blockingPool)
}

object LocalParsedDatasourceSpec extends LocalDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-parsed-datasource-spec")

  def datasource =
    LocalParsedDatasource[IO, RValue](
      Paths.get("./impl/src/test/resources"),
      1024,
      ParsableType.json(JsonVariant.LineDelimited, true),
      None,
      blockingPool)

  "parses array-wrapped JSON" >>* {
    val ds =
      LocalParsedDatasource[IO, RValue](
        Paths.get("./impl/src/test/resources"),
        1024,
        ParsableType.json(JsonVariant.ArrayWrapped, true),
        None,
        blockingPool)

    val iread =
      InterpretedRead(ResourcePath.root() / ResourceName("smallZips.json"), ScalarStages.Id)

    ds.evaluate(iread)
      .flatMap(_.data.foldMap(κ(1)).compile.lastOrError)
      .map(_ must_=== 100)
  }

  "decompresses gzipped resources" >>* {
    val ds =
      LocalParsedDatasource[IO, RValue](
        Paths.get("./impl/src/test/resources"),
        1024,
        ParsableType.json(JsonVariant.ArrayWrapped, true),
        Some(CompressionScheme.Gzip),
        blockingPool)

    val iread =
      InterpretedRead(ResourcePath.root() / ResourceName("smallZips.json.gz"), ScalarStages.Id)

    ds.evaluate(iread)
      .flatMap(_.data.foldMap(κ(1)).compile.lastOrError)
      .map(_ must_=== 100)
  }
}
