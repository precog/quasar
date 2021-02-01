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

package quasar.impl.local

import slamdata.Predef._

import cats.effect.{Blocker, IO, Resource}
import fs2.Stream

import java.nio.file.Paths
import scala.concurrent.ExecutionContext

import quasar.ScalarStages
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.common.data.RValue
import quasar.concurrent._
import quasar.connector._
import quasar.connector.datasource._
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.κ
import quasar.qscript.InterpretedRead

import shims.{monadToScalaz, monoidToCats}

import tectonic.Plate

abstract class LocalDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val tmr = IO.timer(ExecutionContext.Implicits.global)

  def local: Resource[IO, Datasource[Resource[IO, ?], Stream[IO, ?], InterpretedRead[ResourcePath], QueryResult[IO], ResourcePathType.Physical]]

  def datasource = local

  val nonExistentPath =
    ResourcePath.root() / ResourceName("non") / ResourceName("existent")

  def gatherMultiple[A](g: Stream[IO, A]) = g.compile.toList

  def compileData(qr: QueryResult[IO]): IO[Int] =
    qr match {
      case QueryResult.Parsed(_, data, _) => data.data.foldMap(κ(1)).compile.lastOrError
      case QueryResult.Typed(_, data, _) => data.data.foldMap(κ(1)).compile.lastOrError
      case QueryResult.Stateful(_, plate, state, data, _) =>
        val bytes = data(None) ++ recurseStateful(plate, state, data)
        bytes.foldMap(κ(1)).compile.lastOrError
    }

  private def recurseStateful[P <: Plate[Unit], S](
      plateF: IO[P],
      state: P => IO[Option[S]],
      data: Option[S] => Stream[IO, Byte])
      : Stream[IO, Byte] =
    Stream.eval(plateF.flatMap(state)) flatMap {
      case s @ Some(_) =>
        data(s) ++ recurseStateful(plateF, state, data)
      case None =>
        Stream.empty
    }

  "directory jail" >> {
    val tio = ResourcePath.root() / ResourceName("..") / ResourceName("scala")

    "prevents escaping root directory during discovery" >>* {
      local.flatMap(_.prefixedChildPaths(tio))
        .use(o => IO.pure(o must beNone))
    }

    "prevents escaping root directory during evaluation" >>* {
      val load =
        local
          .flatMap(_.loadFull(InterpretedRead(tio, ScalarStages.Id)).value)
          .use(IO.pure(_))

      MonadResourceErr[IO].attempt(load).map(_.toEither must beLeft.like {
        case ResourceError.PathNotFound(p) => p must equal(tio)
      })
    }
  }

  "returns data from a nonempty file" >>* {
    local
      .flatMap(
        _.loadFull(InterpretedRead(ResourcePath.root() / ResourceName("smallZips.ldjson"), ScalarStages.Id))
          .semiflatMap(qr => Resource.liftF(compileData(qr)))
          .value)
      .use(r => IO.pure(r must beSome(be_>(0))))
  }
}

object LocalDatasourceSpec extends LocalDatasourceSpec {
  val local =
    Blocker.cached[IO]("local-datasource-spec")
      .map(LocalDatasource[IO](
        Paths.get("./impl/src/test/resources"),
        1024,
        DataFormat.precise(DataFormat.ldjson),
        _))
}

object LocalStatefulDatasourceSpec extends LocalDatasourceSpec {
  val local =
    Blocker.cached[IO]("local-stateful-datasource-spec")
      .map(LocalStatefulDatasource[IO](
        Paths.get("./impl/src/test/resources"),
        1024,
        DataFormat.precise(DataFormat.ldjson),
        10,
        _))
}

object LocalParsedDatasourceSpec extends LocalDatasourceSpec {

  def make(format: DataFormat) =
    Blocker.cached[IO]("local-parsed-datasource-spec")
      .map(LocalParsedDatasource[IO, RValue](
        Paths.get("./impl/src/test/resources"),
        1024,
        format,
        _))

  val local = make(DataFormat.precise(DataFormat.ldjson))

  "parses array-wrapped JSON" >>* {
    val iread =
      InterpretedRead(ResourcePath.root() / ResourceName("smallZips.json"), ScalarStages.Id)

    make(DataFormat.precise(DataFormat.json))
      .flatMap(
        _.loadFull(iread)
          .semiflatMap(qr => Resource.liftF(compileData(qr)))
          .value)
      .use(r => IO.pure(r must_=== Some(100)))
  }

  "parses csv" >>* {
    val iread =
      InterpretedRead(ResourcePath.root() / ResourceName("smallZips.csv"), ScalarStages.Id)

    make(DataFormat.SeparatedValues.Default)
      .flatMap(
        _.loadFull(iread)
          .semiflatMap(qr => Resource.liftF(compileData(qr)))
          .value)
      .use(r => IO.pure(r must_=== Some(100)))
  }

  "decompression scenarios" >> {
    "decompresses gzipped resources" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("smallZips.json.gz"), ScalarStages.Id)

      make(DataFormat.gzipped(DataFormat.precise(DataFormat.json)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(100)))
    }

    "decompresses zipped single ldjson array file" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("arrayLdjsonSmallZips.json.zip"), ScalarStages.Id)

      make(DataFormat.zipped(DataFormat.precise(DataFormat.json)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(100)))
    }

    "decompresses zipped multiple array ldjson files in directory" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("multiArrayLdjsonSmallZipsInDir.zip"), ScalarStages.Id)

      make(DataFormat.zipped(DataFormat.precise(DataFormat.ldjson)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(3)))
    }

    "decompresses zipped multiple ldjson array files" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("multiArrayLdjsonSmallZips.zip"), ScalarStages.Id)

      make(DataFormat.zipped(DataFormat.precise(DataFormat.ldjson)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(3)))
    }

    "decompresses zipped multiple ldjson files" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("multiLdjsonSmallZips.zip"), ScalarStages.Id)

      make(DataFormat.zipped(DataFormat.precise(DataFormat.ldjson)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(30)))
    }

    "decompresses zipped multiple ldjson files in directory" >>* {
      val iread =
        InterpretedRead(ResourcePath.root() / ResourceName("multiLdjsonSmallZipsInDir.zip"), ScalarStages.Id)

      make(DataFormat.zipped(DataFormat.precise(DataFormat.ldjson)))
        .flatMap(
          _.loadFull(iread)
            .semiflatMap(qr => Resource.liftF(compileData(qr)))
            .value)
        .use(r => IO.pure(r must_=== Some(30)))
    }
  }
}
