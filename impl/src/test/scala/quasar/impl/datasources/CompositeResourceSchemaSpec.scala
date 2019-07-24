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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.{IdStatus, ScalarStages}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.common.data.RValue
import quasar.connector.{CompressionScheme, ParsableType, QueryResult, ResourceError}
import quasar.connector.ParsableType.JsonVariant
import quasar.contrib.iota._
import quasar.contrib.matryoshka.envT
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.impl.datasource.CompositeResult
import quasar.impl.schema._
import quasar.sst._
import quasar.sst.StructuralType.TypeST
import quasar.tpe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Left

import java.lang.IllegalArgumentException
import java.nio.charset.Charset

import cats.effect.{IO, Timer}

import eu.timepit.refined.auto._

import fs2.{gzip, Pure, Stream}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._

import qdata.QDataDecode

import scalaz.IMap
import scalaz.std.anyVal._
import scalaz.std.option._

import shims.{eqToScalaz => _, orderToScalaz => _, _}

import spire.std.double._

object CompositeResourceSchemaSpec extends quasar.EffectfulQSpec[IO] {

  import CompositeResourceSchema.{SourceKey, ValueKey}

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val ioTimer: Timer[IO] =
    IO.timer(global)

  val defaultCfg = SstConfig.Default[Fix[EJson], Double]

  val path: ResourcePath =
    ResourcePath.root() / ResourceName("data")

  val BoolsData: List[Boolean] =
    List(true, true, false, true, false)

  val sst = envT(
    TypeStat.bool(3.0, 2.0),
    TypeST(TypeF.simple[Fix[EJson], SST[Fix[EJson], Double]](SimpleType.Bool))).embed

  val srcSst = envT(
    TypeStat.str(5.0, 2.0, 2.0, "/a", "/c"),
    TypeST(TypeF.simple[Fix[EJson], SST[Fix[EJson], Double]](SimpleType.Str))).embed

  val aggSst = envT(
    TypeStat.coll(5.0, Some(2.0), Some(2.0)),
    TypeST(TypeF.map[Fix[EJson], SST[Fix[EJson], Double]](
      IMap(
        EJson.str[Fix[EJson]](SourceKey) -> srcSst,
        EJson.str[Fix[EJson]](ValueKey) -> sst),
      None))).embed

  val schema = SstSchema.fromSampled(100.0, sst)

  val parsedResult: QueryResult[IO] =
    QueryResult.parsed(
      QDataDecode[RValue],
      Stream.emits(BoolsData.map(RValue.rBoolean(_))),
      ScalarStages.Id)

  val unparsedResult: QueryResult.Unparsed[IO] =
    QueryResult.typed(
      ParsableType.Json(JsonVariant.LineDelimited, false),
      Stream.emits(BoolsData.mkString("\n").getBytes(Charset.forName("UTF-8"))),
      ScalarStages.Id)

  val resourceSchema: ResourceSchema[IO, SstConfig[Fix[EJson], Double], (ResourcePath, CompositeResult[IO, QueryResult[IO]])] =
    CompositeResourceSchema[IO, Fix[EJson], Double](SstEvalConfig(20L, 1L, 100L))

  "computes an SST of parsed data" >>* {
    resourceSchema(defaultCfg, (path, Left(parsedResult)), 1.hour) map { qsst =>
      qsst must_= Some(schema)
    }
  }

  "computes an SST of unparsed data" >>* {
    resourceSchema(defaultCfg, (path, Left(unparsedResult)), 1.hour) map { qsst =>
      qsst must_= Some(schema)
    }
  }

  "computes an SST of gzipped data" >>* {
    val gzippedResult =
      QueryResult.compressed(
        CompressionScheme.Gzip,
        unparsedResult.modifyBytes(_ through gzip.compress(50)))

    resourceSchema(defaultCfg, (path, Left(gzippedResult)), 1.hour) map { qsst =>
      qsst must_= Some(schema)
    }
  }

  "computes an SST from aggregated results" >>* {
    val as = Stream(true, true).repeat
    val bs = Stream(true, false).repeat
    val cs = Stream(false)

    def boolResult(bs: Stream[Pure, Boolean]) =
      QueryResult.parsed(
        QDataDecode[RValue],
        bs.map(RValue.rBoolean(_)).covary[IO],
        ScalarStages.Id)

    val agg = Stream(
      (ResourcePath.root() / ResourceName("a")) -> boolResult(as),
      (ResourcePath.root() / ResourceName("b")) -> boolResult(bs),
      (ResourcePath.root() / ResourceName("c")) -> boolResult(cs))

    resourceSchema(defaultCfg, (path, Right(agg.covary[IO])), 1.hour) map { qsst =>
      qsst must_= Some(SstSchema.fromSampled(100, aggSst))
    }
  }

  "emits parser errors as ResourceError" >>* {
    val badResult =
      QueryResult.typed[IO](
        ParsableType.Json(JsonVariant.LineDelimited, false),
        Stream.emits("""{ "foo": sdlfkj""".getBytes(Charset.forName("UTF-8"))),
        ScalarStages.Id)

    val qsst = resourceSchema(defaultCfg, (path, Left(badResult)), 1.hour)

    MonadError_[IO, ResourceError].attempt(qsst) map { r =>
      r must be_-\/.like {
        case ResourceError.MalformedResource(p, expect, _, _) =>
          p must_= path
          expect must_=== "ldjson"
      }
    }
  }

  "error when stages modifies input" >>* {
    val withInstrs =
      QueryResult.stages.set(ScalarStages(IdStatus.IncludeId, List()))(parsedResult)

    resourceSchema(defaultCfg, (path, Left(withInstrs)), 1.hour)
      .attempt
      .map(_ must beLeft(beAnInstanceOf[IllegalArgumentException]))
  }

  "halts computation after time limit" >> todo
}
