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

package quasar.api.services

import slamdata.Predef._
import quasar.contrib.pathy.APath
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fs.mount._
import quasar.api.JsonFormat.{SingleArray, LineDelimited}
import quasar.api.JsonPrecision.{Precise, Readable}
import quasar.api.MessageFormat.JsonContentType

import argonaut.{Json, Argonaut}
import Argonaut._
import org.http4s.{MediaType, Charset, EntityEncoder}
import org.http4s.headers.`Content-Type`
import org.scalacheck.Arbitrary
import scalaz._
import scalaz.concurrent.Task

object Fixture {

  implicit val arbJson: Arbitrary[Json] = Arbitrary(Arbitrary.arbitrary[String].map(jString(_)))

  val jsonReadableLine = JsonContentType(Readable,LineDelimited)
  val jsonPreciseLine = JsonContentType(Precise,LineDelimited)
  val jsonReadableArray = JsonContentType(Readable,SingleArray)
  val jsonPreciseArray = JsonContentType(Precise,SingleArray)

  sealed abstract class JsonType

  case class PreciseJson(value: Json) extends JsonType
  object PreciseJson {
    implicit val entityEncoder: EntityEncoder[PreciseJson] =
      EntityEncoder.encodeBy(`Content-Type`(jsonPreciseArray.mediaType, Charset.`UTF-8`)) { pJson =>
        org.http4s.argonaut.jsonEncoder.toEntity(pJson.value)
      }
    implicit val arb: Arbitrary[PreciseJson] = Arbitrary(Arbitrary.arbitrary[Json].map(PreciseJson(_)))
  }

  case class ReadableJson(value: Json) extends JsonType
  object ReadableJson {
    implicit val entityEncoder: EntityEncoder[ReadableJson] =
      EntityEncoder.encodeBy(`Content-Type`(jsonReadableArray.mediaType, Charset.`UTF-8`)) { rJson =>
        org.http4s.argonaut.jsonEncoder.toEntity(rJson.value)
      }
    implicit val arb: Arbitrary[ReadableJson] = Arbitrary(Arbitrary.arbitrary[Json].map(ReadableJson(_)))
  }

  implicit val readableLineDelimitedJson: EntityEncoder[List[ReadableJson]] =
    EntityEncoder.stringEncoder.contramap[List[ReadableJson]] { rJsons =>
      rJsons.map(rJson => Argonaut.nospace.pretty(rJson.value)).mkString("\n")
    }.withContentType(`Content-Type`(jsonReadableLine.mediaType, Charset.`UTF-8`))

  implicit val preciseLineDelimitedJson: EntityEncoder[List[PreciseJson]] =
    EntityEncoder.stringEncoder.contramap[List[PreciseJson]] { pJsons =>
      pJsons.map(pJson => Argonaut.nospace.pretty(pJson.value)).mkString("\n")
    }.withContentType(`Content-Type`(jsonPreciseLine.mediaType, Charset.`UTF-8`))

  case class Csv(value: String)
  object Csv {
    implicit val entityEncoder: EntityEncoder[Csv] =
      EntityEncoder.encodeBy(`Content-Type`(MediaType.`text/csv`, Charset.`UTF-8`)) { csv =>
        EntityEncoder.stringEncoder(Charset.`UTF-8`).toEntity(csv.value)
      }
  }

  def mountingInter(mounts: Map[APath, MountConfig]): Task[Mounting ~> Task] = {
    type MEff[A] = Coproduct[Task, MountConfigs, A]
    TaskRef(mounts).map { configsRef =>

      val mounter: Mounting ~> Free[MEff, ?] = Mounter.trivial[MEff]

      val meff: MEff ~> Task =
        reflNT[Task] :+: KeyValueStore.impl.fromTaskRef(configsRef)

      foldMapNT(meff) compose mounter
    }
  }
}
