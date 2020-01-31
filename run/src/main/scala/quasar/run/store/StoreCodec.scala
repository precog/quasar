/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.run.store

import slamdata.Predef._

import eu.timepit.refined.auto._
import quasar.api.destination.{DestinationName, DestinationRef, DestinationType}
import quasar.fp.numeric.Positive

import argonaut.{HCursor, CodecJson, DecodeJson, DecodeResult, EncodeJson, Json}

object StoreCodec {
  def destinationRefCodec[C: DecodeJson: EncodeJson]: CodecJson[DestinationRef[C]] = {
    val RefNameField = "name"
    val RefTypeNameField = "typeName"
    val RefTypeVersionField = "typeVersion"
    val RefConfigField = "config"

    def encode(destRef: DestinationRef[C]): Json =
      Json(
        RefNameField -> Json.jString(destRef.name.value),
        RefTypeNameField -> Json.jString(destRef.kind.name.value),
        RefTypeVersionField -> Json.jNumber(destRef.kind.version),
        RefConfigField -> EncodeJson.of[C].encode(destRef.config))

    def decode(c: HCursor): DecodeResult[DestinationRef[C]] =
      for {
        name <- c.get[String](RefNameField).map(DestinationName(_))
        kindTypeName <- c.get[String](RefTypeNameField).flatMap(kn =>
          DestinationType.stringName.getOption(kn).fold(
            DecodeResult.fail[DestinationType.Name](s"$RefTypeNameField does not match DestinationType.NameP", c.history))(
            DecodeResult.ok[DestinationType.Name](_)))
        kindVersion <- c.get[Long](RefTypeVersionField).flatMap(kv =>
          Positive(kv).fold(
            DecodeResult.fail[Positive](s"$RefTypeVersionField field is not Positive", c.history))(
            DecodeResult.ok[Positive](_)))
        configDoc <- c.get[C](RefConfigField)
      } yield DestinationRef[C](DestinationType(kindTypeName, kindVersion), name, configDoc)

    CodecJson[DestinationRef[C]](encode(_), decode(_))
  }
}
