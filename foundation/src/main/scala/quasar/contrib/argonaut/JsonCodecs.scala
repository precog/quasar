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

package quasar.contrib.argonaut

import slamdata.Predef._

import scala.Float

import argonaut.DecodeJson

object JsonCodecs {

  implicit def decodeDouble: DecodeJson[Double] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toDouble), "Double")

  implicit def decodeFloat: DecodeJson[Float] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toFloat), "Float")

  implicit def decodeInt: DecodeJson[Int] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toInt), "Int")

  implicit def decodeLong: DecodeJson[Long] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toLong), "Long")

  implicit def decodeShort: DecodeJson[Short] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toShort), "Short")

  implicit def decodeBigInt: DecodeJson[BigInt] =
    DecodeJson.optionDecoder(_.number.flatMap(_.toBigInt), "BigInt")

  implicit def decodeBigDecimal: DecodeJson[BigDecimal] =
    DecodeJson.optionDecoder(_.number.map(_.toBigDecimal), "BigDecimal")

  implicit def decodeBoolean: DecodeJson[Boolean] =
    DecodeJson.optionDecoder(_.bool, "Boolean")
}
