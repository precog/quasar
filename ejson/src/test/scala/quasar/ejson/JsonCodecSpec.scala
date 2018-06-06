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

package quasar.ejson

import slamdata.Predef._
import quasar.Qspec
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota._

import matryoshka.{equalTEqual => _, _}
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.scalacheck._
import scalaz._, Scalaz._

final class JsonCodecSpec extends Qspec with EJsonArbitrary {
  import JsonCodec.DecodingFailed
  import Common.Optics.{nul, str}

  implicit val params = Parameters(maxSize = 10)

  type E = Fix[EJson]
  type J = Fix[Json]

  val decodeMƒ: CoalgebraM[DecodingFailed[J] \/ ?, EJson, J] =
    JsonCodec.decodeƒ[J] >>> (_.run)

  val roundtrip: E => DecodingFailed[J] \/ E =
    _.cata(JsonCodec.encodeƒ[J]).anaM[E](decodeMƒ)

  val soloKeys = JsonCodec.ExtKeys \\ ISet.fromList(List(JsonCodec.KeyK, JsonCodec.ValueK, JsonCodec.MetaK))

  "faithfully roundtrip EJson" >> prop { e: E =>
    roundtrip(e).toEither must beRight(equal(e))
  }

  soloKeys.toList foreach { k =>
    s"fail when singleton map with `$k` key maps to unexpected value" >> {
      val n = CommonJson(nul[J]()).embed
      val j = ObjJson(Obj(ListMap(k -> n))).embed
      j.anaM[E](decodeMƒ) must beLike {
        case -\/(DecodingFailed(_, v)) => v must_= n
      }
    }
  }

  "map keys beginning with the codec sigil are preserved" >> prop { (k10: String, k20: String, v1: E, v2: E) =>
    val (k1, k2) = (JsonCodec.Sigil.toString + k10, JsonCodec.Sigil.toString + k20)
    val m = ExtEJson(Map(List(
      CommonEJson(str[E](k1)).embed -> v1,
      CommonEJson(str[E](k2)).embed -> v2
    ))).embed

    roundtrip(m).toEither must beRight(equal(m))
  }

  "map keys equal to one of the codec keys are preserved" >> prop { v: E =>
    val m = ExtEJson(Map(JsonCodec.ExtKeys.toList map { k =>
      CommonEJson(str[E](k)).embed -> v
    })).embed

    roundtrip(m).toEither must beRight(equal(m))
  }

  "map keys equal to one of the codec keys with additional sigil prefix are preserved" >> prop { v: E =>
    val m = ExtEJson(Map(JsonCodec.ExtKeys.toList map { k =>
      CommonEJson(str[E](JsonCodec.Sigil.toString + k)).embed -> v
    })).embed

    roundtrip(m).toEither must beRight(equal(m))
  }
}
