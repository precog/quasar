/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.fs.mount

import quasar.Predef.{Map, String, ArrowAssoc}
import quasar.SKI._
import quasar.fs.APath
import quasar.fp.PathyCodecJson._

import scala.AnyVal

import argonaut._, Argonaut._
import monocle.Iso
import pathy.Path._
import scalaz.syntax.bifunctor._
import scalaz.syntax.foldable._
import scalaz.std.tuple._
import scalaz.std.list._

final case class MountingsConfig2(toMap: Map[APath, MountConfig2]) extends AnyVal

object MountingsConfig2 {
  val empty: MountingsConfig2 =
    MountingsConfig2(Map())

  val mapIso: Iso[MountingsConfig2, Map[APath, MountConfig2]] =
    Iso((_: MountingsConfig2).toMap)(MountingsConfig2(_))

  implicit val mountingsConfigEncodeJson: EncodeJson[MountingsConfig2] =
    EncodeJson.of[Map[String, MountConfig2]]
      .contramap(_.toMap.map(_.leftMap(posixCodec.printPath(_))))

  implicit val mountingsConfigDecodeJson: DecodeJson[MountingsConfig2] =
    DecodeJson.of[Map[String, MountConfig2]]
      .flatMap(m0 => DecodeJson(κ(m0.toList.foldLeftM(Map[APath, MountConfig2]()) {
        case (m, (s, mc)) =>
          jString(s).as[APath](aPathDecodeJson).map(p => m + (p -> mc))
      }))).map(MountingsConfig2(_))
}
