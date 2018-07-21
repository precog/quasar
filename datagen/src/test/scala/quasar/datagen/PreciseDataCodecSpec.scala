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

package quasar.datagen

import slamdata.Predef.{Option, String}
import quasar.common.data.{Data, DataGenerators}
import quasar.frontend.data.DataCodec
import quasar.ejson._

import fs2.{Pure, Stream}
import matryoshka.data.Fix
import scalaz.std.list._

final class PreciseDataCodecSpec extends quasar.Qspec with DataGenerators {
  type J = Fix[EJson]

  def parsePrecise(s: String): Option[Data] =
    DataCodec.parse(s)(DataCodec.Precise).toOption

  def renderPrecise(d: Data): Option[String] =
    DataCodec.render(d)(DataCodec.Precise)

  "precise data codec roundtrips" >> prop { d: Data =>
    val encoded = renderPrecise(d)
    // NB: Because we may have elided any `NA` values from `d` when rendering.
    val decoded = encoded flatMap parsePrecise

    Stream.emit(encoded)
      .unNone
      .through(codec.ejsonDecodePreciseData[Pure, J])
      .through(codec.ejsonEncodePreciseData[Pure, J])
      .map(parsePrecise)
      .unNone
      .toList must equal(decoded.toList)
  }
}
