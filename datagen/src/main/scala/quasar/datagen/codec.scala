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

import slamdata.Predef.{Stream => _, _}
import quasar.{Data, DataCodec}
import quasar.ejson.{optics => eoptics, EJson}
import quasar.contrib.iota.copkTraverse

import argonaut.{Json, Parse}
import fs2.{Pipe, Stream}
import matryoshka.{Corecursive, Recursive}
import matryoshka.implicits._
import scalaz.syntax.show._

object codec {
  /** Decodes EJson from JSON-encoded precise Data. */
  def ejsonDecodePreciseData[F[_], J](implicit J: Corecursive.Aux[J, EJson]): Pipe[F, String, J] =
    _.flatMap(Parse.parseWith(_, Stream.emit, failedStream))
      .flatMap(js => DataCodec.Precise.decode(js).fold(e => failedStream(e.message), Stream.emit))
      .map(_.ana[J](Data.toEJson[EJson] andThen (_.run getOrElse eoptics.nul[Data]())))

  /** Encodes EJson as JSON-encoded precise Data. */
  def ejsonEncodePreciseData[F[_], J](implicit J: Recursive.Aux[J, EJson]): Pipe[F, J, String] =
    _.map(_.cata(Data.fromEJson))
      .flatMap(d =>
        DataCodec.Precise.encode(d)
          .fold(failedStream[F, Json](s"Unable to encode as JSON: ${d.shows}"))(Stream.emit(_).covary[F]))
      .map(_.nospaces)
}
