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

import cats.Eq
import cats.effect.{IO, Resource}

import org.mapdb.{DBMaker, Serializer}

import java.lang.Integer

import scala.concurrent.ExecutionContext.Implicits.global

import MapDbByteStoresSpec._

final class MapDbByteStoresSpec extends ByteStoresSpec[IO, Integer] {
  val byteStores =
    Resource.make(IO(DBMaker.memoryDB().make()))(db => IO(db.close()))
      .evalMap(db => MapDbByteStores("mapdb-bytestores-spec", db, Serializer.INTEGER))

  val k1 = new Integer(3)
  val k2 = new Integer(7)
}

object MapDbByteStoresSpec {
  implicit val integerEq: Eq[Integer] =
    Eq.fromUniversalEquals
}
