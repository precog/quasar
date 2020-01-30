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

package quasar.impl.storage

import slamdata.Predef.{Int, String}

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Ref
import scalaz.IMap
import scalaz.std.anyVal._
import scalaz.std.string._

object RefIndexedStoreSpec extends RefSpec(Ref.unsafe[IO, Int](0))

abstract class RefSpec(idxRef: Ref[IO, Int]) extends IndexedStoreSpec[IO, Int, String] {
  val emptyStore =
    Resource.liftF(Ref.of[IO, IMap[Int, String]](IMap.empty).map(RefIndexedStore(_)))

  val freshIndex = idxRef.modify(i => (i + 1, i + 1))

  val valueA = "A"

  val valueB = "B"
}
