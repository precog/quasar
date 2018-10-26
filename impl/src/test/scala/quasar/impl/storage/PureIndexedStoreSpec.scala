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

package quasar.impl.storage

import slamdata.Predef.{Int, String}
import quasar.contrib.cats.stateT._
import quasar.contrib.cats.effect.stateT.catsStateTEffect

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats.data.StateT
import cats.effect.IO
import cats.syntax.applicative._
import scalaz.IMap
import scalaz.std.anyVal._
import scalaz.std.string._
import shims._

final class PureIndexedStoreSpec extends
    IndexedStoreSpec[StateT[IO, IMap[Int, String], ?], Int, String] {

  val emptyStore =
    PureIndexedStore[StateT[IO, IMap[Int, String], ?], Int, String]
      .pure[StateT[IO, IMap[Int, String], ?]]

  val freshIndex = StateT.liftF(IO(Random.nextInt()))

  val valueA = "A"

  val valueB = "B"
}
