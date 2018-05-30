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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.specs2.Spec
import quasar.fp._

import matryoshka._
import scalaz._, Scalaz._
import scalaz.scalacheck.{ScalazProperties => propz}

final class TaggedSpec extends Spec with TaggedArbitrary {
  checkAll(propz.equal.laws[Tagged[Int]])
  checkAll(propz.traverse1.laws[Tagged])
}
