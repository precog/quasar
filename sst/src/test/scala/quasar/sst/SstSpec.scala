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

package quasar.sst

import quasar.contrib.algebra._
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson._
import quasar.ejson.implicits._
import quasar.fp._, Helpers._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.scalacheck.Parameters
import qdata.QData
import scalaz.Show
import spire.math.Real

object SstSpec extends quasar.Qspec with EJsonArbitrary {

  implicit val params = Parameters(maxSize = 10)
  implicit val realShow: Show[Real] = Show.showFromToString

  type J = Fix[EJson]
  type JS = Fix[Json]

  val norm: EJson[J] => EJson[J] = {
    // Sidestep all the decimal conversion by truncating
    case CommonEJson(Dec(v)) => ExtEJson(Int(v.toBigInt))
    case ExtEJson(Char(i)) => CommonEJson(Str(i.toString))
    case j => j
  }

  "creating from qdata is equivalent to EJson" >> prop { js: JS =>
    val normd = js.transCata[J](EJson.fromJson(EJson.str[J](_)) andThen norm)
    QData.convert[J, SST[J, Real]](normd) must_= SST.fromEJson(Real(1), normd)
  }
}
