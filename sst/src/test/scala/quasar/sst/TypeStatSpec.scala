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
import quasar.contrib.algebra._
import quasar.contrib.specs2.Spec
import quasar.ejson.{Decoded, DecodeEJson, EJson, EncodeEJson}

import matryoshka.data.Fix
import scalaz.Show
import scalaz.std.anyVal._
import scalaz.scalacheck.{ScalazProperties => propz}
import spire.laws.arb._
import spire.math.Real
import spire.std.double._

final class TypeStatSpec extends Spec with TypeStatArbitrary {
  import TypeStat._

  implicit val realShow: Show[Real] = Show.showFromToString

  checkAll(propz.equal.laws[TypeStat[Real]])
  checkAll(propz.semigroup.laws[TypeStat[Real]])

  "combining preserves shape" >> prop { ts: TypeStat[Real] =>
    count[Real].isEmpty(ts + ts) must_== count[Real].isEmpty(ts)
  }

  "combining sums counts" >> prop { (x: TypeStat[Real], y: TypeStat[Real]) =>
    (x + y).size must equal(x.size + y.size)
  }

  "EJson codec" >> prop { ts: TypeStat[Double] =>
    DecodeEJson[TypeStat[Double]].decode(
      EncodeEJson[TypeStat[Double]].encode[Fix[EJson]](ts)
    ) must equal(Decoded.success(ts))
  }
}
