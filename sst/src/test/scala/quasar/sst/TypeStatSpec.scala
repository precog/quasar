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

import slamdata.Predef.{Char => SChar, Int => SInt, _}
import quasar.contrib.algebra._
import quasar.contrib.specs2.Spec
import quasar.ejson.{Decoded, DecodeEJson, EJson, EncodeEJson}
import quasar.fp.numeric.SampleStats

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

  "combining int and dec widens to dec" >> prop { (x: SInt, y: SInt) =>
    val xi = TypeStat.int(SampleStats.one(Real(x)), BigInt(x), BigInt(x))
    val xd = TypeStat.dec(SampleStats.one(Real(x)), BigDecimal(x), BigDecimal(x))
    val yd = TypeStat.dec(SampleStats.one(Real(y)), BigDecimal(y), BigDecimal(y))

    (xi + yd) must equal(xd + yd)
  }

  "combining char and str widens to str" >> prop { (c: SChar, s: String) =>
    val cc = TypeStat.char(SampleStats.one(Real(c.toInt)), c, c)
    val cs = TypeStat.str(Real(1), Real(1), Real(1), c.toString, c.toString)
    val ss = TypeStat.str(Real(1), Real(s.length), Real(s.length), s, s)

    (cc + ss) must equal(cs + ss)
  }

  "EJson codec" >> prop { ts: TypeStat[Double] =>
    DecodeEJson[TypeStat[Double]].decode(
      EncodeEJson[TypeStat[Double]].encode[Fix[EJson]](ts)
    ) must equal(Decoded.success(ts))
  }
}
