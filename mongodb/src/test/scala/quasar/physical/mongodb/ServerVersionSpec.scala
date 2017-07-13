/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.QuasarSpecification

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class ServerVersionSpec extends org.specs2.scalaz.Spec with QuasarSpecification with ArbitraryServerVersion {
  "ServerVersion" should {
    "parse simple version" >> {
      ServerVersion.fromString("2.6.11") must beRightDisjunction(ServerVersion(2, 6, Some(11), ""))
    }

    "parse truncated version" >> {
      ServerVersion.fromString("2.6") must beRightDisjunction(ServerVersion(2, 6, None, ""))
    }

    "parse multiple digits" >> {
      ServerVersion.fromString("22.67.174") must beRightDisjunction(ServerVersion(22, 67, Some(174), ""))
    }

    "parse with extra" >> {
      ServerVersion.fromString("3.2.7-50-g67602c7") must beRightDisjunction(ServerVersion(3, 2, Some(7), "50-g67602c7"))
    }

    "parse with extra (space-separated)" >> {
      ServerVersion.fromString("0.11.5 RC1") must beRightDisjunction(ServerVersion(0, 11, Some(5), "RC1"))
    }

    "parse with extra (_-separated)" >> {
      ServerVersion.fromString("0.11.5_00") must beRightDisjunction(ServerVersion(0, 11, Some(5), "00"))
    }

    "parse with extra and no revision" >> {
      ServerVersion.fromString("3.2-foo") must beRightDisjunction(ServerVersion(3, 2, None, "foo"))
    }

    "parse with unexpected non-revision" >> {
      ServerVersion.fromString("3.2.foo") must beRightDisjunction(ServerVersion(3, 2, None, "foo"))
    }

    "fail with missing minor version" >> {
      ServerVersion.fromString("4.abc") must beLeftDisjunction("Unable to parse server version: 4.abc")
    }

    "never throw during parsing" >> prop { (str: String) =>
        \/.fromTryCatchNonFatal(ServerVersion.fromString(str)) must beRightDisjunction
    }

    "round-trip any" >> prop { (vers: ServerVersion) =>
      ServerVersion.fromString(vers.shows) must beRightDisjunction(vers)
    }

    implicit val ord: scala.math.Ordering[ServerVersion] = Order[ServerVersion].toScalaOrdering

    "compare with same major/minor" >> prop { (maj: Int, min: Int, rev: Int, extra: String) =>
      // NB: any revision number is greater than none
      ServerVersion(maj, min, Some(rev), extra) must
        beGreaterThan(ServerVersion(maj, min, None, ""))
    }

    "compare with different major" >> prop { (maj: Int, min: Int, rev: Int, extra: String) =>
      maj < Int.MaxValue ==> {
        ServerVersion(maj, min, Some(rev), extra) must
          beLessThan(ServerVersion(maj + 1, 0, None, ""))
      }
    }

    "compare with different minor" >> prop { (maj: Int, min: Int, rev: Int, extra: String) =>
      min < Int.MaxValue ==> {
        ServerVersion(maj, min, Some(rev), extra) must
          beLessThan(ServerVersion(maj, min + 1, None, ""))
      }
    }
  }

  checkAll(order.laws[ServerVersion])
}
