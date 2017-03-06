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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.numeric.Natural
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.Arbitraries._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.scalacheck.numeric._
import org.scalacheck.Arbitrary
import scalaz._

final class AsQNameSpec extends quasar.Qspec {
  type Result[A] = MarkLogicPlannerError \/ A

  implicit val reasonableNatural: Arbitrary[Natural] =
    Arbitrary(chooseRefinedNum[Refined, Long, NonNegative](0L, 1000L))

  "asQName" should {
    "identity for a valid QName" >> prop { qn: QName =>
      asQName[Result](qn.toString) must_= \/.right(qn)
    }

    "prefix numerals" >> prop { n: Natural =>
      asQName[Result](n.value.toString) must_= \/.right(QName.local(NCName(Refined.unsafeApply(s"_$n"))))
    }

    "error when string is not a QName" >> {
      val notAQName = "92foo"
      asQName[Result](notAQName) must_= \/.left(MarkLogicPlannerError.invalidQName(notAQName))
    }
  }
}
