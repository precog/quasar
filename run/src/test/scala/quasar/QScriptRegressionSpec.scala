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

package quasar.run

import slamdata.Predef._

import cats.effect.IO

import matryoshka.data.Fix

import shims._

object QScriptRegressionSpec extends CountingRegressionSpec {

  val countingEvaluator = new RegressionQueryEvaluator[Fix, IO]

  "optimized qscript" should {

    "have a Read" >> {

      val q1 = "select * from foo"
      q1 in {
        val result = count(q1)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(0)
      }
    }

    "have correct number of LeftShift" >> {
      val q1 = "select a{_}, b[_], c{_:} from foo"
      q1 in {
        val result = count(q1)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(3)
      }

      val q2 = "(select a{_} from foo) union all (select b[_] from bar)"
      q2 in {
        val result = count(q2)

        result must countReadAs(2)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(2)
      }

      // ch2128
      val q3 = """select (SELECT * FROM (SELECT t{_}.testField FROM `real-giraffe.json` AS t) AS t2 WHERE type_of(t2) = "string") AS testField, (SELECT kv{_} FROM `real-giraffe.json` AS kv) AS S"""
      q3 in {
        val result = count(q3)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(1)
      }

      // ch1473
      val q4 = "select first[_].second{_}.third as value, first[_].second{_:} as key from mydata"
      q4 in {
        val result = count(q4)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(2)
      }

      // ch4703
      val q5 = "select a[*][*], a[*][*].b[*] from zips"
      q5 in {
        val result = count(q5)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(3)
      }

      // ch4758
      val q6 = "select topObj{_:} as k1, topObj{_}{_:} as k2, topObj{_}{_} as v2 from `nested.data`"
      q6 in {
        val result = count(q6)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(2)
      }
    }

    "handle real-world queries" >> {

      // ch2068
      val q1 = """
       |SELECT
       |  (SELECT * FROM (SELECT t2{_:} FROM `post-giraffe-unnested.data` AS t2) AS ot2 WHERE ot2 = "S") AS Key,
       |  (SELECT t3.dateTime FROM (SELECT t4.dateTime AS dateTime, t4{_:} AS t5 FROM `post-giraffe-unnested.data` AS t4) AS t3 WHERE type_of(t3.dateTime) = "number" AND (t3.t5 = "MAS" OR t3.t5 = "S")) AS DateTime
       |FROM `post-giraffe-unnested.data`
      """.stripMargin
      q1 in {
        val result = count(q1)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(1)
      }

      // ch3531
      val q2 = """
       |SELECT
       |  (SELECT * FROM (SELECT ty.ZZ FROM `post-giraffe-subset` AS ty) AS v WHERE type_of(v) = "string") AS g10,
       |  (SELECT * FROM (SELECT ty.ZZ FROM `post-giraffe-subset` AS ty) AS v) AS g8,
       |  (SELECT * FROM (SELECT ty.ZZ FROM `post-giraffe-subset` AS ty) AS v) AS g9
       |FROM `post-giraffe-subset`
      """.stripMargin
      q2 in {
        val result = count(q2)

        result must countReadAs(1)
        result must countInterpretedReadAs(0)
        result must countLeftShiftAs(0)
      }
    }
  }
}
