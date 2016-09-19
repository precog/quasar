/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.std

import quasar.Predef._
import quasar.{Data, LogicalPlan, Qspec}, LogicalPlan._

import matryoshka._
import org.specs2.execute._
import org.scalacheck.{Arbitrary, Gen}
import org.threeten.bp.{Instant, ZoneOffset}

trait StdLibTestRunner {
  def nullary(
    prg: Fix[LogicalPlan],
    expected: Data): Result

  def unary(
    prg: Fix[LogicalPlan] => Fix[LogicalPlan],
    arg: Data,
    expected: Data): Result

  def binary(
    prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
    arg1: Data, arg2: Data,
    expected: Data): Result

  def ternary(
    prg: (Fix[LogicalPlan], Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
    arg1: Data, arg2: Data, arg3: Data,
    expected: Data): Result

  /** Defines the domain of values for `Data.Int` for which the implementation is
    * well-behaved.
    */
  def intDomain: Gen[BigInt]

  /** Defines the domain of values for `Data.Int` for which the implementation is
    * well-behaved.
    */
  def decDomain: Gen[BigDecimal]

  /** Defines the domain of values for `Data.Str` for which the implementation is
    * well-behaved.
    */
  def stringDomain: Gen[String]
}

/** Abstract spec for the standard library, intended to be implemented for each
  * library implementation, of which there are one or more per backend.
  */
abstract class StdLibSpec extends Qspec {

  def tests(runner: StdLibTestRunner) = {
    import runner._

    implicit val arbBigInt = Arbitrary[BigInt] { runner.intDomain }
    implicit val arbBigDecimal = Arbitrary[BigDecimal] { runner.decDomain }
    implicit val arbString = Arbitrary[String] { runner.stringDomain }

    "StringLib" >> {
      import StringLib._

      "Concat" >> {
        "any strings" >> prop { (str1: String, str2: String) =>
          binary(Concat(_, _).embed, Data.Str(str1), Data.Str(str2), Data.Str(str1 + str2))
        }
      }

      // NB: `Like` is simplified to `Search`

      "Search" >> {
        todo
      }

      "Length" >> {
        "any string" >> prop { (str: String) =>
          unary(Length(_).embed, Data.Str(str), Data.Int(str.length))
        }
      }

      "Lower" >> {
        "any string" >> prop { (str: String) =>
          unary(Lower(_).embed, Data.Str(str), Data.Str(str.toLowerCase))
        }
      }

      "Upper" >> {
        "any string" >> prop { (str: String) =>
          unary(Upper(_).embed, Data.Str(str), Data.Str(str.toUpperCase))
        }
      }

      "Substring" >> {
        "simple" >> {
          // NB: not consistent with PostgreSQL, which is 1-based for `start`
          ternary(Substring(_, _, _).embed, Data.Str("Thomas"), Data.Int(1), Data.Int(3), Data.Str("hom"))
        }

        "empty string and any offsets" >> prop { (start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          val length = length0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(""), Data.Int(start), Data.Int(length), Data.Str(""))
        }

        "any string with entire range" >> prop { (str: String) =>
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(0), Data.Int(str.length), Data.Str(str))
        }

        "any string with 0 length" >> prop { (str: String, start0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(0), Data.Str(""))
        }

        "any string and offsets" >> prop { (str: String, start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          val length = length0 % 1000

          // NB: this is the MongoDB behavior, for lack of a better idea
          val expected = StringLib.safeSubstring(str, start, length)
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(length), Data.Str(expected))
        }
      }

      "Boolean" >> {
        "true" >> {
          unary(Boolean(_).embed, Data.Str("true"), Data.Bool(true))
        }

        "false" >> {
          unary(Boolean(_).embed, Data.Str("false"), Data.Bool(false))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Integer" >> {
        "any BigInt in the domain" >> prop { (x: BigInt) =>
          unary(Integer(_).embed, Data.Str(x.toString), Data.Int(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Decimal" >> {
        "any BigDecimal in the domain" >> prop { (x: BigDecimal) =>
          unary(Decimal(_).embed, Data.Str(x.toString), Data.Dec(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Null" >> {
        "null" >> {
          unary(Null(_).embed, Data.Str("null"), Data.Null)
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "ToString" >> {
        "string" >> prop { (str: String) =>
          unary(ToString(_).embed, Data.Str(str), Data.Str(str))
        }

        "null" >> {
          unary(ToString(_).embed, Data.Null, Data.Str("null"))
        }

        "true" >> {
          unary(ToString(_).embed, Data.Bool(true), Data.Str("true"))
        }

        "false" >> {
          unary(ToString(_).embed, Data.Bool(false), Data.Str("false"))
        }

        "int" >> prop { (x: BigInt) =>
          unary(ToString(_).embed, Data.Int(x), Data.Str(x.toString))
        }

        "dec" >> prop { (x: BigDecimal) =>
          unary(ToString(_).embed, Data.Dec(x), Data.Str(x.toString))
        }

        // TODO: need `Arbitrary`s for all of these
        // "timestamp" >> prop { (x: Instant) =>
        //   unary(ToString(_).embed, Data.Timestamp(x), Data.Str(x.toString))
        // }
        //
        // "date" >> prop { (x: LocalDate) =>
        //   unary(ToString(_).embed, Data.Date(x), Data.Str(x.toString))
        // }
        //
        // "time" >> prop { (x: LocalTime) =>
        //   unary(ToString(_).embed, Data.Time(x), Data.Str(x.toString))
        // }
        //
        // "interval" >> prop { (x: Duration) =>
        //   unary(ToString(_).embed, Data.Interval(x), Data.Str(x.toString))
        // }
      }
    }

    "DateLib" >> {
      import DateLib._

      "TimeOfDay" >> {
        "timestamp" >> {
          val now = Instant.now
          val expected = now.atZone(ZoneOffset.UTC).toLocalTime
          unary(TimeOfDay(_).embed, Data.Timestamp(now), Data.Time(expected))
        }
      }
    }
  }
}
