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

import slamdata.Predef._
import quasar.{Data, DataArbitrary}, DataArbitrary.dataShrink
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import java.math.MathContext
import java.time._
import scala.collection.Traversable

import eu.timepit.refined.auto._
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import org.specs2.scalacheck.Parameters
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

abstract class StructuralPlannerSpec[F[_]: Monad, FMT](
  implicit SP: StructuralPlanner[F, FMT]
) extends XQuerySpec {

  def toM: F ~> M

  implicit val scalacheckParams = Parameters(maxSize = 10)

  // FIXME: No idea why this is necessary, but ScalaCheck arbContainer
  //        demands it and can't seem to find one in this context.
  implicit def listToTraversable[A](as: List[A]): Traversable[A] = as

  implicit val arbitraryData: Arbitrary[Data] = {
    val genKey = Gen.alphaNumChar flatMap (c => Gen.alphaNumStr map (c.toString + _))
    val genDbl = Gen.choose(-1000.0, 1000.0)

    val secondsInDay: Long        = 24L * 60 * 60
    //  TODO: Many negative years parse fine, but some don't, randomly
    //  1600-01-01 to 9999-12-31
    val genSeconds: Gen[Long]     = Gen.choose(-11676096000L, 253402214400L)
    val genSecondOfDay: Gen[Long] = Gen.choose(0L, secondsInDay - 1)
    val genMillis: Gen[Long]      = Gen.choose(0L, 999L)
    val genNanos: Gen[Long]       = genMillis map (_ * 1000000)

    val genInstant: Gen[Instant]   = (genSeconds |@| genNanos)(Instant.ofEpochSecond)
    val genDuration: Gen[Duration] = (genSeconds |@| genNanos)(Duration.ofSeconds)
    val genDate: Gen[LocalDate]    = genSeconds map (s => LocalDate.ofEpochDay(s / secondsInDay))
    val genTime: Gen[LocalTime]    = genSecondOfDay map LocalTime.ofSecondOfDay

    val genAtomic = Gen.oneOf[Data](
                                 Data.Null,
      Gen.alphaStr           map Data.Str,
      arbitrary[Boolean]     map Data.Bool,
      genDbl                 map (d => Data.Dec(BigDecimal(d, MathContext.DECIMAL32))),
      arbitrary[Int]         map (i => Data.Int(BigInt(i))),
      genInstant             map Data.Timestamp,
      genDate                map Data.Date,
      genTime                map Data.Time,
      genDuration            map Data.Interval,
      arbitrary[Array[Byte]] map Data.Binary.fromArray)

    Arbitrary(DataArbitrary.genNested(genKey, genAtomic))
  }

  val emptyArr: Data              = Data._arr(List())
  val emptyObj: Data              = Data._obj(ListMap())
  val lit     : Data => F[XQuery] = DataPlanner[F, FMT](_)


  case class StrKey(asString: String)
  val genStrKey: Gen[StrKey] = (Gen.alphaNumChar |@| Gen.alphaNumStr)(_ + _) map StrKey
  implicit val arbGenStrKey: Arbitrary[StrKey] = Arbitrary(genStrKey)

  case class MapEntry(key: String, value: Data)
  implicit val arbMapEntry: Arbitrary[MapEntry] =
    Arbitrary((genStrKey |@| arbitrary[Data])((key, value) => MapEntry(key.asString, value)))

  case class TestCase(obj: ListMap[String, Data])

  val genNonEmptyObj: Gen[ListMap[String, Data]] =
    arbitrary[NonEmptyList[MapEntry]] map (nel => ListMap(nel.toList.map(me => (me.key, me.value)): _*))

  implicit val arbNonEmptyObj: Arbitrary[TestCase] =
    Arbitrary(genNonEmptyObj map TestCase)

  // Generates a (key, obj) where key does not exist in obj
  case class WithoutKeyTestCase(key: String, obj: ListMap[String, Data])
  implicit val withoutKey: Arbitrary[WithoutKeyTestCase] =
    Arbitrary(for {
      obj <- genNonEmptyObj
      keys = obj.keys.toList
      key <- Gen.oneOf(keys) map (_ + "foo")
    } yield (WithoutKeyTestCase(key, obj - key)))

  // Generates a (key, obj) where key exists in obj
  case class WithKeyTestCase(key: String, obj: ListMap[String, Data])
  implicit val withKey: Arbitrary[WithKeyTestCase] =
    Arbitrary(for {
      obj <- genNonEmptyObj
      key <- Gen.oneOf(obj.keys.toList)
    } yield WithKeyTestCase(key, obj))

  xquerySpec(bn => s"Structural Planner (${bn.name})") { evalM =>
    val evalF = evalM.compose[F[XQuery]](toM(_))

    "null_ returns EJson null" >> {
      evalF(SP.null_) must resultIn(Data.Null)
    }

    "arrayAppend" >> {
      "append to non-empty array" >> prop { (x0: Data, y0: Data) =>
        val r = evalF((lit(x0) |@| lit(y0))((x, y) =>
          SP.singletonArray(x) >>= (SP.arrayAppend(_, y))
        ).join)

        r must resultIn(Data._arr(List(x0, y0)))
      }

      "append to empty array" >> prop { x0: Data =>
        val r = evalF((lit(emptyArr) |@| lit(x0))((emp, x) =>
          SP.arrayAppend(emp, x)
        ).join)

        r must resultIn(Data._arr(List(x0)))
      }
    }

    "arrayConcat" >> {
      "two non-empty arrays" >> prop { (x0: Data, y0: Data) =>
        val r = evalF((lit(x0) |@| lit(y0))((x, y) =>
          (SP.singletonArray(x) |@| SP.singletonArray(y))(SP.arrayConcat(_, _)).join
        ).join)

        r must resultIn(Data._arr(List(x0, y0)))
      }

      "empty to non-empty" >> prop { x0: Data =>
        val r = evalF((lit(emptyArr) |@| lit(x0))((a, x) =>
          SP.singletonArray(x) >>= (SP.arrayConcat(a, _))
        ).join)

        r must resultIn(Data._arr(List(x0)))
      }

      "non-empty to empty" >> prop { x0: Data =>
        val r = evalF((lit(x0) |@| lit(emptyArr))((x, a) =>
          SP.singletonArray(x) >>= (SP.arrayConcat(_, a))
        ).join)

        r must resultIn(Data._arr(List(x0)))
      }

      "empty to empty" >> {
        val r = evalF(lit(emptyArr) >>= (a => SP.arrayConcat(a, a)))
        r must resultIn(emptyArr)
      }
    }

    "arrayElementAt" >> {
      def eltAt(xs: NonEmptyList[Data], at: Int): ErrorMessages \/ Data =
        evalF(lit(Data._arr(xs.toList)) >>= (SP.arrayElementAt(_, at.xqy)))

      "at 0 returns first element" >> prop { xs: NonEmptyList[Data] =>
        eltAt(xs, 0) must resultIn(xs.head)
      }

      "at length - 1 returns last element" >> prop { xs: NonEmptyList[Data] =>
        eltAt(xs, xs.length - 1) must resultIn(xs.last)
      }

      "at inner returns inner element" >> prop { (x: Data, y: Data, z: Data) =>
        eltAt(NonEmptyList(x, y, z), 1) must resultIn(y)
      }

      "at < 0 returns nothing" >> prop { xs: NonEmptyList[Data] =>
        eltAt(xs, -1) must resultInNothing
      }

      "at >= length returns nothing" >> prop { xs: NonEmptyList[Data] =>
        eltAt(xs, xs.length) must resultInNothing
      }

      "empty array returns nothing" >> prop { at: Int =>
        evalF(lit(emptyArr) >>= (SP.arrayElementAt(_, at.xqy))) must resultInNothing
      }
    }

    "leftShift" >> todo

    "objectDelete" >> {
      "removes existing key from an object" >> prop { testCase: WithKeyTestCase =>
        val obj = Data._obj(testCase.obj)
        val expectedObj = testCase.obj - testCase.key

        evalF(lit(obj) >>= (SP.objectDelete(_, testCase.key.xs))) must resultIn(Data._obj(expectedObj))
      }

      "identity when key not present in object" >> prop { testCase: WithoutKeyTestCase =>
        val obj = Data._obj(testCase.obj)
        evalF(lit(obj) >>= (SP.objectDelete(_, testCase.key.xs))) must resultIn(obj)
      }

      "identity on empty object" >> prop { key: StrKey =>
        evalF((lit(emptyObj) |@| key.asString.xs.point[F])(SP.objectDelete).join) must resultIn(emptyObj)
      }
    }

    "objectInsert" >> {
      "adds new assoc to non-empty object" >> prop { (testCase: WithoutKeyTestCase, y: Data) =>
        val res = evalF((lit(Data._obj(ListMap(testCase.obj.toList: _*))) |@| lit(y))(
          (SP.objectInsert(_, testCase.key.xs, _))).join)

        res must resultIn(Data._obj(testCase.obj + (testCase.key -> y)))
      }

      "adds new assoc to empty object" >> prop { (key: StrKey, y: Data) =>
        val res = evalF((lit(emptyObj) |@| key.asString.xs.point[F] |@| lit(y))(SP.objectInsert).join)
        res must resultIn(Data._obj(ListMap(key.asString -> y)))
      }
    }

    "objectLookup" >> {
      "returns value for existing key" >> prop { testCase: WithKeyTestCase =>
        val obj = Data._obj(testCase.obj)
        val expected = testCase.obj.get(testCase.key).get

        evalF((lit(obj) |@| testCase.key.xs.point[F])(SP.objectLookup).join) must resultIn(expected)
      }

      "returns nothing for non-existent key in non-empty object" >> prop { testCase: WithoutKeyTestCase =>
        val obj = Data._obj(testCase.obj)
        evalF(lit(obj) >>= (SP.objectLookup(_, testCase.key.xs))) must resultInNothing
      }

      "returns nothing for empty object" >> {
        evalF((lit(emptyObj) |@| "NOT_EXIST".xs.point[F])(SP.objectLookup).join) must resultInNothing
      }
    }

    "objectMerge" >> {
      "left identity with empty object" >> prop { testCase: TestCase =>
        val obj = Data._obj(testCase.obj)
        evalF((lit(emptyObj) |@| lit(obj))(SP.objectMerge).join) must resultIn(obj)
      }

      "right identity with empty object" >> prop { testCase: TestCase =>
        val obj = Data._obj(testCase.obj)
        evalF((lit(obj) |@| lit(emptyObj))(SP.objectMerge).join) must resultIn(obj)
      }

      "union when keys are disjoint" >> prop { testCase: TestCase =>
        val ents1 = testCase.obj
        val ents2 = testCase.obj map { case (k, v) => s"${k}b" -> v }
        evalF((lit(Data._obj(ents1)) |@| lit(Data._obj(ents2)))(SP.objectMerge).join) must resultIn(Data._obj(ents1 ++ ents2))
      }

      "right biased when keys overlap" >> prop { (testCase: TestCase, values: NonEmptyList[Data]) =>
        val left = testCase.obj
        val right = ListMap(testCase.obj.keys.zip(values.toList).toList: _*)

        val o = Data._obj(left ++ right)

        evalF((lit(Data._obj(left)) |@| lit(Data._obj(right)))(SP.objectMerge).join) must resultIn(o)
      }
    }
  }
}
