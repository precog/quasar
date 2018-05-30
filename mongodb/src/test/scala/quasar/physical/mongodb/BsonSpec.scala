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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.fp._
import quasar.javascript._
import quasar.jscore

import scalaz._, Scalaz._

class BsonSpec_1_0_Spec extends BsonSpec(BsonVersion.`1.0`)
class BsonSpec_1_1_Spec extends BsonSpec(BsonVersion.`1.1`)

abstract class BsonSpec(v: BsonVersion) extends quasar.Qspec {
  import Bson._

  "fromRepr" should {
    "handle partially invalid object" in {
      val native = Doc(ListMap(
        "a" -> Int32(0),
        "b" -> JavaScript(Js.Null))).repr

      fromRepr(native) must_==
        Doc(ListMap(
          "a" -> Int32(0),
          "b" -> Undefined))
    }

    "preserve NA" in {
      val b = Doc(ListMap("a" -> Undefined))

      fromRepr(b.repr) must_== b
    }

    import BsonGen._
    implicit val arbitraryBson: org.scalacheck.Arbitrary[Bson] = BsonGen.arbBson(v)

    "be (fully) isomorphic for representable types" >> prop { (bson: Bson) =>
      val representable = bson match {
        case JavaScript(_)         => false
        case JavaScriptScope(_, _) => false
        case Undefined             => false
        case _ => true
      }

      val wrapped = Doc(ListMap("value" -> bson))

      if (representable)
        fromRepr(wrapped.repr) must_== wrapped
      else
        fromRepr(wrapped.repr) must_== Doc(ListMap("value" -> Undefined))
    }.setGen(simpleGen(v))

    "be 'semi' isomorphic for all types" >> prop { (bson: Bson) =>
      val wrapped = Doc(ListMap("value" -> bson)).repr

      // (fromRepr >=> repr >=> fromRepr) == fromRepr
      fromRepr(fromRepr(wrapped).repr) must_== fromRepr(wrapped)
    }
  }

  "toJs" should {
    import BsonGen._
    implicit val arbitraryBson: org.scalacheck.Arbitrary[Bson] = BsonGen.arbBson(v)

    "correspond to Data.toJs where toData is defined" >> prop { (bson: Bson) =>
      val data = BsonCodec.toData(bson)
      (data != Data.NA) ==> {
        data match {
          case Data.Int(x) =>
            // NB: encoding int as Data loses size info
            (bson.toJs must_== jscore.Call(jscore.ident("NumberInt"), List(jscore.Literal(Js.Str(x.shows)))).toJs) or
            (bson.toJs must_== jscore.Call(jscore.ident("NumberLong"), List(jscore.Literal(Js.Str(x.shows)))).toJs)
          case Data.Dec(x) if v === BsonVersion.`1.1` =>
            (bson.toJs must_== jscore.Call(jscore.ident("NumberDecimal"), List(jscore.Literal(Js.Str(x.shows)))).toJs) or
            (bson.toJs.some must_== data.toJs.map(_.toJs))
          case _ =>
            BsonCodec.fromData(v, data).fold(
              _ => scala.sys.error("failed to convert data to BSON: " + data.shows),
              _.toJs.some must_== data.toJs.map(_.toJs))
        }
      }
    }.setGen(simpleGen(v))
  }
}

object BsonGen {
  import org.scalacheck._
  import Gen._
  import Arbitrary._

  import Bson._

  implicit def arbBson(v: BsonVersion): Arbitrary[Bson] = Arbitrary(Gen.oneOf(
    simpleGen(v),
    resize(5, objGen(v)),
    resize(5, arrGen(v))))

  val simpleGen_1_0: Gen[Bson] = oneOf(
    const(Null),
    const(Bool(true)),
    const(Bool(false)),
    resize(20, arbitrary[String]).map(Text.apply),
    arbitrary[Int].map(Int32.apply),
    arbitrary[Long].map(Int64.apply),
    arbitrary[Double].map(Dec.apply),
    listOf(arbitrary[Byte]).map(bytes => Binary.fromArray(bytes.toArray)),
    listOfN(12, arbitrary[Byte]).map(bytes => ObjectId.fromArray(bytes.toArray)),
    const(Date(0)),
    const(Timestamp(0, 0)),
    const(Regex("a.*", "")),
    const(JavaScript(Js.Null)),
    const(JavaScriptScope(Js.Null, ListMap.empty)),
    resize(5, arbitrary[String]).map(Symbol.apply),
    const(MinKey),
    const(MaxKey),
    const(Undefined))

  def simpleGen(v: BsonVersion): Gen[Bson] = v match {
    case BsonVersion.`1.0` => simpleGen_1_0
    case BsonVersion.`1.1` => oneOf(
      simpleGen_1_0,
      arbitrary[BigDecimal].filter(_.mc != java.math.MathContext.UNLIMITED).map(Dec128.apply)
    )
  }

  def objGen(v: BsonVersion) = for {
    pairs <- listOf(for {
      n <- resize(5, alphaStr)
      v <- simpleGen(v)
    } yield n -> v)
  } yield Doc(pairs.toListMap)

  def arrGen(v: BsonVersion) = listOf(simpleGen(v)).map(Arr.apply)
}
