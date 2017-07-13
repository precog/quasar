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
import quasar._
import quasar.fp._
import quasar.javascript._
import quasar.jscore

import scalaz._, Scalaz._

class BsonSpecs extends quasar.Qspec {
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
    }.setGen(simpleGen)

    "be 'semi' isomorphic for all types" >> prop { (bson: Bson) =>
      val wrapped = Doc(ListMap("value" -> bson)).repr

      // (fromRepr >=> repr >=> fromRepr) == fromRepr
      fromRepr(fromRepr(wrapped).repr) must_== fromRepr(wrapped)
    }
  }

  "toJs" should {
    import BsonGen._

    "correspond to Data.toJs where toData is defined" >> prop { (bson: Bson) =>
      val data = BsonCodec.toData(bson)
      (data != Data.NA && !data.isInstanceOf[Data.Set]) ==> {
        data match {
          case Data.Int(x) =>
            // NB: encoding int as Data loses size info
            (bson.toJs must_== jscore.Call(jscore.ident("NumberInt"), List(jscore.Literal(Js.Str(x.shows)))).toJs) or
            (bson.toJs must_== jscore.Call(jscore.ident("NumberLong"), List(jscore.Literal(Js.Str(x.shows)))).toJs)
          case _ =>
            BsonCodec.fromData(data).fold(
              _ => scala.sys.error("failed to convert data to BSON: " + data.shows),
              _.toJs.some must_== data.toJs.map(_.toJs))
        }
      }
    }.setGen(simpleGen)
  }
}

object BsonGen {
  import org.scalacheck._
  import Gen._
  import Arbitrary._

  import Bson._

  implicit val arbBson: Arbitrary[Bson] = Arbitrary(Gen.oneOf(
    simpleGen,
    resize(5, objGen),
    resize(5, arrGen)))

  val simpleGen = oneOf(
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

  val objGen = for {
    pairs <- listOf(for {
      n <- resize(5, alphaStr)
      v <- simpleGen
    } yield n -> v)
  } yield Doc(pairs.toListMap)

  val arrGen = listOf(simpleGen).map(Arr.apply)
}
