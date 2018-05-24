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

import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

class BsonCodec_1_0_Spec extends BsonCodecSpec(BsonVersion.`1.0`)
class BsonCodec_1_1_Spec extends BsonCodecSpec(BsonVersion.`1.1`)

abstract class BsonCodecSpec(v: BsonVersion) extends quasar.Qspec {
  import BsonCodec._

  import DataGenerators._

  implicit val ShowData = new Show[Data] {
    override def show(v: Data) = Cord(v.toString)
  }
  implicit val ShowBson = new Show[Bson] {
    override def show(v: Bson) = Cord(v.toString)
  }

  "fromData" should {
    "fail with bad Id" in {
      fromData(v, Data.Id("invalid")) must beLeftDisjunction
    }

    "of double does convert to Bson.Dec not Bson.Dec128" >> prop { (d: Double) =>
      fromData(v, Data.Dec(d)) must_== \/-(Bson.Dec(d))
    }

    val resType =
      if (v lt BsonVersion.`1.1`) "Bson.Dec" else "Bson.Dec128"
    s"of bigdecimal that do not fit a double convert to $resType" in {
      val b = BigDecimal("123456E789")
      val r = v match {
        case BsonVersion.`1.0` => Bson.Dec(Double.PositiveInfinity)
        case BsonVersion.`1.1` => Bson.Dec128(b)
      }
      fromData(v, Data.Dec(b)) must_== \/-(r)
    }

    "be isomorphic for preserved values" >> prop { (data: Data) =>
      // (fromData >=> toData) == identity, except for values that are known not to be preserved

      import Data._

      def preserved(d: Data): Boolean = d match {
        case Int(x)            => x.isValidLong
        case Interval(i)       => false
        case OffsetDateTime(_) => false
        case OffsetDate(_)     => false
        case OffsetTime(_)     => false
        case LocalDateTime(_)  => false
        case LocalDate(_)      => false
        case LocalTime(_)      => false
        case Arr(value)        => value.forall(preserved)
        case Obj(value)        => value.values.forall(preserved)
        case _                 => true
      }

      preserved(data) ==> {
        fromData(v, data).map(toData) must beRightDisjunction(data)
      }
    }

    implicit val arbitraryBson = BsonGen.arbBson(v)

    "be 'semi'-isomorphic for all Bson values" >> prop { (bson: Bson) =>
      // (toData >=> fromData >=> toData) == toData

      val data = toData(bson)
      fromData(v, data).map(toData _) must beRightDisjunction(data)
    }
  }

  "toData" should {
    "convert MinKey to NA" in {
      toData(Bson.MinKey) must_== Data.NA
    }

    "be 'semi'-isomorphic for all Data values" >> prop { (data: Data) =>
      // (fromData >=> toData >=> fromData) == fromData
      // Which is to say, every Bson value that results from conversion
      // can be converted to Data and back to Bson, recovering the same
      // Bson value.
      val r = fromData(v, data)
      r.isRight ==> {
        r.flatMap(bson => fromData(v, toData(bson))) must_== r
      }
    }
  }

  "round trip to repr (all Data types)" >> prop { (data: Data) =>
      val r = fromData(v, data)
      r.isRight ==> {
        val wrapped = r.map(bson => Bson.Doc(ListMap("value" -> bson)))
        wrapped.map(w => Bson.fromRepr(w.repr)) must_== wrapped
      }
  }
}
