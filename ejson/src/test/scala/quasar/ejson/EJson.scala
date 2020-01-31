/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.ejson

import slamdata.Predef.{Int => SInt, _}
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.specs2.Spec
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota._

import scala.Predef.implicitly
import scala.Predef.$conforms

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import monocle.syntax.fields._
import org.specs2.scalacheck._
import qdata._
import qdata.time.{DateTimeInterval, OffsetDate, TimeGenerators}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties.{equal => eql, _}

class EJsonSpecs extends Spec with EJsonArbitrary {
  import Extension.Optics.meta

  // To keep generated EJson managable
  implicit val params = Parameters(maxSize = 10)

  type J = Fix[EJson]
  type JS = Fix[Json]

  val J = Fixed[J]

  checkAll("Common", order.laws[Common[String]])
  checkAll("Common", traverse.laws[Common])

  checkAll("Obj", order.laws[Obj[String]])
  checkAll("Obj", traverse.laws[Obj])

  checkAll("Extension", traverse.laws[Extension](implicitly, implicitly, Extension.structuralOrder(Order[SInt])))
  checkAll("Extension", order.laws[Extension[SInt]](Extension.structuralOrder(Order[SInt]), implicitly))
  checkAll("Extension", eql.laws[Extension[SInt]](Extension.structuralEqual(Equal[SInt]), implicitly))

  checkAll("EJson", order.laws[J])

  "ordering ignores metadata" >> prop { (x: J, y: J, m: J) =>
    val xMeta = ExtEJson(meta[J](x, m)).embed
    (xMeta ?|? y) ≟ (x ?|? y)
  }

  def addAssoc(ys: List[(J, J)]): J => J = totally {
    case Embed(ExtEJson(Map(xs))) => ExtEJson(Map(xs ::: ys)).embed
  }

  "Type extractor" >> {
    "roundtrip" >> prop { tt: TypeTag =>
      Type.unapply(Type[J](tt).project) ≟ Some(tt)
    }

    "just match type" >> prop { (tt: TypeTag, ys: List[(J, J)]) =>
      Type.unapply(addAssoc(ys)(Type[J](tt)).project) ≟ Some(tt)
    }
  }

  "SizedType extractor" >> {
    "roundtrip" >> prop { (tt: TypeTag, n: BigInt) =>
      SizedType.unapply(SizedType[J](tt, n).project) ≟ Some((tt, n))
    }

    "only match when type and size present" >> prop { (tt: TypeTag, ys: List[(J, J)]) =>
      SizedType.unapply(addAssoc(ys)(Type[J](tt)).project) ≟ None
    }
  }

  "embeds json" >> prop { js: JS =>
    val str = Fixed[J].str

    js.transCata[J](EJson.fromJson(str(_)))
      .transAnaM[Option, JS, Json](EJson.toJson(str.getOption(_))) ≟ js.some
  }

  "qdata" >> {
    import TimeGenerators._

    val rt = new QDataRoundtrip[J]

    val strKey = _1[(J, J), J] composePrism J.str

    val normalize: EJson[J] => EJson[J] = {
      case ExtEJson(Char(c)) => CommonEJson(Str(c.toString))
      case CommonEJson(Dec(n)) if n.isWhole => ExtEJson(Int(n.toBigInt))
      case ExtEJson(Map(js)) => ExtEJson(Map(js.filter(strKey.nonEmpty)))
      case j => j
    }

    "roundtrips representable values" >> prop { j: J =>
      val norm = j.transCata[J](normalize)
      rt.roundtrip(norm) must_=== Some(norm)
    }

    "roundtrips LocalDate" >> prop { t: LocalDate =>
      QDataDecode[J].getLocalDate(QDataEncode[J].makeLocalDate(t)) must_=== t
    }

    "roundtrips LocalDateTime" >> prop { t: LocalDateTime =>
      QDataDecode[J].getLocalDateTime(QDataEncode[J].makeLocalDateTime(t)) must_=== t
    }

    "roundtrips LocalTime" >> prop { t: LocalTime =>
      QDataDecode[J].getLocalTime(QDataEncode[J].makeLocalTime(t)) must_=== t
    }

    "roundtrips OffsetDate" >> prop { t: OffsetDate =>
      QDataDecode[J].getOffsetDate(QDataEncode[J].makeOffsetDate(t)) must_=== t
    }

    "roundtrips OffsetDateTime" >> prop { t: OffsetDateTime =>
      QDataDecode[J].getOffsetDateTime(QDataEncode[J].makeOffsetDateTime(t)) must_=== t
    }

    "roundtrips OffsetTime" >> prop { t: OffsetTime =>
      QDataDecode[J].getOffsetTime(QDataEncode[J].makeOffsetTime(t)) must_=== t
    }

    "roundtrips DateTimeInterval" >> prop { t: DateTimeInterval =>
      QDataDecode[J].getInterval(QDataEncode[J].makeInterval(t)) must_=== t
    }
  }
}
