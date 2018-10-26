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

package quasar.yggdrasil
package table

import quasar.blueeyes.json.JValue
import quasar.common.data.{Data, DataGenerators}
import quasar.frontend.data.DataCodec
import quasar.precog.JsonTestSupport
import quasar.precog.common.{CLong, CNum, RValue}

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import tectonic.json.Parser

import scala.collection.immutable.ListMap
import scala.math.BigDecimal

object SlicePlateSpec extends Specification with ScalaCheck with DataGenerators {
  import JsonTestSupport._

  "slice plate parsing through tectonic" should {
    "round trip readable" in prop { values: List[JValue] =>
      val input = values.mkString("\n")
      val plate = new SlicePlate(false)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toJsonElements)
          results mustEqual values
      }
    }

    "round trip precise" in prop { values0: List[Data] =>
      val values = values0.map(sortFields(_)).flatMap(stripNA(_))

      val input = values.flatMap(DataCodec.Precise.encode(_)).map(_.nospaces).mkString("\n")
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues).map(RValue.toData(_)).map(sortFields(_))
          results mustEqual values
      }
    }

    "parse Long.MaxValue + 1" in {
      val input = "9223372036854775808"
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues)
          results mustEqual List(CNum(BigDecimal("9223372036854775808")))
      }
    }

    "parse Long.MinValue" in {
      val input = Long.MinValue.toString
      val plate = new SlicePlate(true)
      val parser = Parser(plate, Parser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toRValues)
          results mustEqual List(CLong(Long.MinValue))
      }
    }
  }

  private[this] def sortFields(data: Data): Data = data match {
    case Data.Obj(fields) =>
      val fields2 = ListMap(fields.mapValues(sortFields(_)).toList.sortBy(_._1): _*)
      Data.Obj(fields2)

    case Data.Arr(values) =>
      Data.Arr(values.map(sortFields(_)))

    case other => other
  }

  private[this] def stripNA(data: Data): Option[Data] = data match {
    case Data.Obj(fields) =>
      val fields2 = fields flatMap {
        case (key, value) => stripNA(value).map(key -> _)
      }

      Some(Data.Obj(fields2))

    case Data.Arr(values) =>
      Some(Data.Arr(values.flatMap(stripNA(_))))

    case Data.NA => None

    case other => Some(other)
  }
}
