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

package quasar.common.data

import slamdata.Predef._

import quasar.Qspec

import qdata._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.option._

object QDataRValueSpec extends Qspec with RValueGenerators {

  val qdataRoundtrip = new QDataRoundtrip[RValue]

  def adjustExpected(value: RValue): Option[RValue] = value match {
    case RObject(obj) if obj.isEmpty =>
      Some(CEmptyObject)

    case RObject(obj) =>
      obj.traverse(v => adjustExpected(v)).map(o => RObject(o))

    case RArray(arr) if arr.isEmpty =>
      Some(CEmptyArray)

    case RArray(arr) =>
      arr.traverse(v => adjustExpected(v)).map(RArray(_))

    case RMeta(value, meta) => for {
      newVal <- adjustExpected(value)
      newMeta <- adjustExpected(meta)
      newMetaObj <- RValue.rObject.getOption(newMeta)
    } yield RValue.rMeta(newVal, RObject(newMetaObj))


    case CUndefined => None // not supported by qdata
    case CArray(_, _) => None // not supported by quasar (or qdata)

    case d => Some(d)
  }

  "roundtrip arbitrary RValue" >> prop { value: RValue =>
    qdataRoundtrip.roundtrip(value) must_=== adjustExpected(value)
  }.set(minTestsOk = 1000)
}
