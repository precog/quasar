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

package quasar.physical.rdbms.fs

import java.time.format.DateTimeFormatter

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.physical.rdbms.model._

package object postgres {

  implicit val codec: DataCodec = DataCodec.Precise
  import DataCodec.PreciseKeys._

  implicit val typeMapper: TypeMapper = TypeMapper(
    {
      case JsonCol   => "jsonb"
      case StringCol => "text"
      case IntCol    => "bigint"
      case DecCol    => "decimal"
      case NullCol   => "int"
      case BoolCol   => "bool"
    },
    _.toLowerCase match {
      case "text" | "varchar" => StringCol
      case "int" | "bigint"   => IntCol
      case "jsonb" | "json"   => JsonCol
      case "decimal" | "numeric" => DecCol
      case "bool" => BoolCol
      // TODO more types
    }
  )

  private def escapeStr(v: String) = v.replaceAllLiterally("'","''")

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def escapeData(d: Data): Data = d match {
    case Data.Obj(es) => Data.Obj(es.map{ case (k,v) => (escapeStr(k), escapeData(v))})
    case Data.Arr(es) => Data.Arr(es.map(escapeData))
    case Data.Str(s) => Data.Str(escapeStr(s))
    case _ => d
  }

  val tsFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

  implicit val dataFormatter: DataFormatter = DataFormatter((n, v, t) =>
    (v, t) match {
      case (Data.Obj(_), _) | (Data.Arr(_), _) =>
        "'" + DataCodec.render(escapeData(v)).getOrElse("{}") + "'"
      case (Data.Int(num), JsonCol) => s"'[$num]'"
      case (Data.Str(txt), JsonCol) => s"""'["${escapeStr(txt)}"]'"""
      case (Data.Dec(num), JsonCol) => s"'[$num]'"
      case (Data.Bool(bool), JsonCol) => s"'[$bool]'"
      case (Data.Int(num), _) => s"$num"
      case (Data.Str(txt), _) => s"'${escapeStr(txt)}'"
      case (Data.Dec(num), _) => s"$num"
      case (Data.Bool(bool), _) => s"$bool"
      case (Data.Id(oid), _) => s"""'{"$IdKey": "$oid"}'"""
      // TODO support datetimes
      case (other, _) => s"""'{"$n": "unsupported ($other)"}'"""
    })
}
