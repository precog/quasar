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

package quasar.physical.rdbms.fs

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.physical.rdbms.model._

package object postgres {
  
  implicit val codec: DataCodec = DataCodec.Precise

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

  implicit val dataFormatter: DataFormatter = DataFormatter((n, v) =>
    v match {
      case Data.Obj(_) | Data.Arr(_) =>
        "'" + DataCodec.render(escapeData(v)).getOrElse("{}") + "'"
      case Data.Int(num) => s"$num"
      case Data.Str(txt) => s"'${escapeStr(txt)}'"
      case Data.Dec(num) => s"$num"
      case Data.Bool(bool) => s"$bool"
      case _             => s"""'{"$n": "unsupported""}'""" // TODO
    })
}