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

package quasar.precog.common
package ingest

import quasar.blueeyes._, json._, serialization._
import DefaultSerialization._
import Versioned._
import Extractor._
import java.util.Base64
import scalaz._, Scalaz._

sealed trait ContentEncoding {
  def id: String
  def encode(raw: Array[Byte]): String
  def decode(compressed: String): Array[Byte]
}

object ContentEncoding {
  val decomposerV1: Decomposer[ContentEncoding] = new Decomposer[ContentEncoding] {
    def decompose(ce: ContentEncoding) = JObject("encoding" -> ce.id.serialize)
  }

  val extractorV1: Extractor[ContentEncoding] = new Extractor[ContentEncoding] {
    override def validated(obj: JValue): Validation[Error, ContentEncoding] = {
      obj.validated[String]("encoding").flatMap {
        case "uncompressed" => Success(RawUTF8Encoding)
        case "base64"       => Success(Base64Encoding)
        case invalid        => Failure(Invalid("Unknown encoding " + invalid))
      }
    }
  }

  implicit val decomposer = decomposerV1.versioned(Some("1.0".v))
  implicit val extractor  = extractorV1.versioned(Some("1.0".v))
}

object RawUTF8Encoding extends ContentEncoding {
  val id = "uncompressed"
  def encode(raw: Array[Byte])   = new String(raw, "UTF-8")
  def decode(compressed: String) = compressed.getBytes("UTF-8")
}

object Base64Encoding extends ContentEncoding {
  val id = "base64"
  def encode(raw: Array[Byte])   = new String(Base64.getEncoder.encode(raw), "UTF-8")
  def decode(compressed: String) = Base64.getDecoder.decode(compressed)
}

case class FileContent(data: Array[Byte], mimeType: MimeType, encoding: ContentEncoding)

object FileContent {
  import MimeTypes._
  val XQuirrelData    = MimeType("application", "x-quirrel-data")
  val XQuirrelScript  = MimeType("text", "x-quirrel-script")
  val XJsonStream     = MimeType("application", "x-json-stream")
  val ApplicationJson = application / json
  val TextCSV         = text / csv
  val TextPlain       = text / plain
  val AnyMimeType     = MimeType("*", "*")
  val OctetStream     = application / `octet-stream`

  val stringTypes = Set(XQuirrelScript, ApplicationJson, TextCSV, TextPlain)

  def apply(data: Array[Byte], mimeType: MimeType): FileContent =
    if (stringTypes.contains(mimeType)) {
      FileContent(data, mimeType, RawUTF8Encoding)
    } else {
      FileContent(data, mimeType, Base64Encoding)
    }

  val DecomposerV0: Decomposer[FileContent] = new Decomposer[FileContent] {
    def decompose(v: FileContent) = JObject(
      "data"     -> JString(v.encoding.encode(v.data)),
      "mimeType" -> v.mimeType.jv,
      "encoding" -> v.encoding.jv
    )
  }

  val ExtractorV0: Extractor[FileContent] = new Extractor[FileContent] {
    def validated(jv: JValue) = {
      jv match {
        case JObject(fields) =>
          (fields.get("encoding").toSuccess(Invalid("File data object missing encoding field.")).flatMap(_.validated[ContentEncoding]) |@|
                fields.get("mimeType").toSuccess(Invalid("File data object missing MIME type.")).flatMap(_.validated[MimeType]) |@|
                fields.get("data").toSuccess(Invalid("File data object missing data field.")).flatMap(_.validated[String])) {
            (encoding, mimeType, contentString) =>
              FileContent(encoding.decode(contentString), mimeType, encoding)
          }

        case _ =>
          Failure(Invalid("File contents " + jv.renderCompact + " was not properly encoded as a JSON object."))
      }
    }
  }

  implicit val decomposer = DecomposerV0
  implicit val extractor  = ExtractorV0
}
