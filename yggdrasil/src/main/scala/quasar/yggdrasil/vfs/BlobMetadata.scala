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

package quasar.yggdrasil
package vfs

import quasar.precog.common.security.Authorities

import java.time.LocalDateTime

import quasar.blueeyes._
import quasar.blueeyes.json._
import quasar.blueeyes.json.serialization._

import shapeless.HList

import scalaz.syntax.std.option._

case class BlobMetadata(mimeType: MimeType, size: Long, created: LocalDateTime, authorities: Authorities)

object BlobMetadata {
  import DefaultDecomposers._
  import DefaultExtractors._
  import Versioned._

  implicit val mimeTypeDecomposer: Decomposer[MimeType] = new Decomposer[MimeType] {
    def decompose(u: MimeType) = JString(u.toString)
  }

  implicit val mimeTypeExtractor: Extractor[MimeType] = new Extractor[MimeType] {
    def validated(jv: JValue) = StringExtractor.validated(jv).flatMap { ms =>
      MimeTypes.parseMimeTypes(ms).headOption.toSuccess(Extractor.Error.invalid("Could not extract mime type from '%s'".format(ms)))
    }
  }

  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil

  implicit val decomposer = decomposerV[BlobMetadata](schema, Some("1.0".v))

  implicit val extractor = extractorV[BlobMetadata](schema, Some("1.0".v))
}
