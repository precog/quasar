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

package quasar.api

import slamdata.Predef._
import quasar.contrib.pathy.{RFile, sandboxCurrent}

import argonaut._, Argonaut._
import org.http4s.headers.`Content-Type`
import pathy.Path._
import scalaz._, Scalaz._

final case class FileMetadata(contentType: `Content-Type`)

object FileMetadata {

  implicit val contentTypeCodecJson: CodecJson[`Content-Type`] =
    CodecJson.derived(
      EncodeJson.of[String].contramap[`Content-Type`](_.value),
      DecodeJson.of[String].flatMap(s => DecodeJson(cur =>
        DecodeResult(`Content-Type`.parse(s).leftMap(_.message -> cur.history).toEither))))

  implicit val codecJson: CodecJson[FileMetadata] =
    CodecJson(
      md => Json("Content-Type" := md.contentType),
      json => json.fields match {
        case Some("Content-Type" :: Nil) =>
          val ctCur = json --\ "Content-Type"
          ctCur.as[`Content-Type`].map(FileMetadata(_))
        case _ =>
          DecodeResult.fail(s"invalid metadata: ${json.focus}", json.history)
      })
}

final case class ArchiveMetadata(files: Map[RFile, FileMetadata])

object ArchiveMetadata {
  val HiddenFile: RFile = currentDir </> file(".quasar-metadata.json")

  implicit val codecJson: CodecJson[ArchiveMetadata] =
    CodecJson.derived(
      EncodeJson.of[Map[String, FileMetadata]].contramap(
        _.files map { case (k, v) => posixCodec.printPath(k) -> v }),
      DecodeJson.of[Map[String, FileMetadata]].flatMap { (m: Map[String, FileMetadata]) =>
        val m1: String \/ Map[RFile, FileMetadata] = (m.toList traverse { case (k, v) =>
          posixCodec.parseRelFile(k).flatMap(sandboxCurrent)
            .cata(p => (p -> v).right, s"expected relative file path; found: $k".left) }).map(_.toMap)
        DecodeJson(cur => DecodeResult(m1.bimap(_ -> cur.history, ArchiveMetadata(_)).toEither))
      })
}
