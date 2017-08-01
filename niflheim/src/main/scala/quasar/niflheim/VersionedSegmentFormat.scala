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

package quasar.niflheim

import java.nio.channels.{ReadableByteChannel, WritableByteChannel}

import scalaz.{Validation, Failure}
import scalaz.syntax.monad._

import java.io.IOException

/**
 * A `VersionedSegmentFormat` wraps formats and is used to deal with multiple
 * versions for `SegmentFormat`s. The version of a segment format is always
 * written first, followed by the actual segment. The format with the highest
 * version number is always used for writing. For reads, the version is read
 * first and the format corresponding to this version is used to read the rest
 * of the segment. If no format exists for that version, then we return an
 * error.
 */
case class VersionedSegmentFormat(formats: Map[Int, SegmentFormat]) extends SegmentFormat with Versioning {
  val magic: Short = 0x0536.toShort
  val (version, format) = {
    val (ver, format) = formats.maxBy(_._1)
    (ver.toShort, format)
  }

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment) = {
      for {
        _ <- writeVersion(channel)
        _ <- format.writer.writeSegment(channel, segment)
      } yield ()
    }
  }

  object reader extends SegmentReader {
    def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId] = {
      readVersion(channel) flatMap { version =>
        formats get version map { format =>
          format.reader.readSegmentId(channel)
        } getOrElse {
          Failure(new IOException(
            "Invalid version found. Expected one of %s, found %d." format (formats.keys mkString ",", version)))
        }
      }
    }

    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] = {
      readVersion(channel) flatMap { version =>
        formats get version map { format =>
          format.reader.readSegment(channel)
        } getOrElse {
          Failure(new IOException(
            "Invalid version found. Expected one of %s, found %d." format (formats.keys mkString ",", version)))
        }
      }
    }
  }
}
