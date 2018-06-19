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

package quasar.niflheim

import quasar.precog.common._

import quasar.precog.BitSet
import quasar.precog.util.BitSetUtil.Implicits._

import java.io.IOException
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.nio.ByteBuffer

import scala.{ specialized => spec }

import scalaz.{Validation, Success, Failure}
import scalaz.syntax.monad._

object V1SegmentFormat extends SegmentFormat {
  object reader extends SegmentReader {
    private def wrapException[A](f: => A): Validation[IOException, A] = try {
      Success(f)
    } catch { case e: Exception =>
      Failure(new IOException(e))
    }

    def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId] = for {
      buffer <- readChunk(channel)
      blockId <- wrapException(buffer.getLong())
      cpath <- wrapException(CPath(Codec.Utf8Codec.read(buffer)))
      ctype <- CTypeFlags.readCType(buffer)
    } yield SegmentId(blockId, cpath, ctype)

    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] = {
      def readArray[A](ctype: CValueType[A]): Validation[IOException, (BitSet, Array[A])] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val defined = Codec.BitSetCodec.read(buffer)
        val codec = getCodecFor(ctype)
        val values = ctype.classTag.newArray(length)
        defined.foreach { row =>
          values(row) = codec.read(buffer)
        }
        (defined, values)
      }

      def readNull(ctype: CNullType): Validation[IOException, (BitSet, Int)] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val defined = Codec.BitSetCodec.read(buffer)
        (defined, length)
      }

      def readBoolean(): Validation[IOException, (BitSet, Int, BitSet)] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val defined = Codec.BitSetCodec.read(buffer)
        val values = Codec.BitSetCodec.read(buffer)
        (defined, length, values)
      }

      for {
        header <- readSegmentId(channel)
        segment <- header match {
          case SegmentId(blockid, cpath, CBoolean) =>
            readBoolean() map { case (defined, length, values) =>
              BooleanSegment(blockid, cpath, defined, values, length)
            }

          case SegmentId(blockid, cpath, ctype: CValueType[a]) =>
            readArray(ctype) map { case (defined, values) =>
              ArraySegment(blockid, cpath, ctype, defined, values)
            }

          case SegmentId(blockid, cpath, ctype: CNullType) =>
            readNull(ctype) map { case (defined, length) =>
              NullSegment(blockid, cpath, ctype, defined, length)
            }
        }
      } yield segment
    }
  }

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment): Validation[IOException, Unit] = {
      for {
        _ <- writeSegmentId(channel, segment)
        _ <- segment match {
          case seg: ArraySegment[a] =>
            writeArraySegment(channel, seg, getCodecFor(seg.ctype))
          case seg: BooleanSegment =>
            writeBooleanSegment(channel, seg)
          case seg: NullSegment =>
            writeNullSegment(channel, seg)
        }
      } yield ()
    }

    private def writeSegmentId(channel: WritableByteChannel, segment: Segment): Validation[IOException, Unit] = {
      val tpeFlag = CTypeFlags.getFlagFor(segment.ctype)
      val strPath = segment.cpath.toString
      val maxSize = Codec.Utf8Codec.maxSize(strPath) + tpeFlag.length + 8

      writeChunk(channel, maxSize) { buffer =>
        buffer.putLong(segment.blockid)
        Codec.Utf8Codec.writeUnsafe(strPath, buffer)
        buffer.put(tpeFlag)
      }
    }

    private def writeArraySegment[@spec(Boolean,Long,Double) A](channel: WritableByteChannel,
        segment: ArraySegment[A], codec: Codec[A]): Validation[IOException, Unit] = {
      var maxSize = Codec.BitSetCodec.maxSize(segment.defined) + 4
      segment.defined.foreach { row =>
        maxSize += codec.maxSize(segment.values(row))
      }

      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.values.length)
        Codec.BitSetCodec.writeUnsafe(segment.defined, buffer)
        segment.defined foreach { row =>
          codec.writeUnsafe(segment.values(row), buffer)
        }
      }
    }

    private def writeBooleanSegment(channel: WritableByteChannel, segment: BooleanSegment) = {
      val maxSize = Codec.BitSetCodec.maxSize(segment.defined) + Codec.BitSetCodec.maxSize(segment.values) + 4
      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.length)
        Codec.BitSetCodec.writeUnsafe(segment.defined, buffer)
        Codec.BitSetCodec.writeUnsafe(segment.values, buffer)
      }
    }

    private def writeNullSegment(channel: WritableByteChannel, segment: NullSegment) = {
      val maxSize = Codec.BitSetCodec.maxSize(segment.defined) + 4
      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.length)
        Codec.BitSetCodec.writeUnsafe(segment.defined, buffer)
      }
    }
  }

  private def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  def writeChunk[A](channel: WritableByteChannel, maxSize: Int)(f: ByteBuffer => A): Validation[IOException, A] = {
    val buffer = allocate(maxSize + 4)
    buffer.position(4)

    val result = f(buffer)

    buffer.flip()
    buffer.putInt(0, buffer.limit() - 4)

    try {
      while (buffer.remaining() > 0) {
        channel.write(buffer)
      }
      Success(result)
    } catch {
      case ex: IOException =>
        Failure(ex)
    }
  }

  def readChunk(channel: ReadableByteChannel): Validation[IOException, ByteBuffer] = {
    try {
      val buffer0 = allocate(4)
      while (buffer0.remaining() > 0) {
        channel.read(buffer0)
      }
      buffer0.flip()
      val length = buffer0.getInt()

      val buffer = allocate(length)
      while (buffer.remaining() > 0) {
        channel.read(buffer)
      }
      buffer.flip()
      Success(buffer)
    } catch {
      case ioe: IOException =>
        Failure(ioe)
    }
  }

  private def getCodecFor[A](ctype: CValueType[A]): Codec[A] = ctype match {
    case CBoolean => Codec.BooleanCodec
    case CString => Codec.Utf8Codec
    case CLong => Codec.PackedLongCodec
    case CDouble => Codec.DoubleCodec
    case CNum => Codec.BigDecimalCodec
    case COffsetDateTime => Codec.OffsetDateTimeCodec
    case COffsetTime => Codec.OffsetTimeCodec
    case COffsetDate => Codec.OffsetDateCodec
    case CLocalDateTime => Codec.LocalDateTimeCodec
    case CLocalTime => Codec.LocalTimeCodec
    case CLocalDate => Codec.LocalDateCodec
    case CInterval => Codec.IntervalCodec
    case CArrayType(elemType) =>
      Codec.ArrayCodec(getCodecFor(elemType))(elemType.classTag)
  }
}

