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

import quasar.precog._
import quasar.blueeyes._
import quasar.precog.util.{ BitSetUtil, ByteBufferMonad, ByteBufferPool }

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{ Charset, CharsetEncoder, CoderResult }
import java.math.{ BigDecimal => BigDec }
import java.time.LocalDateTime

import scalaz._

/**
  * Codecs allow a writer to deal with the case where we have a buffer overflow
  * when attempting to write all data to the buffer. This lets the writer return
  * some state indicating more data needs to be written. This state is then
  * given to a `writeMore` method so it can finish the writing. It may take
  * several calls to `writeMore` before it all is finally written.
  */
trait Codec[@spec(Boolean, Long, Double) A] { self =>
  type S

  /** Returns the exact encoded size of `a`. */
  def encodedSize(a: A): Int

  /** Returns an upper bound on the size of `a`. */
  def maxSize(a: A): Int = encodedSize(a)

  /**
    * Returns a lower bound on the space required in a buffer so that `a` can
    * be written.
    */
  def minSize(a: A): Int = 8

  /**
    * Initiate a write. If a is successfully written (in its entirety) to
    * `buffer`, then `None` is returned. Otherwise, `Some(s)` is returned,
    * indicating that some more data (ie. `s`) still needs to be written using
    * `writeMore(s, _)`. This last condition will normally happen when we run
    * out of space in the buffer, so `a` was only partially written.
    */
  def writeInit(a: A, buffer: ByteBuffer): Option[S]

  /**
    * Writes the remaining data from a call to `writeInit` to `buffer`. If this
    * is written completely and successfully, `None` is returned. Otherwise,
    * `Some(s2)` is returned, which contains more data to be written with
    * `writeMore`.
    */
  def writeMore(s: S, buffer: ByteBuffer): Option[S]

  /**
    * Writes `a` using a `ByteBufferMonad`. This is much slower than just using
    * writeInit/writeMore.
    */
  def write[M[_]](a: A)(implicit M: ByteBufferMonad[M]): M[Unit] = {
    import scalaz.syntax.monad._

    val min = minSize(a)

    def loop(s: S): M[Unit] =
      for {
        buf <- M.getBuffer(min)
        _ <- writeMore(s, buf) map (loop(_)) getOrElse ().point[M]
      } yield ()

    for {
      buf <- M.getBuffer(min)
      _ <- writeInit(a, buf) map (loop(_)) getOrElse ().point[M]
    } yield ()
  }

  /**
    * Writes `a` entirely to a series of `ByteBuffer`s returned by `acquire`.
    * The returned set of `ByteBuffer`s is in reverse order, so that calls
    * to `writeAll` can be chained by passing in the previous result to `used`.
    */
  def writeAll(a: A)(acquire: () => ByteBuffer, used: List[ByteBuffer] = Nil): List[ByteBuffer] = {
    @inline
    @tailrec def loop(s: Option[S], buffers: List[ByteBuffer]): List[ByteBuffer] = s match {
      case None => buffers
      case Some(s) =>
        val buf = acquire()
        loop(writeMore(s, buf), buf :: buffers)
    }

    used match {
      case buffers @ (buf :: _) if buf.remaining() >= minSize(a) =>
        loop(writeInit(a, buf), buffers)

      case buffers =>
        val buf = acquire()
        loop(writeInit(a, buf), buf :: buffers)
    }
  }

  /**
    * Performs an *unsafe* write of `a` to `buffer`. This does not perform any
    * safety checks and assumes `buffer` has at least `encodedSize(a)` bytes
    * remaining. The behaviour if this is not the case is undefined.
    */
  def writeUnsafe(a: A, buffer: ByteBuffer): Unit

  /**
    * Reads a value of type `A` from `buffer`. This should generally follow the
    * contract that `writeUnsafe(a, buffer); read(buffer) == a`.
    */
  def read(buffer: ByteBuffer): A

  /**
    * This is similar to `read(buffer)`, except that a value does not actually
    * need to be returned.
    */
  def skip(buffer: ByteBuffer): Unit = read(buffer)

  def as[B](to: B => A, from: A => B): Codec[B] = new Codec[B] {
    type S = self.S

    def encodedSize(b: B)      = self.encodedSize(to(b))
    override def maxSize(b: B) = self.maxSize(to(b))
    override def minSize(b: B) = self.minSize(to(b))

    def writeUnsafe(b: B, buf: ByteBuffer) = self.writeUnsafe(to(b), buf)
    def writeInit(b: B, buf: ByteBuffer)   = self.writeInit(to(b), buf)
    def writeMore(s: S, buf: ByteBuffer)   = self.writeMore(s, buf)

    def read(src: ByteBuffer): B = from(self.read(src))
  }
}

object Codec {

  @inline final def apply[A](implicit codec: Codec[A]): Codec[A] = codec

  private val byteBufferPool = new ByteBufferPool()

  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]) = new IndexedSeqCodec(elemCodec)

  implicit def arrayCodec[@spec(Boolean, Long, Double) A: Codec: CTag]: Codec[Array[A]] = ArrayCodec(Codec[A])

  /**
    * A utility method for getting the encoded version of `a` as an array of
    * bytes.
    */
  def writeToArray[A](a: A)(implicit codec: Codec[A]): Array[Byte] = {
    import ByteBufferPool._

    byteBufferPool.run(for {
      _ <- codec.write[ByteBufferPoolS](a)
      bytes <- flipBytes
      _ <- release
    } yield bytes)
  }

  private final val FALSE_VALUE = 0.toByte
  private final val TRUE_VALUE  = 1.toByte

  @tailrec
  def writePackedInt(n: Int, buf: ByteBuffer): Unit =
    if ((n & ~0x7F) != 0) {
      buf.put((n & 0x7F | 0x80).toByte)
      writePackedInt(n >> 7, buf)
    } else {
      buf.put((n & 0x7F).toByte)
    }

  def readPackedInt(buf: ByteBuffer): Int = {
    @tailrec def loop(n: Int, offset: Int): Int = {
      val b = buf.get()
      if ((b & 0x80) != 0) {
        loop(n | ((b & 0x7F) << offset), offset + 7)
      } else {
        n | ((b & 0x7F) << offset)
      }
    }
    loop(0, 0)
  }

  @tailrec
  def sizePackedInt(n: Int, size: Int = 1): Int =
    if ((n & ~0x7F) != 0) {
      sizePackedInt(n >>> 7, size + 1)
    } else size

  case class CompositeCodec[A, B, C](codecA: Codec[A], codecB: Codec[B], from: C => (A, B), to: (A, B) => C) extends Codec[C] {
    type S = Either[(codecA.S, B), codecB.S]

    def encodedSize(c: C) = {
      val (a, b) = from(c)
      codecA.encodedSize(a) + codecB.encodedSize(b)
    }

    override def minSize(c: C) = codecA.minSize(from(c)._1) max codecB.minSize(from(c)._2)
    override def maxSize(c: C) = {
      val (a, b) = from(c)
      codecA.maxSize(a) + codecB.maxSize(b)
    }

    def writeUnsafe(c: C, buf: ByteBuffer) {
      val (a, b) = from(c)
      codecA.writeUnsafe(a, buf)
      codecB.writeUnsafe(b, buf)
    }

    def writeInit(c: C, buf: ByteBuffer): Option[S] = {
      val (a, b) = from(c)
      (codecA.writeInit(a, buf) map (s => Left((s, b)))) orElse (codecB.writeInit(b, buf) map (Right(_)))
    }

    def writeMore(more: S, buf: ByteBuffer) = more match {
      case Left((s, b)) => (codecA.writeMore(s, buf) map (s => Left((s, b)))) orElse (codecB.writeInit(b, buf) map (Right(_)))
      case Right(s)     => codecB.writeMore(s, buf) map (Right(_))
    }

    def read(buf: ByteBuffer): C = to(codecA.read(buf), codecB.read(buf))
    override def skip(buf: ByteBuffer) {
      codecA.skip(buf)
      codecB.skip(buf)
    }
  }

  trait FixedWidthCodec[@spec(Boolean, Long, Double) A] extends Codec[A] {
    type S = A

    def size: Int

    def encodedSize(a: A)      = size
    override def maxSize(a: A) = size
    override def minSize(a: A) = size

    def writeInit(a: A, b: ByteBuffer): Option[A] =
      if (b.remaining >= size) {
        writeUnsafe(a, b)
        None
      } else {
        Some(a)
      }

    def writeMore(a: A, b: ByteBuffer): Option[A] = writeInit(a, b)
    override def skip(buf: ByteBuffer) {
      buf.position(buf.position() + size)
    }
  }

  case class ConstCodec[A](a: A) extends FixedWidthCodec[A] {
    val size = 0
    def writeUnsafe(a: A, sink: ByteBuffer): Unit = ()
    def read(buffer: ByteBuffer): A               = a
    override def skip(buf: ByteBuffer) {}
  }

  implicit case object ByteCodec extends FixedWidthCodec[Byte] {
    val size = 1
    def writeUnsafe(x: Byte, sink: ByteBuffer) { sink.put(x) }
    def read(src: ByteBuffer): Byte = src.get()
  }

  implicit case object BooleanCodec extends FixedWidthCodec[Boolean] {
    val size = 1
    def writeUnsafe(x: Boolean, sink: ByteBuffer) {
      if (x) sink.put(TRUE_VALUE) else sink.put(FALSE_VALUE)
    }
    def read(src: ByteBuffer): Boolean = src.get() match {
      case TRUE_VALUE  => true
      case FALSE_VALUE => false
      case invalid     => sys.error("Error reading boolean: expecting %d or %d, found %d" format (TRUE_VALUE, FALSE_VALUE, invalid))
    }
  }

  case object LongCodec extends FixedWidthCodec[Long] {
    val size = 8
    def writeUnsafe(n: Long, sink: ByteBuffer) { sink.putLong(n) }
    def read(src: ByteBuffer): Long = src.getLong()
  }

  implicit case object PackedLongCodec extends Codec[Long] {
    type S = Long

    override def maxSize(n: Long) = 10
    override def minSize(n: Long) = 10

    def encodedSize(sn: Long) = {
      @tailrec def loop(size: Int, n: Long): Int = {
        if ((n & ~0x7FL) != 0) loop(size + 1, n >> 7) else size
      }

      val n = if (sn < 0) ~sn else sn
      if ((n & ~0x3FL) != 0) loop(2, n >> 6) else 1
    }

    def writeInit(n: Long, buf: ByteBuffer): Option[S] = {
      if (buf.remaining() < 10) {
        Some(n)
      } else {
        writeUnsafe(n, buf)
        None
      }
    }

    def writeMore(n: Long, buf: ByteBuffer): Option[S] = writeInit(n, buf)

    def writeUnsafe(sn: Long, buf: ByteBuffer) {

      @inline
      @tailrec
      def loop(n: Long): Unit = if (n != 0) {
        buf.put(if ((n & ~0x7FL) != 0) (n & 0x7FL | 0x80L).toByte else (n & 0x7FL).toByte)
        loop(n >> 7)
      }

      var n = sn
      val lo = if (sn < 0) {
        n = ~sn
        n & 0x3FL | 0x40L
      } else {
        n & 0x3FL
      }

      if ((~0x3FL & n) != 0) {
        buf.put((lo | 0x80L).toByte)
        loop(n >> 6)
      } else {
        buf.put(lo.toByte)
      }
    }

    def read(buf: ByteBuffer): Long = {
      @inline
      @tailrec def loop(offset: Int, n: Long): Long = {
        val lo = buf.get().toLong
        val nn = n | ((lo & 0x7FL) << offset)
        if ((lo & 0x80L) != 0) loop(offset + 7, nn) else nn
      }

      val lo = buf.get().toLong
      val n  = if ((lo & 0x80L) != 0) loop(6, lo & 0x3FL) else (lo & 0x3FL)
      if ((lo & 0x40L) != 0) ~n else n
    }

    override def skip(buf: ByteBuffer) {
      while ((buf.get() & ~0x7F) != 0) {
        // Spin.
      }
    }
  }

  implicit val DateCodec   = Codec[Long].as[LocalDateTime](_.getMillis, dateTime.fromMillis)
  implicit val PeriodCodec = Codec[Long].as[Period](_.getMillis, period.fromMillis)

  implicit case object DoubleCodec extends FixedWidthCodec[Double] {
    val size = 8
    def writeUnsafe(n: Double, sink: ByteBuffer) { sink.putDouble(n) }
    def read(src: ByteBuffer): Double = src.getDouble()
  }

  implicit case object Utf8Codec extends Codec[String] {

    type S = Either[String, (CharBuffer, CharsetEncoder)]

    override def maxSize(s: String) = s.length * 4 + 5
    override def minSize(s: String) = 5

    def encodedSize(s: String): Int = {
      val size0 = strEncodedSize(s)
      size0 + sizePackedInt(size0)
    }

    private def strEncodedSize(s: String): Int = {
      var i    = 0
      var size = 0
      while (i < s.length) {
        val ch = s.codePointAt(i)
        if (ch < 0x80) {
          size += 1
        } else if (ch < 0x800) {
          size += 2
        } else if (ch < 0x10000) {
          size += 3
        } else {
          size += 4
          i += 1
        }
        i += 1
      }
      size
    }

    def writeUnsafe(a: String, sink: ByteBuffer) {
      val bytes = a.getBytes(Utf8Charset)
      writePackedInt(bytes.length, sink)
      sink.put(a.getBytes(Utf8Charset))
    }

    def writeInit(a: String, sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 5) Some(Left(a))
      else {
        writePackedInt(strEncodedSize(a), sink)

        val source  = CharBuffer.wrap(a)
        val encoder = Utf8Charset.newEncoder

        if (encoder.encode(source, sink, true) == CoderResult.OVERFLOW) {
          Some(Right((source, encoder)))
        } else {
          None
        }
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(a) => writeInit(a, sink)
      case Right((source, encoder)) =>
        if ((encoder.encode(source, sink, true) == CoderResult.OVERFLOW) ||
            (encoder.flush(sink) == CoderResult.OVERFLOW)) {
          Some(Right((source, encoder)))
        } else {
          None
        }
    }

    def read(src: ByteBuffer): String = {
      val bytes = new Array[Byte](readPackedInt(src))
      src.get(bytes)
      new String(bytes, Utf8Charset)
    }

    override def skip(buf: ByteBuffer) {
      val len = readPackedInt(buf)
      buf.position(buf.position() + len)
    }

    def compare(a: ByteBuffer, b: ByteBuffer): Int = {
      val alen = readPackedInt(a)
      val blen = readPackedInt(b)
      var cmp = 0
      var pos = 0
      val len = if (alen < blen) alen else blen
      while (cmp == 0 && pos < len) {
        cmp = (a.get() & 0xFF) - (b.get() & 0xFF)
        pos += 1
      }

      a.position(a.position() + (alen - pos))
      b.position(b.position() + (blen - pos))

      if (cmp == 0) {
        alen - blen
      } else {
        cmp
      }
    }
  }

  implicit val JBigDecimalCodec = CompositeCodec[Array[Byte], Long, BigDec](
    arrayCodec[Byte],
    PackedLongCodec,
    x => (x.unscaledValue.toByteArray, x.scale.toLong),
    (u, s) => new BigDec(new java.math.BigInteger(u), s.toInt))

  implicit val BigDecimalCodec = JBigDecimalCodec.as[BigDecimal](_.underlying, BigDecimal(_, java.math.MathContext.UNLIMITED))

  final class IndexedSeqCodec[A](val elemCodec: Codec[A]) extends Codec[IndexedSeq[A]] {

    type S = Either[IndexedSeq[A], (elemCodec.S, List[A])]

    override def minSize(as: IndexedSeq[A]): Int = 5
    override def maxSize(as: IndexedSeq[A]): Int =
      as.foldLeft(0) { (acc, a) =>
        acc + elemCodec.maxSize(a)
      } + 5

    def encodedSize(as: IndexedSeq[A]): Int = {
      val size = as.foldLeft(0) { (acc, a) =>
        acc + elemCodec.encodedSize(a)
      }
      size + sizePackedInt(as.size)
    }

    def writeUnsafe(as: IndexedSeq[A], sink: ByteBuffer) {
      writePackedInt(as.length, sink)
      as foreach { elemCodec.writeUnsafe(_, sink) }
    }

    @tailrec
    private def writeArray(as: List[A], sink: ByteBuffer): Option[S] = as match {
      case a :: as =>
        elemCodec.writeInit(a, sink) match {
          case Some(s) => Some(Right((s, as)))
          case None    => writeArray(as, sink)
        }
      case _ => None
    }

    def writeInit(as: IndexedSeq[A], sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 5) Some(Left(as))
      else {
        writePackedInt(as.length, sink)
        writeArray(as.toList, sink)
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(as)       => writeInit(as, sink)
      case Right((s, as)) => elemCodec.writeMore(s, sink) map (Right(_, as)) orElse writeArray(as.toList, sink)
    }

    def read(src: ByteBuffer): IndexedSeq[A] =
      ((0 until readPackedInt(src)) map (_ => elemCodec.read(src))).toIndexedSeq

    override def skip(buf: ByteBuffer) {
      (0 until readPackedInt(buf)) foreach { _ =>
        elemCodec.skip(buf)
      }
    }
  }
  case class ArrayCodec[@spec(Boolean, Long, Double) A: CTag](elemCodec: Codec[A]) extends Codec[Array[A]] {
    type S = Either[Array[A], (elemCodec.S, Array[A], Int)]

    override def minSize(as: Array[A]): Int = 5
    override def maxSize(as: Array[A]): Int = {
      @tailrec
      def loop(row: Int, size: Int): Int =
        if (row < as.length) {
          loop(row + 1, size + elemCodec.maxSize(as(row)))
        } else size

      loop(0, 5)
    }

    def encodedSize(as: Array[A]): Int = {
      @tailrec
      def loop(row: Int, size: Int): Int =
        if (row < as.length) {
          loop(row + 1, size + elemCodec.encodedSize(as(row)))
        } else size
      loop(0, sizePackedInt(as.length))
    }

    def writeUnsafe(as: Array[A], sink: ByteBuffer) {
      writePackedInt(as.length, sink)
      @tailrec
      def loop(row: Int): Unit = if (row < as.length) {
        elemCodec.writeUnsafe(as(row), sink)
        loop(row + 1)
      }
      loop(0)
    }

    @tailrec
    private def writeArray(as: Array[A], row: Int, sink: ByteBuffer): Option[S] =
      if (row < as.length) {
        elemCodec.writeInit(as(row), sink) match {
          case Some(s) => Some(Right((s, as, row + 1)))
          case None    => writeArray(as, row + 1, sink)
        }
      } else None

    def writeInit(as: Array[A], sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 5) Some(Left(as))
      else {
        writePackedInt(as.length, sink)
        writeArray(as, 0, sink)
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(as)            => writeInit(as, sink)
      case Right((s, as, row)) => elemCodec.writeMore(s, sink) map (s => Right((s, as, row))) orElse writeArray(as, row, sink)
    }

    def read(src: ByteBuffer): Array[A] = {
      val dst = new Array[A](readPackedInt(src))
      var row = 0
      while (row < dst.length) {
        dst(row) = elemCodec.read(src)
        row += 1
      }
      dst
    }

    override def skip(buf: ByteBuffer) {
      val length = readPackedInt(buf)
      var i = 0
      while (i < length) { elemCodec.skip(buf); i += 1 }
    }
  }

  /** A Codec that can (un)wrap CValues of type CValueType. */
  case class CValueCodec[A](cType: CValueType[A])(implicit val codec: Codec[A]) extends Codec[CWrappedValue[A]] {
    type S = codec.S
    def encodedSize(a: CWrappedValue[A])                   = codec.encodedSize(a.value)
    def writeUnsafe(a: CWrappedValue[A], sink: ByteBuffer) = codec.writeUnsafe(a.value, sink)
    def writeInit(a: CWrappedValue[A], sink: ByteBuffer)   = codec.writeInit(a.value, sink)
    def writeMore(s: S, sink: ByteBuffer)                  = codec.writeMore(s, sink)
    def read(src: ByteBuffer)                              = cType(codec.read(src))
    override def skip(buf: ByteBuffer)                     = codec.skip(buf)
  }

  // Problem: This can't be specialised.
  trait StatefulCodec {
    type A
    val codec: Codec[A]

    def init(a: A, sink: ByteBuffer): Option[State] =
      codec.writeInit(a, sink) map (State(_))

    case class State(s: codec.S) {
      def more(sink: ByteBuffer): Option[State] = codec.writeMore(s, sink) map (State(_))
    }
  }

  def wrappedWriteInit[AA](a: AA, sink: ByteBuffer)(implicit _codec: Codec[AA]): Option[StatefulCodec#State] =
    (new StatefulCodec {
      type A = AA
      val codec = _codec
    }).init(a, sink)

  implicit val BitSetCodec = ArrayCodec[Long](LongCodec)(implicitly).as[BitSet](_.getBits, BitSetUtil.fromArray)

  case class SparseBitSetCodec(size: Int) extends Codec[BitSet] {

    // The maxBytes is max. bits / 8 = (highestOneBit(size) << 3) / 8
    private val maxBytes = java.lang.Integer.highestOneBit(size) max 1

    type S = (Array[Byte], Int)

    def encodedSize(bs: BitSet)      = writeBitSet(bs).size
    override def maxSize(bs: BitSet) = maxBytes

    def writeUnsafe(bs: BitSet, sink: ByteBuffer) {
      sink.put(writeBitSet(bs))
    }

    def writeInit(bs: BitSet, sink: ByteBuffer): Option[S] = {
      val spaceLeft = sink.remaining()
      val bytes     = writeBitSet(bs)

      if (spaceLeft >= bytes.length) {
        sink.put(bytes)
        None
      } else {
        sink.put(bytes, 0, spaceLeft)
        Some((bytes, spaceLeft))
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = {
      val (bytes, offset) = more
      val bytesLeft       = bytes.length - offset
      val spaceLeft       = sink.remaining()

      if (spaceLeft >= bytesLeft) {
        sink.put(bytes, offset, bytesLeft)
        None
      } else {
        sink.put(bytes, offset, spaceLeft)
        Some((bytes, offset + spaceLeft))
      }
    }

    def read(src: ByteBuffer): BitSet = readBitSet(src)

    override def skip(buf: ByteBuffer) {
      var b = buf.get()
      while ((b & 3) != 0 && (b & 12) != 0 && (b & 48) != 0 && (b & 192) != 0) {
        b = buf.get()
      }
    }

    def writeBitSet(bs: BitSet): Array[Byte] = {
      val bytes = new Array[Byte](maxBytes)

      def set(offset: Int) {
        val i = offset >>> 3
        bytes(i) = ((1 << (offset & 0x7)) | bytes(i).toInt).toByte
      }

      def rec(bs: List[Int], l: Int, r: Int, offset: Int): Int = {
        val c = (l + r) / 2

        if (l == r) {
          offset
        } else if (r - l == 1) {
          if (bs == (l :: Nil)) {
            set(offset)
          }
          set(offset + 1)
          offset + 2
        } else {
          bs partition (_ < c) match {
            case (Nil, Nil) =>
              offset
            case (Nil, hi) =>
              set(offset + 1)
              rec(hi, c, r, offset + 2)
            case (lo, Nil) =>
              set(offset)
              rec(lo, l, c, offset + 2)
            case (lo, hi) =>
              set(offset)
              set(offset + 1)
              rec(hi, c, r, rec(lo, l, c, offset + 2))
          }
        }
      }

      val len = rec(bs.toList, 0, size, 0)
      java.util.Arrays.copyOf(bytes, (len >>> 3) + 1) // The +1 covers the extra 2 '0' bits.
    }

    def readBitSet(src: ByteBuffer): BitSet = {
      val pos = src.position()
      @inline def get(offset: Int): Boolean =
        (src.get(pos + (offset >>> 3)) & (1 << (offset & 7))) != 0

      val bits = new BitSet
      def read(l: Int, r: Int, offset: Int): Int = {
        if (l == r) {
          offset
        } else if (r - l == 1) {
          if (get(offset)) {
            bits(l) = true
          }
          offset + 2
        } else {
          val c = (l + r) / 2
          (get(offset), get(offset + 1)) match {
            case (false, false) => offset + 2
            case (false, true)  => read(c, r, offset + 2)
            case (true, false)  => read(l, c, offset + 2)
            case (true, true)   => read(c, r, read(l, c, offset + 2))
          }
        }
      }

      val offset = read(0, size, 0)
      src.position(pos + (offset >>> 3) + 1)
      bits
    }
  }

  case class SparseRawBitSetCodec(size: Int) extends Codec[RawBitSet] {

    // The maxBytes is max. bits / 8 = (highestOneBit(size) << 3) / 8
    private val maxBytes = java.lang.Integer.highestOneBit(size) max 1

    type S = (Array[Byte], Int)

    def encodedSize(bs: RawBitSet)      = writeBitSet(bs).size
    override def maxSize(bs: RawBitSet) = maxBytes

    def writeUnsafe(bs: RawBitSet, sink: ByteBuffer) {
      sink.put(writeBitSet(bs))
    }

    def writeInit(bs: RawBitSet, sink: ByteBuffer): Option[S] = {
      val spaceLeft = sink.remaining()
      val bytes     = writeBitSet(bs)

      if (spaceLeft >= bytes.length) {
        sink.put(bytes)
        None
      } else {
        sink.put(bytes, 0, spaceLeft)
        Some((bytes, spaceLeft))
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = {
      val (bytes, offset) = more
      val bytesLeft       = bytes.length - offset
      val spaceLeft       = sink.remaining()

      if (spaceLeft >= bytesLeft) {
        sink.put(bytes, offset, bytesLeft)
        None
      } else {
        sink.put(bytes, offset, spaceLeft)
        Some((bytes, offset + spaceLeft))
      }
    }

    def read(src: ByteBuffer): RawBitSet = readBitSet(src)

    override def skip(buf: ByteBuffer) {
      var b = buf.get()
      while ((b & 3) != 0 && (b & 12) != 0 && (b & 48) != 0 && (b & 192) != 0) {
        b = buf.get()
      }
    }

    def writeBitSet(bs: RawBitSet): Array[Byte] = {
      val bytes = new Array[Byte](maxBytes)

      def set(offset: Int) {
        val i = offset >>> 3
        bytes(i) = ((1 << (offset & 0x7)) | bytes(i).toInt).toByte
      }

      val arr = RawBitSet.toArray(bs)
      def rec_(start: Int, end: Int, l: Int, r: Int, offset: Int): Int = {

        if (l == r) return offset

        if (r - l == 1) {
          if (arr(start) == l) set(offset)
          set(offset + 1)
          return offset + 2
        }

        val c = (l + r) / 2
        var i = start
        while (arr(i) < c && i < end) i += 1

        if (i == start) {
          if (i == end) {
            offset
          } else {
            set(offset + 1)
            rec_(i, end, c, r, offset + 2)
          }
        } else {
          if (i == end) {
            set(offset)
            rec_(start, i, l, c, offset + 2)
          } else {
            set(offset)
            set(offset + 1)
            rec_(i, end, c, r, rec_(start, i, l, c, offset + 2))
          }
        }
      }

      def rec(bs: List[Int], l: Int, r: Int, offset: Int): Int = {
        val c = (l + r) / 2

        if (l == r) {
          offset
        } else if (r - l == 1) {
          if (bs == (l :: Nil)) {
            set(offset)
          }
          set(offset + 1)
          offset + 2
        } else {
          bs partition (_ < c) match {
            case (Nil, Nil) =>
              offset
            case (Nil, hi) =>
              set(offset + 1)
              rec(hi, c, r, offset + 2)
            case (lo, Nil) =>
              set(offset)
              rec(lo, l, c, offset + 2)
            case (lo, hi) =>
              set(offset)
              set(offset + 1)
              rec(hi, c, r, rec(lo, l, c, offset + 2))
          }
        }
      }

      val len = rec(RawBitSet.toList(bs), 0, size, 0)
      java.util.Arrays.copyOf(bytes, (len >>> 3) + 1) // The +1 covers the extra 2 '0' bits.
    }

    def readBitSet(src: ByteBuffer): RawBitSet = {
      val pos = src.position()
      @inline def get(offset: Int): Boolean =
        (src.get(pos + (offset >>> 3)) & (1 << (offset & 7))) != 0

      val bits = RawBitSet.create(size)

      def read(l: Int, r: Int, offset: Int): Int = {
        if (l == r) {
          offset
        } else if (r - l == 1) {
          if (get(offset)) {
            RawBitSet.set(bits, l)
          }
          offset + 2
        } else {
          val c = (l + r) / 2
          (get(offset), get(offset + 1)) match {
            case (false, false) => offset + 2
            case (false, true)  => read(c, r, offset + 2)
            case (true, false)  => read(l, c, offset + 2)
            case (true, true)   => read(c, r, read(l, c, offset + 2))
          }
        }
      }

      val offset = read(0, size, 0)
      src.position(pos + (offset >>> 3) + 1)
      bits
    }
  }
}
