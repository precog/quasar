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
package jdbm3

import table._
import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.util.RawBitSet
import scalaz._, Scalaz._
import quasar.precog.util.{ ByteBufferMonad, ByteBufferPool, NumericComparisons }, ByteBufferPool._

import java.nio.ByteBuffer
import java.time.ZonedDateTime

import scala.annotation.tailrec
import scala.collection.mutable

trait ColumnEncoder {
  def encodeFromRow(row: Int): Array[Byte]
}

trait ColumnDecoder {
  def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0): Unit
}

trait RowFormat {

  def columnRefs: Seq[ColumnRef]

  def ColumnEncoder(cols: Seq[Column]): ColumnEncoder
  def ColumnDecoder(cols: Seq[ArrayColumn[_]]): ColumnDecoder

  def encode(cValues: List[CValue]): Array[Byte]
  def decode(bytes: Array[Byte], offset: Int = 0): List[CValue]

  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val selectors = columnRefs map (_.selector)
    val aVals     = selectors zip decode(a) groupBy (_._1)
    val bVals     = selectors zip decode(b) groupBy (_._1)

    val cmp: Int = selectors.distinct.iterator map { cPath =>
      val a = aVals(cPath) find (_._2 != CUndefined)
      val b = bVals(cPath) find (_._2 != CUndefined)
      (a, b) match {
        case (None, None)                 => 0
        case (None, _)                    => -1
        case (_, None)                    => 1
        case (Some((_, a)), Some((_, b))) => (a ?|? b).toInt
      }
    } find (_ != 0) getOrElse 0

    cmp
  }
}

object RowFormat {
  val byteBufferPool = new ByteBufferPool()

  def forSortingKey(columnRefs: Seq[ColumnRef]): RowFormat = SortingKeyRowFormatV1(columnRefs)

  def forValues(columnRefs: Seq[ColumnRef]): RowFormat = ValueRowFormatV1(columnRefs)

  case class ValueRowFormatV1(_columnRefs: Seq[ColumnRef]) extends ValueRowFormat with RowFormatCodecs {
    // This is really stupid, but required to work w/ JDBM.
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }

    // TODO Get this from somewhere else?
    def pool = byteBufferPool
  }

  case class SortingKeyRowFormatV1(_columnRefs: Seq[ColumnRef]) extends RowFormatCodecs with SortingRowFormat {
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }

    def pool = byteBufferPool
  }

  case class IdentitiesRowFormatV1(_columnRefs: Seq[ColumnRef]) extends IdentitiesRowFormat {
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }
  }
}

trait RowFormatSupport { self: StdCodecs =>
  protected trait ColumnValueEncoder {
    def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]]
  }

  protected trait SimpleColumnValueEncoder[A] extends ColumnValueEncoder {
    val codec: Codec[A]

    @tailrec
    protected final def writeMore(s: codec.S, pool: ByteBufferPool, buffers: List[ByteBuffer]): List[ByteBuffer] = {
      val buffer = pool.acquire
      codec.writeMore(s, buffer) match {
        case Some(s) => writeMore(s, pool, buffer :: buffers)
        case None    => (buffer :: buffers).reverse
      }
    }
  }

  def getColumnEncoder(cType: CType, col: Column): ColumnValueEncoder = (cType, col) match {
    case (CLong, col: LongColumn) =>
      new SimpleColumnValueEncoder[Long] {
        val codec = Codec[Long]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CDouble, col: DoubleColumn) =>
      new SimpleColumnValueEncoder[Double] {
        val codec = Codec[Double]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CNum, col: NumColumn) =>
      new SimpleColumnValueEncoder[BigDecimal] {
        val codec = Codec[BigDecimal]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CBoolean, col: BoolColumn) =>
      new SimpleColumnValueEncoder[Boolean] {
        val codec = Codec[Boolean]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CString, col: StrColumn) =>
      new SimpleColumnValueEncoder[String] {
        val codec = Codec[String]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CDate, col: DateColumn) =>
      new SimpleColumnValueEncoder[ZonedDateTime] {
        val codec = Codec[ZonedDateTime]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CPeriod, col: PeriodColumn) =>
      new SimpleColumnValueEncoder[Period] {
        val codec = Codec[Period]

        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
          codec.writeInit(col(row), buffer) match {
            case Some(s) => Some(writeMore(s, pool, buffer :: Nil))
            case None    => None
          }
        }
      }

    case (CEmptyObject, col: EmptyObjectColumn) =>
      new ColumnValueEncoder {
        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = None
      }

    case (CEmptyArray, col: EmptyArrayColumn) =>
      new ColumnValueEncoder {
        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = None
      }

    case (CNull, col: NullColumn) =>
      new ColumnValueEncoder {
        def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = None
      }

    case (cType, col) => sys.error("Cannot create column encoder, columns of wrong type (expected %s, found %s)." format (cType, col.tpe))
  }

  protected trait ColumnValueDecoder {
    def decode(row: Int, buf: ByteBuffer): Unit
  }

  def getColumnDecoder(cType: CType, col: ArrayColumn[_]): ColumnValueDecoder = (cType, col) match {
    case (CLong, col: ArrayLongColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[Long].read(buf))
      }
    case (CDouble, col: ArrayDoubleColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[Double].read(buf))
      }
    case (CNum, col: ArrayNumColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[BigDecimal].read(buf))
      }
    case (CBoolean, col: ArrayBoolColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[Boolean].read(buf))
      }
    case (CString, col: ArrayStrColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[String].read(buf))
      }
    case (CDate, col: ArrayDateColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[ZonedDateTime].read(buf))
      }
    case (CPeriod, col: ArrayPeriodColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, Codec[Period].read(buf))
      }
    case (CEmptyObject, col: MutableEmptyObjectColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, true)
      }
    case (CEmptyArray, col: MutableEmptyArrayColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, true)
      }
    case (CNull, col: MutableNullColumn) =>
      new ColumnValueDecoder {
        def decode(row: Int, buf: ByteBuffer) = col.update(row, true)
      }
    case _ => sys.error("Cannot create column decoder, columns of wrong type.")
  }

  protected def encodeRow(row: Int, undefined: Array[Int], encoders: Array[ColumnValueEncoder], init: ByteBuffer, pool: ByteBufferPool): Array[Byte] = {

    var buffer = init
    var filled: mutable.ListBuffer[ByteBuffer] = null

    @inline
    @tailrec
    def encodeAll(i: Int): Unit = if (i < encoders.length) {
      if (!RawBitSet.get(undefined, i)) {
        encoders(i).encode(row, buffer, pool) match {
          case Some(buffers) =>
            if (filled == null)
              filled = new mutable.ListBuffer[ByteBuffer]()
            filled ++= buffers
            buffer = pool.acquire
          case None =>
        }
      }
      encodeAll(i + 1)
    }

    encodeAll(0)

    if (filled != null) {
      filled += buffer
      val all   = filled.toList
      val bytes = ByteBufferPool.getBytesFrom(filled.toList)
      all foreach { pool.release(_) }
      bytes

    } else {
      buffer.flip()
      val len   = buffer.remaining()
      val bytes = new Array[Byte](len)
      buffer.get(bytes)
      pool.release(buffer)
      bytes
    }
  }
}

trait ValueRowFormat extends RowFormat with RowFormatSupport { self: StdCodecs =>
  import ByteBufferPool._

  def pool: ByteBufferPool

  def encode(cValues: List[CValue]) = getBytesFrom(RowCodec.writeAll(cValues)(pool.acquire _).reverse)

  def decode(bytes: Array[Byte], offset: Int): List[CValue] =
    RowCodec.read(ByteBufferWrap(bytes, offset, bytes.length - offset))

  def ColumnEncoder(cols: Seq[Column]) = {
    require(columnRefs.size == cols.size)

    val colValueEncoders: Array[ColumnValueEncoder] = {
      (columnRefs zip cols).map({
        case (ColumnRef(_, cType), col) =>
          getColumnEncoder(cType, col)
      })(collection.breakOut)
    }

    new ColumnEncoder {
      val colsArray = cols.toArray
      def encodeFromRow(row: Int) = {
        val undefined = RawBitSet.create(colsArray.length)

        @inline
        @tailrec def definedCols(i: Int): Unit = if (i >= 0) {
          if (!colsArray(i).isDefinedAt(row)) RawBitSet.set(undefined, i)
          definedCols(i - 1)
        }
        definedCols(colsArray.length - 1)

        val init = pool.acquire
        Codec[Array[Int]].writeUnsafe(undefined, init)
        encodeRow(row, undefined, colValueEncoders, init, pool)
      }
    }
  }

  def ColumnDecoder(cols: Seq[ArrayColumn[_]]) = {
    require(columnRefs.size == cols.size)

    //val decoders: Seq[(ColumnValueDecoder, Int)] = // Seq[((Int, ByteBuffer) => Unit, Int)] =
    //  (columnRefs zip cols map { case (ref, col) => getColumnDecoder(ref.ctype, col) }).zipWithIndex
    val decoders: List[ColumnValueDecoder] = (columnRefs zip cols).map {
      case (ref, col) => getColumnDecoder(ref.ctype, col)
    }(collection.breakOut)

    new ColumnDecoder {
      def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0) {
        val buf       = ByteBufferWrap(src, offset, src.length - offset)
        val undefined = Codec[Array[Int]].read(buf)
        @tailrec
        def helper(i: Int, decs: List[ColumnValueDecoder]) {
          decs match {
            case h :: t =>
              if (!RawBitSet.get(undefined, i)) h.decode(row, buf)
              helper(i + 1, t)
            case Nil =>
          }
        }
        helper(0, decoders)
      }
    }
  }

  case object RowCodec extends Codec[List[CValue]] {
    import Codec.{ StatefulCodec, wrappedWriteInit }

    @transient lazy val rawBitSetCodec = Codec[Array[Int]]

    @transient private lazy val codecs: List[Codec[_ <: CValue]] = columnRefs.toList map {
      case ColumnRef(_, cType: CValueType[_]) => Codec.CValueCodec(cType)(codecForCValueType(cType))
      case ColumnRef(_, cType: CNullType)     => Codec.ConstCodec(cType)
    }

    type S = (Either[rawBitSetCodec.S, StatefulCodec#State], List[CValue])

    private def undefineds(xs: List[CValue]): Array[Int] = {
      val bits = RawBitSet.create(xs.size)

      @inline
      @tailrec
      def rec(i: Int, xs: List[CValue]): Unit = xs match {
        case CUndefined :: xs => RawBitSet.set(bits, i); rec(i + 1, xs)
        case _ :: xs          => rec(i + 1, xs)
        case Nil              =>
      }
      rec(0, xs)

      bits
    }

    def encodedSize(xs: List[CValue]) = xs.foldLeft(rawBitSetCodec.encodedSize(undefineds(xs))) { (acc, x) =>
      acc + (x match {
            case x: CWrappedValue[_] => codecForCValueType(x.cType).encodedSize(x.value)
            case _                   => 0
          })
    }

    override def maxSize(xs: List[CValue]) = xs.foldLeft(rawBitSetCodec.maxSize(undefineds(xs))) { (acc, x) =>
      acc + (x match {
            case x: CWrappedValue[_] => codecForCValueType(x.cType).maxSize(x.value)
            case _                   => 0
          })
    }

    def writeUnsafe(xs: List[CValue], sink: ByteBuffer) {
      rawBitSetCodec.writeUnsafe(undefineds(xs), sink)
      xs foreach {
        case x: CWrappedValue[_] => codecForCValueType(x.cType).writeUnsafe(x.value, sink)
        case _                   =>
      }
    }

    @tailrec
    private def writeCValues(xs: List[CValue], sink: ByteBuffer): Option[S] = xs match {
      case x :: xs =>
        (x match {
          case CBoolean(x)      => wrappedWriteInit[Boolean](x, sink)
          case CString(x)       => wrappedWriteInit[String](x, sink)
          case CDate(x)         => wrappedWriteInit[ZonedDateTime](x, sink)
          case CPeriod(x)       => wrappedWriteInit[Period](x, sink)
          case CLong(x)         => wrappedWriteInit[Long](x, sink)
          case CDouble(x)       => wrappedWriteInit[Double](x, sink)
          case CNum(x)          => wrappedWriteInit[BigDecimal](x, sink)
          case CArray(x, cType) => wrappedWriteInit(x, sink)(codecForCValueType(cType))
          case _: CNullType     => None
        }) match {
          case None    => writeCValues(xs, sink)
          case Some(s) => Some((Right(s), xs))
        }

      case Nil => None
    }

    def writeInit(xs: List[CValue], sink: ByteBuffer) = {
      rawBitSetCodec.writeInit(undefineds(xs), sink) match {
        case Some(s) => Some((Left(s), xs))
        case None    => writeCValues(xs, sink)
      }
    }

    def writeMore(more: S, sink: ByteBuffer) = more match {
      case (Left(s), xs)  => rawBitSetCodec.writeMore(s, sink) map (s => (Left(s), xs)) orElse writeCValues(xs, sink)
      case (Right(s), xs) => s.more(sink) map (s => (Right(s), xs)) orElse writeCValues(xs, sink)
    }

    def read(src: ByteBuffer): List[CValue] = {
      val undefined = rawBitSetCodec.read(src)
      codecs.zipWithIndex collect {
        case (codec, i) if RawBitSet.get(undefined, i) => CUndefined
        case (codec, _)                                => codec.read(src)
      }
    }
  }
}

/**
  * This is a row format that is optimized for quickly comparing 2 encoded rows
  * (ie. byte arrays).
  */
trait SortingRowFormat extends RowFormat with StdCodecs with RowFormatSupport {
  import SortingRowFormat._

  def pool: ByteBufferPool

  override implicit def StringCodec = Codec.Utf8Codec

  @transient
  abstract override implicit lazy val BigDecimalCodec: Codec[BigDecimal] =
    Codec.CompositeCodec[Double, BigDecimal, BigDecimal](Codec[Double], super.BigDecimalCodec, bd => (bd.toDouble, bd), (_, bd) => bd)

  @transient lazy val selectors: List[(CPath, List[CType])] = {
    val refs: Map[CPath, Seq[ColumnRef]] = columnRefs.groupBy(_.selector)
    (columnRefs map (_.selector)).distinct.map(selector => (selector, refs(selector).map(_.ctype).toList))(collection.breakOut)
  }

  private def zipWithSelectors[A](xs: Seq[A]): List[(CPath, Seq[(A, CType)])] = {
    @tailrec
    def zip(zipped: List[(CPath, Seq[(A, CType)])], right: Seq[A], sels: List[(CPath, List[CType])]): List[(CPath, Seq[(A, CType)])] = sels match {
      case Nil => zipped.reverse
      case (path, cTypes) :: sels =>
        val (head, tail) = right splitAt cTypes.size
        zip((path, head zip cTypes) :: zipped, tail, sels)
    }

    zip(Nil, xs, selectors)
  }

  def ColumnEncoder(cols: Seq[Column]) = {
    val colValueEncoders: Array[ColumnValueEncoder] = zipWithSelectors(cols).map({
      case (_, colsAndTypes) =>
        val writers: Seq[ColumnValueEncoder] = colsAndTypes map {
          case (col, cType) =>
            val writer = getColumnEncoder(cType, col)
            new ColumnValueEncoder {
              def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
                val flag = SortingRowFormat.flagForCType(cType)
                if (buffer.remaining() > 0) {
                  buffer.put(flag)
                  writer.encode(row, buffer, pool)
                } else {
                  val nextBuffer = pool.acquire
                  nextBuffer.put(flag)
                  writer.encode(row, nextBuffer, pool) match {
                    case Some(buffers) => Some(buffer :: buffers)
                    case None          => Some(buffer :: nextBuffer :: Nil)
                  }
                }
              }
            }
        }

        val selCols: Seq[Column] = colsAndTypes map (_._1)

        new ColumnValueEncoder {
          def encode(row: Int, buffer: ByteBuffer, pool: ByteBufferPool): Option[List[ByteBuffer]] = {
            (writers zip selCols) find (_._2.isDefinedAt(row)) map (_._1.encode(row, buffer, pool)) getOrElse {
              val flag = SortingRowFormat.flagForCType(CUndefined)
              if (buffer.remaining() > 0) {
                buffer.put(flag)
                None
              } else {
                val nextBuffer = pool.acquire
                nextBuffer.put(flag)
                Some(buffer :: nextBuffer :: Nil)
              }
            }
          }
        }
    })(collection.breakOut)

    new ColumnEncoder {
      val undefined = RawBitSet.create(0)
      def encodeFromRow(row: Int) =
        encodeRow(row, undefined, colValueEncoders, pool.acquire, pool)
    }
  }

  def ColumnDecoder(cols: Seq[ArrayColumn[_]]) = {
    val decoders: List[Map[Byte, ColumnValueDecoder]] =
      zipWithSelectors(cols) map {
        case (_, colsWithTypes) =>
          val decoders: Map[Byte, ColumnValueDecoder] = (for ((col, cType) <- colsWithTypes) yield {
            (flagForCType(cType), getColumnDecoder(cType, col))
          })(collection.breakOut)

          decoders
      }

    new ColumnDecoder {
      def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0) {
        val buf = ByteBufferWrap(src, offset, src.length - offset)

        @tailrec
        def decode(decoders: List[Map[Byte, ColumnValueDecoder]]): Unit = decoders match {
          case selDecoder :: decoders =>
            val flag = buf.get()
            if (flag != FUndefined) {
              selDecoder(flag).decode(row, buf)
            }
            decode(decoders)
          case Nil =>
          // Do nothing.
        }

        decode(decoders)
      }
    }
  }

  def encode(cValues: List[CValue]): Array[Byte] = {

    val cvals: List[CValue] = zipWithSelectors(cValues) map {
      case (_, cvals) => cvals map (_._1) find (_ != CUndefined) getOrElse CUndefined
    }

    val writes: ByteBufferPoolS[List[Unit]] = cvals.map {
      case v: CNullValue =>
        writeFlagFor[ByteBufferPoolS](v.cType)

      case v: CWrappedValue[_] =>
        for {
          _ <- writeFlagFor[ByteBufferPoolS](v.cType)
          _ <- codecForCValueType(v.cType).write(v.value)
        } yield ()
    }.sequence[ByteBufferPoolS, Unit]

    pool.run(for {
      _ <- writes
      bytes <- flipBytes
      _ <- release
    } yield bytes)
  }

  def decode(bytes: Array[Byte], offset: Int = 0): List[CValue] = {
    val buf = ByteBufferWrap(bytes)

    def readForSelector(cTypes: List[CType]): List[CValue] = {
      val cValue = cTypeForFlag(buf.get()) match {
        case cType: CValueType[_] =>
          cType(codecForCValueType(cType).read(buf))
        case cType: CNullType =>
          cType
      }

      val cType = cValue.cType
      cTypes map {
        case `cType` => cValue
        case _       => CUndefined
      }
    }

    selectors.map {
      case (_, cTypes) =>
        readForSelector(cTypes)
    }.flatten
  }

  override def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val abuf = ByteBufferWrap(a)
    val bbuf = ByteBufferWrap(b)

    @inline
    def compareNext(): Int = {
      val aType = abuf.get()
      val bType = bbuf.get()

      if ((aType & 0xF0) == (bType & 0xF0)) {
        ((aType & 0xF0).toByte) match {
          case FUndefined => 0
          case FBoolean =>
            abuf.get() - bbuf.get()
          case FString =>
            Codec.Utf8Codec.compare(abuf, bbuf)
          case FNumeric =>
            aType match {
              case FLong =>
                val a = Codec[Long].read(abuf)
                bType match {
                  case FLong =>
                    NumericComparisons.compare(a, Codec[Long].read(bbuf))
                  case FDouble =>
                    NumericComparisons.compare(a, Codec[Double].read(bbuf))
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a.toDouble, b) match {
                      case 0 =>
                        BigDecimal(a) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }
                }
              case FDouble =>
                val a = Codec[Double].read(abuf)
                bType match {
                  case FLong =>
                    NumericComparisons.compare(a, Codec[Long].read(bbuf))
                  case FDouble =>
                    NumericComparisons.compare(a, Codec[Double].read(bbuf))
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        BigDecimal(a) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }

                }
              case FBigDecimal =>
                val a = Codec[Double].read(abuf)
                bType match {
                  case FLong =>
                    val b = Codec[Long].read(bbuf)
                    NumericComparisons.approxCompare(a, b.toDouble) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare BigDecimal(b)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        cmp
                    }
                  case FDouble =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare BigDecimal(b)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        cmp
                    }
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }
                }
            }
          case FEmptyObject => 0
          case FEmptyArray  => 0
          case FNull        => 0
          case FDate =>
            math.signum(Codec[Long].read(abuf) - Codec[Long].read(bbuf)).toInt
          case FPeriod =>
            math.signum(Codec[Long].read(abuf) - Codec[Long].read(bbuf)).toInt
          case x => sys.error("Match error for: " + x)
        }
      } else {
        (aType.toInt & 0xFF) - (bType.toInt & 0xFF)
      }
    }

    @tailrec
    def compare(cmp: Int): Int =
      if (cmp == 0) {
        if (abuf.remaining() > 0) compare(compareNext()) else 0
      } else cmp

    compare(0)
  }
}

object SortingRowFormat {
  def writeFlagFor[M[_]](cType: CType)(implicit M: ByteBufferMonad[M]): M[Unit] = {
    val flag = flagForCType(cType)
    M getBuffer 1 map (_ put flag)
  }

  def flagForCType(cType: CType): Byte = cType match {
    case CBoolean      => FBoolean
    case CString       => FString
    case CLong         => FLong
    case CDouble       => FDouble
    case CNum          => FBigDecimal
    case CDate         => FDate
    case CPeriod       => FPeriod
    case CEmptyObject  => FEmptyObject
    case CEmptyArray   => FEmptyArray
    case CNull         => FNull
    case CUndefined    => FUndefined
    case CArrayType(_) => abort("Unexpected CArrayType")
  }

  def cTypeForFlag(flag: Byte): CType = flag match {
    case FBoolean     => CBoolean
    case FString      => CString
    case FLong        => CLong
    case FDouble      => CDouble
    case FBigDecimal  => CNum
    case FDate        => CDate
    case FPeriod      => CPeriod
    case FEmptyObject => CEmptyObject
    case FEmptyArray  => CEmptyArray
    case FNull        => CNull
    case FUndefined   => CUndefined
  }

  private val FUndefined: Byte   = 0x0.toByte
  private val FBoolean: Byte     = 0x10.toByte
  private val FString: Byte      = 0x20.toByte
  private val FNumeric: Byte     = 0x40.toByte
  private val FLong: Byte        = 0x41.toByte
  private val FDouble: Byte      = 0x42.toByte
  private val FBigDecimal: Byte  = 0x43.toByte
  private val FEmptyObject: Byte = 0x60.toByte
  private val FEmptyArray: Byte  = 0x70.toByte
  private val FNull: Byte        = 0x80.toByte
  private val FDate: Byte        = 0x90.toByte
  private val FPeriod: Byte      = 0x91.toByte
}

trait IdentitiesRowFormat extends RowFormat {

  lazy val identities: Int = columnRefs.size

  // FYI: This is here purely to ensure backwards compatiblity. Not used.
  // When we upgrade the serialization format, we can remove this.
  private final val codec = Codec.PackedLongCodec

  private final def packedSize(n: Long): Int = {

    @inline
    @tailrec
    def loop(size: Int, n: Long): Int = {
      val m = n >>> 7
      if (m == 0) size + 1 else loop(size + 1, m)
    }

    loop(0, n)
  }

  // Packs the Long n into bytes, starting at offset, and returns the next free
  // position in bytes to store a Long.
  private final def packLong(n: Long, bytes: Array[Byte], offset: Int): Int = {

    @tailrec
    @inline
    def loop(i: Int, n: Long): Int = {
      val m = n >>> 7
      val b = n & 0x7FL
      if (m != 0) {
        bytes(i) = (b | 0x80L).toByte
        loop(i + 1, m)
      } else {
        bytes(i) = b.toByte
        i + 1
      }
    }

    loop(offset, n)
  }

  @inline private final def shiftIn(b: Byte, shift: Int, n: Long): Long = n | ((b.toLong & 0x7FL) << shift)

  @inline private final def more(b: Byte): Boolean = (b & 0x80) != 0

  def encodeIdentities(xs: Array[Long]) = {

    @inline
    @tailrec
    def sumPackedSize(xs: Array[Long], i: Int, len: Int): Int =
      if (i < xs.length) {
        sumPackedSize(xs, i + 1, len + packedSize(xs(i)))
      } else {
        len
      }

    val bytes = new Array[Byte](sumPackedSize(xs, 0, 0))

    @inline
    @tailrec
    def packAll(xs: Array[Long], i: Int, offset: Int) {
      if (i < xs.length) packAll(xs, i + 1, packLong(xs(i), bytes, offset))
    }

    packAll(xs, 0, 0)
    bytes
  }

  def encode(cValues: List[CValue]): Array[Byte] = {
    @inline
    @tailrec
    def sumPackedSize(cvals: List[CValue], len: Int): Int = cvals match {
      case CLong(n) :: cvals => sumPackedSize(cvals, len + packedSize(n))
      case cv :: _           => sys.error("Expecting CLong, but found: " + cv)
      case Nil               => len
    }

    val bytes = new Array[Byte](sumPackedSize(cValues, 0))

    @inline
    @tailrec
    def packAll(xs: List[CValue], offset: Int): Unit = xs match {
      case CLong(n) :: xs => packAll(xs, packLong(n, bytes, offset))
      case _              =>
    }

    packAll(cValues, 0)
    bytes
  }

  def decode(bytes: Array[Byte], offset: Int): List[CValue] = {
    val longs = new Array[Long](identities)

    @inline
    @tailrec
    def loop(offset: Int, shift: Int, n: Long, i: Int) {
      val lo      = bytes(offset)
      val m       = shiftIn(lo, shift, n)
      val nOffset = offset + 1
      if (more(lo)) loop(nOffset, shift + 7, m, i)
      else {
        longs(i) = m
        if (nOffset < bytes.length)
          loop(nOffset, 0, 0L, i + 1)
      }
    }

    if (identities > 0)
      loop(offset, 0, 0L, 0)

    longs.map(CLong(_))(collection.breakOut)
  }

  def ColumnEncoder(cols: Seq[Column]) = {

    val longCols: Array[LongColumn] = cols.map({
      case col: LongColumn => col
      case col             => sys.error("Expecing LongColumn, but found: " + col)
    })(collection.breakOut)

    new ColumnEncoder {
      def encodeFromRow(row: Int): Array[Byte] = {

        @inline
        @tailrec
        def sumPackedSize(i: Int, len: Int): Int =
          if (i < longCols.length) {
            sumPackedSize(i + 1, len + packedSize(longCols(i)(row)))
          } else len

        val bytes = new Array[Byte](sumPackedSize(0, 0))

        @inline
        @tailrec
        def packAll(i: Int, offset: Int): Unit = if (i < longCols.length) {
          packAll(i + 1, packLong(longCols(i)(row), bytes, offset))
        }

        packAll(0, 0)
        bytes
      }
    }
  }

  def ColumnDecoder(cols: Seq[ArrayColumn[_]]) = {

    val longCols: Array[ArrayLongColumn] = cols.map({
      case col: ArrayLongColumn => col
      case col                  => sys.error("Expecing ArrayLongColumn, but found: " + col)
    })(collection.breakOut)

    new ColumnDecoder {
      def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0) {

        @inline
        @tailrec
        def loop(offset: Int, shift: Int, n: Long, col: Int) {
          val b       = src(offset)
          val m       = shiftIn(b, shift, n)
          val nOffset = offset + 1
          if (more(b)) loop(nOffset, shift + 7, m, col)
          else {
            longCols(col).update(row, m)
            if (nOffset < src.length)
              loop(nOffset, 0, 0L, col + 1)
          }
        }

        if (src.length > 0) loop(0, 0, 0L, 0)
      }
    }
  }

  override def compare(a: Array[Byte], b: Array[Byte]): Int = {

    @inline
    @tailrec
    def loop(offset: Int, shift: Int, n: Long, m: Long): Int = {
      val b1      = a(offset)
      val b2      = b(offset)
      val n2      = shiftIn(b1, shift, n)
      val m2      = shiftIn(b2, shift, m)
      val nOffset = offset + 1
      val moreA   = more(b1)
      val moreB   = more(b2)

      if (moreA && moreB) {
        loop(nOffset, shift + 7, n2, m2)
      } else if (moreA) {
        1
      } else if (moreB) {
        -1
      } else if (n2 < m2) {
        -1
      } else if (m2 < n2) {
        1
      } else if (nOffset < a.length) {
        loop(nOffset, 0, 0L, 0L)
      } else {
        0
      }
    }

    if (identities == 0) 0 else loop(0, 0, 0L, 0L)
  }
}
