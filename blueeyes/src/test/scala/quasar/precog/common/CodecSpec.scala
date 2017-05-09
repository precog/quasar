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

import quasar.blueeyes._
import quasar.precog.util.{ BitSetUtil, ByteBufferPool, RawBitSet }
import org.scalacheck.{Shrink, Arbitrary, Gen}
import quasar.precog._, TestSupportWithArb._

class CodecSpec extends Specification with ScalaCheck {


  // READ ME READ ME READ ME
  // -----------------------
  // all of this stuff is basically obsolete, given the presence of scodec
  // (which didn't exist at the time).  Also I'm pretty sure these tests are
  // going to nondeterministically deadlock due to the blocking queue usage
  // within ByteBufferPool.  so they're just commented out for the time being
  // -----------------------
  // READ ME READ ME READ ME


  /*import ByteBufferPool._

  implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(
    Gen.chooseNum(Double.MinValue / 2, Double.MaxValue / 2) map (BigDecimal(_, java.math.MathContext.UNLIMITED)))

  //implicit def arbBitSet = Arbitrary(Gen.listOf(Gen.choose(0, 500)) map (BitSet(_: _*)))
  implicit def arbBitSet: Arbitrary[BitSet] = Arbitrary(Gen.listOf(Gen.choose(0, 500)) map BitSetUtil.create)

  implicit def arbSparseBitSet: Arbitrary[(Codec[BitSet], BitSet)] = {
    Arbitrary(Gen.chooseNum(0, 500) flatMap { size =>
      val codec = Codec.SparseBitSetCodec(size)
      if (size > 0) {
        Gen.listOf(Gen.choose(0, size - 1)) map { bits =>
          //(codec, BitSet(bits: _*))
          (codec, BitSetUtil.create(bits))
        }
      } else {
        Gen.const((codec, new BitSet()))
      }
    })
  }

  implicit def arbSparseRawBitSet: Arbitrary[(Codec[RawBitSet], RawBitSet)] = {
    Arbitrary(Gen.chooseNum(0, 500) flatMap { size =>
      val codec = Codec.SparseRawBitSetCodec(size)
      if (size > 0) {
        Gen.listOf(Gen.choose(0, size - 1)) map { bits =>
          //(codec, BitSet(bits: _*))
          val bs = RawBitSet.create(size)
          bits foreach { RawBitSet.set(bs, _) }
          (codec, bs)
        }
      } else {
        Gen.const((codec, RawBitSet.create(0)))
      }
    })
  }

  implicit def arbIndexedSeq[A](implicit a: Arbitrary[A]): Arbitrary[IndexedSeq[A]] =
    Arbitrary(Gen.listOf(a.arbitrary) map (Vector(_: _*)))

  implicit def arbArray[A: CTag: Gen]: Arbitrary[Array[A]] = Arbitrary(for {
    values <- Gen.listOf(implicitly[Gen[A]])
  } yield {
    val array: Array[A] = values.toArray
    array
  })

  val pool = new ByteBufferPool()
  val smallPool = new ByteBufferPool(capacity = 10)

  def surviveEasyRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val buf = pool.acquire
    codec.writeUnsafe(a, buf)
    buf.flip()
    codec.read(buf) must_== a
  }

  def surviveHardRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val bytes = smallPool.run(for {
      _ <- codec.write(a)
      bytes <- flipBytes
      _ <- release
    } yield bytes)
    bytes.length must_== codec.encodedSize(a)
    codec.read(ByteBufferWrap(bytes)) must_== a
  }

  "constant codec" should {
    "write 0 bytes" in {
      val codec = Codec.ConstCodec(true)
      codec.encodedSize(true) must_== 0
      codec.read(ByteBufferWrap(new Array[Byte](0))) must_== true
      codec.writeUnsafe(true, java.nio.ByteBuffer.allocate(0))
      ok
    }
  }

  def surviveRoundTrip[A](codec: Codec[A])(implicit a: Arbitrary[A], s: Shrink[A]) = "survive round-trip" in {
    "with large buffers" in { prop { (a: A) => surviveEasyRoundTrip(a)(codec) } }
    "with small buffers" in { prop { (a: A) => surviveHardRoundTrip(a)(codec) } }
  }

  "LongCodec" should surviveRoundTrip(Codec.LongCodec)
  "PackedLongCodec" should surviveRoundTrip(Codec.PackedLongCodec)
  "BooleanCodec" should surviveRoundTrip(Codec.BooleanCodec)
  "DoubleCodec" should surviveRoundTrip(Codec.DoubleCodec)
  "Utf8Codec" should surviveRoundTrip(Codec.Utf8Codec)
  "BigDecimalCodec" should surviveRoundTrip(Codec.BigDecimalCodec)(arbBigDecimal, implicitly)
  "BitSetCodec" should surviveRoundTrip(Codec.BitSetCodec)
  "SparseBitSet" should {
    "survive round-trip" in {
      "with large buffers" in {
        prop { (sparse: (Codec[BitSet], BitSet)) =>
          surviveEasyRoundTrip(sparse._2)(sparse._1)
        }
      }
      "with small buffers" in {
        prop { (sparse: (Codec[BitSet], BitSet)) =>
          surviveHardRoundTrip(sparse._2)(sparse._1)
        }
      }
    }
  }
  "SparseRawBitSet" should {
    "survive round-trip" in {
      "with large buffers" in {
        prop { (sparse: (Codec[RawBitSet], RawBitSet)) =>
          surviveEasyRoundTrip(sparse._2)(sparse._1)
        }
      }
      "with small buffers" in {
        prop { (sparse: (Codec[RawBitSet], RawBitSet)) =>
          surviveHardRoundTrip(sparse._2)(sparse._1)
        }
      }
    }
  }
  "IndexedSeqCodec" should {
    "survive round-trip" in {
      "with large buffers" in {
        prop { (xs: IndexedSeq[Long]) => surviveEasyRoundTrip(xs) }
        prop { (xs: IndexedSeq[IndexedSeq[Long]]) => surviveEasyRoundTrip(xs) }
        prop { (xs: IndexedSeq[String]) => surviveEasyRoundTrip(xs) }
      }
      "with small buffers" in {
        prop { (xs: IndexedSeq[Long]) => surviveHardRoundTrip(xs) }
        prop { (xs: IndexedSeq[IndexedSeq[Long]]) => surviveHardRoundTrip(xs) }
        prop { (xs: IndexedSeq[String]) => surviveHardRoundTrip(xs) }
      }
    }
  }
  "ArrayCodec" should {
    "survive round-trip" in {
      "with large buffers" in {
        prop { (xs: Array[Long]) => surviveEasyRoundTrip(xs) }
        prop { (xs: Array[Array[Long]]) => surviveEasyRoundTrip(xs) }
        prop { (xs: Array[String]) => surviveEasyRoundTrip(xs) }
      }
      "with small buffers" in {
        prop { (xs: Array[Long]) => surviveHardRoundTrip(xs) }
        prop { (xs: Array[Array[Long]]) => surviveHardRoundTrip(xs) }
        prop { (xs: Array[String]) => surviveHardRoundTrip(xs) }
      }
    }
  }
  // "CValueCodec" should surviveRoundTrip(Codec.CValueCodec)*/
}
