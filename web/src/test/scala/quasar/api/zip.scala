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
import scala.Predef.$conforms
import quasar.fp.numeric._
import quasar.fp.ski._
import quasar.contrib.pathy.sandboxCurrent

import org.scalacheck.Arbitrary

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream.Process

import scodec.bits._
import scodec.interop.scalaz._

import pathy.Path._
import quasar.fp.numeric._
import pathy.scalacheck.PathyArbitrary._

import eu.timepit.refined.numeric.{Positive => RPositive, NonNegative}
import eu.timepit.refined.scalacheck.numeric._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.auto._

class ZipSpecs extends quasar.Qspec {
  import Zip._

  // Provide instances of Arbitrary Positive and Natural that allow the computation to complete in a reasonable
  // amount of time and without Java heap space errors.
  implicit val reasonablePositive: Arbitrary[Positive] =
    Arbitrary(chooseRefinedNum[Refined, Long, RPositive](1L, 1000L))
  implicit val reasonableNatural: Arbitrary[Natural] =
    Arbitrary(chooseRefinedNum[Refined, Long, NonNegative](0L, 1000L))

  def randBytes(size: Int): Task[Array[Byte]] = Task.delay {
    val rand = new java.util.Random
    Array.fill[Byte](size)(rand.nextInt.toByte)
  }

  /** A large, highly compressible stream of bytes: 1000 copies of the same
    * 1000 random bytes, which can be read multiple times, but a different
    * stream of bytes each time the outer task is run. */
  def f4: Task[Process[Task, ByteVector]] =
    randBytes(1000).map(bs => Process.emit(ByteVector.view(bs)).repeat.take(1000))

  def unzip[A](f: java.io.InputStream => A)(p: Process[Task, ByteVector]): Task[Map[RelFile[Sandboxed], A]] =
    Task.delay {
      val bytes = p.runLog.unsafePerformSync.toList.concatenate  // FIXME: this means we can't use this to test anything big
      val is = new java.io.ByteArrayInputStream(bytes.toArray)
      val zis = new java.util.zip.ZipInputStream(is)
      Stream.continually(zis.getNextEntry).takeWhile(_ != null).map { entry =>
        sandboxCurrent(posixCodec.parseRelFile(entry.getName).get).get -> f(zis)
      }.toMap
    }

  // For testing, capture all the bytes from a process, parse them with a
  // ZipInputStream, and capture just the size of the contents of each file.
  def counts(p: Process[Task, ByteVector]): Task[Map[RelFile[Sandboxed], Int]] = {
    def count(is: java.io.InputStream): Int =
      Stream.continually(is.read).takeWhile(_ != -1).size
    unzip(count)(p)
  }

  def bytes(p: Process[Task, ByteVector]): Task[Map[RelFile[Sandboxed], ByteVector]] = {
    def read(is: java.io.InputStream): ByteVector = {
      def loop(acc: ByteVector): ByteVector = {
        val buffer = new Array[Byte](16*1024)
        val n = is.read(buffer)
        if (n <= 0) acc else loop(acc ++ ByteVector.view(buffer).take(n.toLong))
      }
      loop(ByteVector.empty)
    }
    unzip(read)(p)
  }

  def bytesMapping(filesAndSize: Map[RelFile[Sandboxed], Positive])
      : Task[Map[RelFile[Sandboxed], Process[Task, ByteVector]]] = {
    def byteStream(size: Positive): Task[Process[Task, ByteVector]] =
      randBytes(size.toInt).map(bytes => Process.emit(ByteVector.view(bytes)))
    filesAndSize.toList.traverse { case (k, v) => byteStream(v).strengthL(k) }.map(_.toMap)
  }

  "zipFiles" should {
    "zip files of constant bytes" >> prop { (filesAndSize: Map[RelFile[Sandboxed], Positive], byte: Byte) =>
      val filesAndBytes = bytesMapping(filesAndSize).unsafePerformSync
      val z = zipFiles(filesAndBytes)
      counts(z).unsafePerformSync must_=== filesAndSize.mapValues(_.value.toInt)
    }.set(minTestsOk = 10) // This test is relatively slow

    "zip files of random bytes" >> prop { filesAndSize: Map[RelFile[Sandboxed], Positive] =>
      val filesAndBytes = bytesMapping(filesAndSize).unsafePerformSync
      val z = zipFiles(filesAndBytes)
      counts(z).unsafePerformSync must_=== filesAndSize.mapValues(_.value.toInt)
    }.set(minTestsOk = 10) // This test is relatively slow

    "zip many large files of random bytes (100 MB)" in {
      // NB: this is mainly a performance check. Right now it's about 2 seconds for 100 MB for me.
      val Files = 100
      val MaxExpectedSize = 1000L*1000L
      val MinExpectedSize = MaxExpectedSize / 2

      val paths = (0 until Files).toList.map(i => currentDir[Sandboxed] </> file("foo" + i))
      val z = zipFiles(paths.strengthR(f4.unsafePerformSync).toMap)

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.unsafePerformSync(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    // NB: un-skip this to verify the heap is not consumed. It's too slow to run
    // every time (~2 minutes).
    "zip many large files of random bytes (10 GB)" in skipped {
      val Files = 10*1000
      val MaxExpectedSize = 100L*1000L*1000L
      val MinExpectedSize = MaxExpectedSize / 2

      val paths = (0 until Files).toList.map(i => currentDir[Sandboxed] </> file("foo" + i))
      val z = zipFiles(paths.strengthR(f4.unsafePerformSync).toMap)

      // NB: can't use my naive `list` function on a large file
      z.map(_.size).sum.runLog.unsafePerformSync(0) must beBetween(MinExpectedSize, MaxExpectedSize)
    }

    "read twice without conflict" >> prop { filesAndSize: Map[RelFile[Sandboxed], Positive] =>
      val filesAndBytes = bytesMapping(filesAndSize).unsafePerformSync
      val z = zipFiles(filesAndBytes)
      bytes(z).unsafePerformSync.toList must equal(bytes(z).unsafePerformSync.toList)
    }.set(minTestsOk = 10) // This test is relatively slow
  }

  "unzipFiles" should {
    "recover any input" >> prop { filesAndSize: Map[RelFile[Sandboxed], Positive] =>
      val filesAndBytes = bytesMapping(filesAndSize).unsafePerformSync
      val z = zipFiles(filesAndBytes)

      val exp = filesAndBytes.mapValues(_.runFoldMap(ι).unsafePerformSync)
      unzipFiles(z).map(_.toMap).run.unsafePerformSync must beRightDisjunction(exp)
    }.set(minTestsOk = 10) // This test is relatively slow
  }
}
