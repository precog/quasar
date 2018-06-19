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

package quasar.precog.util

import quasar.time.OffsetDate

import org.slf4s.Logging

import scalaz._
import scalaz.effect.IO

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.{ collection => sc }
import scala.reflect.ClassTag

import java.io.File
import java.nio.file.Files
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.Arrays.fill

object IOUtils extends Logging {

  def overwriteFile(s: String, f: File): IO[Unit] = writeToFile(s, f, append = false)

  def writeToFile(s: String, f: File, append: Boolean): IO[Unit] = IO {
    Files.write(f.toPath, s.getBytes)
  }

  /** Performs a safe write to the file. Returns true
    * if the file was completely written, false otherwise
    */
  def safeWriteToFile(s: String, f: File): IO[Boolean] = {
    val tmpFile = new File(f.getParentFile, f.getName + "-" + System.nanoTime + ".tmp")

    overwriteFile(s, tmpFile) flatMap { _ =>
      IO(tmpFile.renameTo(f)) // TODO: This is only atomic on POSIX systems
    }
  }

  def recursiveDelete(files: Seq[File]): IO[Unit] = {
    files.toList match {
      case Nil      => IO(())
      case hd :: tl => recursiveDelete(hd).flatMap(_ => recursiveDelete(tl))
    }
  }

  def listFiles(f: File): IO[Array[File]] = IO {
    f.listFiles match {
      case null => Array()
      case xs   => xs
    }
  }

  /** Deletes `file`, recursively if it is a directory. */
  def recursiveDelete(file: File): IO[Unit] = {
    def del(): Unit = { file.delete(); () }

    if (!file.isDirectory) IO(del())
    else listFiles(file) flatMap {
      case Array() => IO(del())
      case xs      => recursiveDelete(xs).map(_ => del())
    }
  }

  /** Recursively deletes empty directories, stopping at the first
    * non-empty dir.
    */
  def recursiveDeleteEmptyDirs(startDir: File, upTo: File): IO[Unit] = {
    if (startDir == upTo) {
      IO { log.debug("Stopping recursive clean at root: " + upTo) }
    } else if (startDir.isDirectory) {
      if (Option(startDir.list).exists(_.length == 0)) {
        IO {
          startDir.delete()
        }.flatMap { _ =>
          recursiveDeleteEmptyDirs(startDir.getParentFile, upTo)
        }
      } else {
        IO { log.debug("Stopping recursive clean on non-empty directory: " + startDir) }
      }
    } else {
      IO { log.warn("Asked to clean a non-directory: " + startDir) }
    }
  }

  def createTmpDir(prefix: String): IO[File] = IO {
    Files.createTempDirectory(prefix).toFile
  }
}

/**
  * Implicit container trait
  */
trait MapUtils {
  implicit def mapUtils[A, B, CC[B] <: sc.GenTraversable[B]](self: sc.GenMap[A, CC[B]]): MapExtras[A, B, CC] =
    new MapExtras(self)
}

class MapExtras[A, B, CC[B] <: sc.GenTraversable[B]](left: sc.GenMap[A, CC[B]]) {
  def cogroup[C, CC2[C] <: sc.GenTraversable[C], Result](right: sc.GenMap[A, CC2[C]])(
      implicit cbf: CanBuildFrom[Nothing, (A, Either3[B, (CC[B], CC2[C]), C]), Result]): Result = {
    val resultBuilder = cbf()

    left foreach {
      case (key, leftValues) => {
        right get key map { rightValues =>
          val _ =
            resultBuilder += (key -> Either3.middle3[B, (CC[B], CC2[C]), C]((leftValues, rightValues)))
        } getOrElse {
          leftValues foreach { b =>
            resultBuilder += (key -> Either3.left3[B, (CC[B], CC2[C]), C](b))
          }
        }
      }
    }

    right foreach {
      case (key, rightValues) => {
        if (!(left get key isDefined)) {
          rightValues foreach { c =>
            resultBuilder += (key -> Either3.right3[B, (CC[B], CC2[C]), C](c))
          }
        }
      }
    }

    resultBuilder.result()
  }
}



object NumericComparisons {

  @inline def compare(a: Long, b: Long): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Long, b: Double): Int = -compare(b, a)

  @inline def compare(a: Long, b: BigDecimal): Int = BigDecimal(a) compare b

  def compare(a: Double, bl: Long): Int = {
    val b = bl.toDouble
    if (b.toLong == bl) {
      if (a < b) -1 else if (a == b) 0 else 1
    } else {
      val error = math.abs(b * 2.220446049250313E-16)
      if (a < b - error) -1 else if (a > b + error) 1 else bl.signum
    }
  }

  @inline def compare(a: Double, b: Double): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Double, b: BigDecimal): Int = BigDecimal(a) compare b

  @inline def compare(a: BigDecimal, b: Long): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: Double): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: BigDecimal): Int = a compare b

  @inline def compare(a: OffsetDateTime, b: OffsetDateTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: OffsetTime, b: OffsetTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: OffsetDate, b: OffsetDate): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalDateTime, b: LocalDateTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalTime, b: LocalTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalDate, b: LocalDate): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }


  @inline def eps(b: Double): Double = math.abs(b * 2.220446049250313E-16)

  def approxCompare(a: Double, b: Double): Int = {
    val aError = eps(a)
    val bError = eps(b)
    if (a + aError < b - bError) -1 else if (a - aError > b + bError) 1 else 0
  }

  import scalaz.Ordering.{ LT, GT, EQ }

  @inline def order(a: Long, b: Long): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Double, b: Double): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Long, b: Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Long, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalDateTime, b: LocalDateTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalDate, b: LocalDate): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalTime, b: LocalTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetDateTime, b: OffsetDateTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetDate, b: OffsetDate): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetTime, b: OffsetTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))
}

object RawBitSet {
  final def create(size: Int): Array[Int] = new Array[Int]((size >>> 5) + 1)

  final def get(bits: Array[Int], i: Int): Boolean = {
    val pos = i >>> 5
    if (pos < bits.length) {
      (bits(pos) & (1 << (i & 0x1F))) != 0
    } else {
      false
    }
  }

  final def set(bits: Array[Int], i: Int) {
    val pos = i >>> 5
    if (pos < bits.length) {
      bits(pos) |= (1 << (i & 0x1F))
    } else {
      throw new IndexOutOfBoundsException("Bit %d is out of range." format i)
    }
  }

  final def clear(bits: Array[Int], i: Int) {
    val pos = i >>> 5
    if (pos < bits.length) {
      bits(pos) &= ~(1 << (i & 0x1F))
    }
  }

  final def clear(bits: Array[Int]) = fill(bits, 0)

  final def toArray(bits: Array[Int]): Array[Int] = {
    var n = 0
    var i = 0
    val len = bits.length
    while (i < len) {
      n += java.lang.Integer.bitCount(bits(i))
      i += 1
    }

    val ints = new Array[Int](n)

    @inline
    @tailrec
    def loopInts(bitsIndex: Int, intsIndex: Int) {
      if (bitsIndex < len) {
        loopInts(bitsIndex + 1, loopBits(bits(bitsIndex), 0, 0, intsIndex))
      }
    }

    @inline
    @tailrec
    def loopBits(bits: Int, shift: Int, value: Int, intsIndex: Int): Int = {
      if (((bits >> shift) & 1) == 1) {
        ints(intsIndex) = value
        if (shift < 31) loopBits(bits, shift + 1, value + 1, intsIndex + 1)
        else intsIndex
      } else {
        if (shift < 31) loopBits(bits, shift + 1, value + 1, intsIndex)
        else intsIndex
      }
    }

    loopInts(0, 0)
    ints
  }

  final def toList(bits: Array[Int]): List[Int] = {

    @inline
    @tailrec
    def rec0(n: Int, hi: Int, lo: Int, bs: List[Int]): List[Int] = {
      if (lo >= 0) {
        if ((n & (1 << lo)) != 0) {
          rec0(n, hi, lo - 1, (hi | lo) :: bs)
        } else {
          rec0(n, hi, lo - 1, bs)
        }
      } else {
        bs
      }
    }

    @inline
    @tailrec
    def rec(i: Int, bs: List[Int]): List[Int] = {
      if (i >= 0) {
        rec(i - 1, rec0(bits(i), i << 5, 31, bs))
      } else {
        bs
      }
    }

    rec(bits.length - 1, Nil)
  }
}



/**
  * Unchecked and unboxed (fast!) deque implementation with a fixed bound.  None
  * of the operations on this datastructure are checked for bounds.  You are
  * trusted to get this right on your own.  If you do something weird, you could
  * end up overwriting data, reading old results, etc.  Don't do that.
  *
  * No objects were allocated in the making of this film.
  */
final class RingDeque[@specialized(Boolean, Int, Long, Double, Float, Short) A: ClassTag](_bound: Int) {
  val bound = _bound + 1

  private val ring = new Array[A](bound)
  private var front = 0
  private var back  = rotate(front, 1)

  def isEmpty = front == rotate(back, -1)

  def empty() {
    back = rotate(front, 1)
  }

  def popFront(): A = {
    val result = ring(front)
    moveFront(1)
    result
  }

  def pushFront(a: A) {
    moveFront(-1)
    ring(front) = a
  }

  def popBack(): A = {
    moveBack(-1)
    ring(rotate(back, -1))
  }

  def pushBack(a: A) {
    ring(rotate(back, -1)) = a
    moveBack(1)
  }

  def removeBack(length: Int) {
    moveBack(-length)
  }

  def length: Int =
    (if (back > front) back - front else (back + bound) - front) - 1

  def toList: List[A] = {
    @inline
    @tailrec
    def buildList(i: Int, accum: List[A]): List[A] =
      if (i < front)
        accum
      else
        buildList(i - 1, ring(i % bound) :: accum)

    buildList(front + length - 1, Nil)
  }

  @inline
  private[this] def rotate(target: Int, delta: Int) =
    (target + delta + bound) % bound

  @inline
  private[this] def moveFront(delta: Int) {
    front = rotate(front, delta)
  }

  @inline
  private[this] def moveBack(delta: Int) {
    back = rotate(back, delta)
  }
}
