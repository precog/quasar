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

package quasar

import quasar.precog.BitSet
import quasar.precog.util._

import scalaz._

import scala.concurrent.ExecutionContext
import scala.math.BigDecimal

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.UUID

package object blueeyes extends precog.PackageTime {
  type ByteBufferPoolS[A] = State[(ByteBufferPool, List[ByteBuffer]), A]

  def Utf8Charset: Charset                                               = Charset forName "UTF-8"
  def uuid(s: String): UUID                                              = UUID fromString s
  def randomUuid(): UUID                                                 = UUID.randomUUID
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                        = ByteBuffer.wrap(xs)
  def ByteBufferWrap(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                        = throw new RuntimeException(msg)
  def decimal(d: String): BigDecimal                                     = BigDecimal(d, java.math.MathContext.UNLIMITED)

  implicit val GlobalEC: ExecutionContext = scala.concurrent.ExecutionContext.global

  implicit def comparableOrder[A <: Comparable[A]] : scalaz.Order[A] =
    scalaz.Order.order[A]((x, y) => scalaz.Ordering.fromInt(x compareTo y))

  @inline implicit def ValidationFlatMapRequested[E, A](d: scalaz.Validation[E, A]): scalaz.ValidationFlatMap[E, A] =
    scalaz.Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  implicit def bigDecimalOrder: scalaz.Order[BigDecimal] =
    scalaz.Order.order((x, y) => Ordering.fromInt(x compare y))

  implicit class ScalaSeqOps[A](xs: scala.collection.Seq[A]) {
    def sortMe(implicit z: scalaz.Order[A]): Vector[A] =
      xs sortWith ((a, b) => z.order(a, b) == Ordering.LT) toVector
  }

  def arrayEq[@specialized A](a1: Array[A], a2: Array[A]): Boolean = {
    val len = a1.length
    if (len != a2.length) return false
    var i = 0
    while (i < len) {
      if (a1(i) != a2(i)) return false
      i += 1
    }
    true
  }

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B = f(x)
  }

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  implicit def bitSetOps(bs: BitSet): BitSetUtil.BitSetOperations = new BitSetUtil.BitSetOperations(bs)
}
