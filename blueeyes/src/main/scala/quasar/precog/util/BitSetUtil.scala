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

package quasar.precog.util

import quasar.blueeyes._

object BitSetUtil {
  class BitSetOperations(private val bs: BitSet) extends AnyVal {
    def toList(): List[Int] = bitSetToList(bs)

    def +(elem: Int)      = { val b = bs.copy; b.set(elem); b }
    def -(elem: Int)      = { val b = bs.copy; b.clear(elem); b }
    def &(other: BitSet)  = { val b = bs.copy; b.and(other); b }
    def &~(other: BitSet) = { val b = bs.copy; b.andNot(other); b }
    def |(other: BitSet)  = { val b = bs.copy; b.or(other); b }
    def ++(other: BitSet) = { val b = bs.copy; b.or(other); b }

    def +=(elem: Int)      = bs.set(elem)
    def -=(elem: Int)      = bs.clear(elem)
    def &=(other: BitSet)  = bs.and(other)
    def &~=(other: BitSet) = bs.andNot(other)
    def |=(other: BitSet)  = bs.or(other)
    def ++=(other: BitSet) = bs.or(other)

    def isEmpty(): Boolean =
      bs.nextSetBit(0) < 0

    def min(): Int = {
      val n = bs.nextSetBit(0)
      if (n < 0) sys.error("can't take min of empty set") else n
    }

    def max(): Int = {
      @tailrec
      def findBit(i: Int, last: Int): Int = {
        val j = bs.nextSetBit(i)
        if (j < 0) last else findBit(j + 1, j)
      }

      val ns = bs.getBits
      var i = ns.length - 1
      while (i >= 0) {
        if (ns(i) != 0) return findBit(i * 64, -1)
        i -= 1
      }
      sys.error("can't find max of empty set")
    }

    def foreach(f: Int => Unit) = {
      var b = bs.nextSetBit(0)
      while (b >= 0) {
        f(b)
        b = bs.nextSetBit(b + 1)
      }
    }
  }

  object Implicits {
    implicit def toOperations(bs: BitSet) = new BitSetOperations(bs)
  }

  def fromArray(arr: Array[Long]) = {
    val bs = new BitSet()
    bs.setBits(arr)
    bs
  }

  def create(): BitSet = new BitSet()

  def create(ns: Array[Int]): BitSet = {
    val bs = new BitSet()
    var i = 0
    val len = ns.length
    while (i < len) {
      bs.set(ns(i))
      i += 1
    }
    bs
  }

  def create(ns: Seq[Int]): BitSet = {
    val bs = new BitSet()
    ns.foreach(n => bs.set(n))
    bs
  }

  def range(start: Int, end: Int): BitSet = {
    val bs = new BitSet()
    Loop.range(start, end)(i => bs.set(i))
    bs
  }

  def takeRange(from: Int, to: Int)(bitset: BitSet): BitSet = {
    val len = bitset.length
    if (from <= 0) {
      val bits = bitset.copy()
      if (to >= len) bits
      else {
        bits.clear(to, len)
        bits
      }
    } else {
      var i = from
      val bits = new BitSet()
      while (i < to) {
        bits(i - from) = bitset(i)
        i += 1
      }
      bits
    }
  }

  def filteredRange(start: Int, end: Int)(pred: Int => Boolean): BitSet = {
    val bs = new BitSet()
    Loop.range(start, end)(i => if (pred(i)) bs.set(i))
    bs
  }

  @inline final def filteredRange(r: Range)(pred: Int => Boolean): BitSet =
    filteredRange(r.start, r.end)(pred)

  def filteredSeq[A](as: List[A])(pred: A => Boolean): BitSet = {
    val bs = new BitSet
    @inline
    @tailrec def loop(lst: List[A], i: Int): Unit = lst match {
      case h :: t =>
        if (pred(h)) bs.set(i)
        loop(t, i + 1)
      case Nil =>
    }
    loop(as, 0)
    bs
  }

  def bitSetToList(bs: BitSet): List[Int] = {
    @tailrec
    def loopBits(long: Long, bit: Int, base: Int, sofar: List[Int]): List[Int] = {
      if (bit < 0)
        sofar
      else if (((long >> bit) & 1) == 1)
        loopBits(long, bit - 1, base, (base + bit) :: sofar)
      else
        loopBits(long, bit - 1, base, sofar)
    }

    @tailrec
    def loopLongs(i: Int, longs: Array[Long], base: Int, sofar: List[Int]): List[Int] = {
      if (i < 0)
        sofar
      else
        loopLongs(i - 1, longs, base - 64, loopBits(longs(i), 63, base, sofar))
    }

    val last = bs.getBitsLength - 1
    loopLongs(last, bs.getBits, last * 64, Nil)
  }
}
