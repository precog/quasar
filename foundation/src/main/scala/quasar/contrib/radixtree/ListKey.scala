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

package quasar.contrib.radixtree

import slamdata.Predef._

import scala.annotation.tailrec

import scalaz._, Scalaz._
import shims._
import com.rklaehn.radixtree.{Hash, RadixTree}


final class ListKey[K: algebra.Order: Hash: scala.reflect.ClassTag] extends RadixTree.Key[List[K]] {
  def size(c: List[K]) = c.length
  val empty = List.empty[K]
  def intern(e: List[K]) = e
  def concat(a: List[K], b: List[K]): List[K] = a ++ b
  def slice(a: List[K], from: Int, until: Int) = a.slice(from, until)
  def compareAt(a: List[K], ai: Int, b: List[K], bi: Int) = algebra.Order.compare(a(ai), b(bi))
  @tailrec
  def indexOfFirstDifference(a: List[K], ai: Int, b: List[K], bi: Int, cnt: Int) = {
    if (cnt === 0 || algebra.Order.neqv(a(ai),b(bi))) ai
    else indexOfFirstDifference(a, ai + 1, b, bi + 1, cnt - 1)
  }
  def eqv(x: List[K], y: List[K]): Boolean = x === y
  def hash(e: List[K]): Int = e.hashCode
}

object ListKey {
  implicit def listKey[K: algebra.Order: Hash: scala.reflect.ClassTag]: RadixTree.Key[List[K]] =
    new ListKey
}
