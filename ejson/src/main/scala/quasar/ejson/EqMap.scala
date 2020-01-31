/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.ejson

import slamdata.Predef._
import quasar.contrib.scalaz.foldable._

import scalaz._, Scalaz._

/** A map from keys of type `K` to values of type `V` that relies on key
  * equality to implement operations.
  *
  * As a result, more types are permitted as keys than for maps relying
  * on an ordering, however, operations on `EqMap` are O(n).
  */
final class EqMap[K, V] private (val toList: List[(K, V)]) {
  def lookup(k: K)(implicit K: Equal[K]): Option[V] =
    toList find (_._1 === k) map (_._2)

  def insert(k: K, v: V)(implicit K: Equal[K]): EqMap[K, V] = {
    def upsert(z: Zipper[(K, V)]): List[(K, V)] =
      z.findZ(_._1 === k).cata(
        _.modify(_ as v).toList,
        (k, v) :: toList)

    toList.toZipper.cata(
      z => new EqMap(upsert(z)),
      EqMap.singleton(k, v))
  }

  def + (kv: (K, V))(implicit K: Equal[K]): EqMap[K, V] =
    insert(kv._1, kv._2)

  def delete(k: K)(implicit K: Equal[K]): EqMap[K, V] =
    new EqMap(toList filterNot (_._1 === k))

  def - (k: K)(implicit K: Equal[K]): EqMap[K, V] =
    delete(k)
}

object EqMap extends EqMapInstances {
  def empty[K, V]: EqMap[K, V] =
    new EqMap(List())

  def fromFoldable[F[_]: Foldable, K: Equal, V](fab: F[(K, V)]): EqMap[K, V] =
    fab.foldLeft(empty[K, V])(_ + _)

  def singleton[K, V](k: K, v: V): EqMap[K, V] =
    new EqMap(List((k, v)))
}

sealed abstract class EqMapInstances {
  implicit def equal[A: Equal, B: Equal]: Equal[EqMap[A, B]] =
    Equal.equal { (x, y) =>
      x.toList equalsAsSets y.toList
    }

  implicit def show[A: Show, B: Show]: Show[EqMap[A, B]] =
    Show.shows(m => "EqMap" + m.toList.shows)
}
