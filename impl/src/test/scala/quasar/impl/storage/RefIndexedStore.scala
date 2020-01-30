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

package quasar.impl.storage

import slamdata.Predef.None

import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.Stream
import scalaz.{IMap, Order}

/** An indexed store backed by a map held in a Ref, for testing */
object RefIndexedStore {
  def apply[I: Order, V](ref: Ref[IO, IMap[I, V]])
      : IndexedStore[IO, I, V] =
    new IndexedStore[IO, I, V] {
      val entries =
        Stream.eval(ref.get).flatMap(m => Stream.emits(m.toList))

      def lookup(i: I) =
        ref.get.map(_.lookup(i))

      def insert(i: I, v: V) =
        ref.update(_.insert(i, v))

      def delete(i: I) =
        for {
          m <- ref.get

          r = m.updateLookupWithKey(i, (_, _) => None)
          (old, m2) = r

          _ <- ref.set(m2)
        } yield old.isDefined
    }
}
