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

package quasar.impl.storage

import slamdata.Predef._

import cats.effect.{IO, ContextShift, Timer}
import cats.effect.concurrent.{Ref, Deferred}
import fs2.Stream
import scalaz.{IMap, Order}

import scala.concurrent.ExecutionContext

/** An indexed store backed by a map held in a Ref, for testing */
object RefIndexedStore {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  def apply[I: Order, V](ref: Ref[IO, IMap[I, V]])
      : IndexedStore[IO, I, V] =
    new IndexedStore[IO, I, V] {
      val entries =
        Stream.eval(ref.get).flatMap(m => Stream.emits(m.toList))

      def lookup(i: I) =
        ref.get.map(_.lookup(i))

      def insert(i: I, v: V) = for {
        _ <- ref.update(_.insert(i, v))
        d <- Deferred.tryable[IO, Unit]
        _ <- d.complete(())
      } yield d

      def delete(i: I) =
        for {
          m <- ref.get

          r = m.updateLookupWithKey(i, (_, _) => None)
          (old, m2) = r

          _ <- ref.set(m2)
          d <- Deferred.tryable[IO, Boolean]
          _ <- d.complete(old.isDefined)
        } yield d
    }
}
