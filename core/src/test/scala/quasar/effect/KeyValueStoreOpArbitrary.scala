/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.effect

import quasar.Predef._
import quasar.SKI._

import java.lang.Integer

import org.scalacheck.{Arbitrary, Gen}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

trait KeyValueStoreOpArbitrary {
  import KeyValueStoreOpArbitrary._

  implicit def keyValueStoreOpArbitrary[K: Arbitrary, V: Arbitrary]: Arbitrary[Free[KeyValueStore[K, V, ?], Unit]] =
    Arbitrary(Gen.oneOf(
      genPut[K, V],
      genUpdate[K, V],
      genAlter[K, V],
      genDelete[K, V],
      genCAA[K, V]))
}

object KeyValueStoreOpArbitrary extends KeyValueStoreOpArbitrary {
  import Arbitrary.arbitrary

  private val genIndex: Gen[Int] =
    Gen.chooseNum[Int](0, Integer.MAX_VALUE)

  private def genPut[K: Arbitrary, V: Arbitrary]: Gen[Free[KeyValueStore[K, V, ?], Unit]] =
    (arbitrary[K] |@| arbitrary[V])(kvs[K, V].put(_, _))

  private def genUpdate[K, V: Arbitrary]: Gen[Free[KeyValueStore[K, V, ?], Unit]] =
    (arbitrary[V] |@| genIndex) { (newV, idx) =>
      key[K, V](idx).flatMapF(kvs[K, V].put(_, newV)).run.void
    }

  private def genAlter[K, V: Arbitrary]: Gen[Free[KeyValueStore[K, V, ?], Unit]] =
    (arbitrary[Option[V]] |@| genIndex) { (updV, idx) =>
      key[K, V](idx).flatMap(kvs[K, V].alter(_, κ(updV))).run.void
    }

  private def genDelete[K, V]: Gen[Free[KeyValueStore[K, V, ?], Unit]] =
    genIndex map { idx =>
      key[K, V](idx).flatMapF(kvs[K, V].delete(_)).run.void
    }

  private def genCAA[K: Arbitrary, V: Arbitrary]: Gen[Free[KeyValueStore[K, V, ?], Unit]] = {
    type F[A] = Free[KeyValueStore[K, V, ?], A]
    (
      arbitrary[Boolean]   |@|
      genIndex             |@|
      arbitrary[Option[K]] |@|
      arbitrary[Option[V]]
    ) { (isSame, idx, maybeK, updV) =>
      maybeK.fold(key[K, V](idx))(OptionT.some[F, K](_)).flatMapF { k =>
        for {
          v <- kvs[K, V].get(k).run
          _ <- kvs[K, V].compareAndAlter(k, isSame ? v | none, updV)
        } yield ()
      }.run.void
    }
  }

  private def key[K, V](idx: Int): OptionT[Free[KeyValueStore[K, V, ?], ?], K] =
    OptionT(kvs[K, V].keys map (ks => ks.nonEmpty option ks(idx % ks.length)))

  private def kvs[K, V] = KeyValueStore.Ops[K, V, KeyValueStore[K, V, ?]]
}
