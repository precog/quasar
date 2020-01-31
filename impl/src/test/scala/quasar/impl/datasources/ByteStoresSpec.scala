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

package quasar.impl.datasources

import quasar.EffectfulQSpec

import cats.Eq
import cats.effect.{Effect, Resource}
import cats.implicits._

import scala.concurrent.ExecutionContext

import scala.{Array, Byte}

abstract class ByteStoresSpec[F[_]: Effect, K: Eq](implicit ec: ExecutionContext)
    extends EffectfulQSpec[F] {

  def byteStores: Resource[F, ByteStores[F, K]]

  val k1: K
  val k2: K

  "keys are distinct" >> {
    k1 =!= k2
  }

  "get" >> {
    "returns the same store for the same key" >>* {
      byteStores use { bs =>
        for {
          s0 <- bs.get(k1)

          _ <- s0.insert("foo", Array[Byte](1, 2, 3))
          _ <- s0.insert("bar", Array[Byte](5, 6, 7))

          s1 <- bs.get(k1)

          foo0 <- s0.lookup("foo")
          bar0 <- s0.lookup("bar")

          foo1 <- s1.lookup("foo")
          bar1 <- s1.lookup("bar")

          _ <- s1.delete("bar")

          bar2 <- s0.lookup("bar")
          bar3 <- s1.lookup("bar")
        } yield {
          foo0 must beSome(Array[Byte](1, 2, 3))
          foo1 must beSome(Array[Byte](1, 2, 3))

          bar0 must beSome(Array[Byte](5, 6, 7))
          bar1 must beSome(Array[Byte](5, 6, 7))

          bar2 must beNone
          bar3 must beNone
        }
      }
    }
  }

  "clear" >> {
    "removes all associations from the store for the specified key" >>* {
      byteStores use { bs =>
        for {
          s0 <- bs.get(k1)

          _ <- s0.insert("foo", Array[Byte](1, 2, 3))

          s1 <- bs.get(k1)

          _ <- bs.clear(k1)

          r0 <- s0.lookup("foo")
          r1 <- s1.lookup("foo")
        } yield {
          r0 must beNone
          r1 must beNone
        }
      }
    }

    "doesn't affect stores for other keys" >>* {
      byteStores use { bs =>
        for {
          s1 <- bs.get(k1)
          s2 <- bs.get(k2)

          _ <- s1.insert("foo", Array[Byte](1, 2, 3))
          _ <- s2.insert("quux", Array[Byte](2, 4, 6))

          _ <- bs.clear(k1)

          r1 <- s1.lookup("foo")
          r2 <- s2.lookup("quux")
        } yield {
          r1 must beNone
          r2 must beSome(Array[Byte](2, 4, 6))
        }
      }
    }
  }
}
