/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{List, Some}
import quasar.EffectfulQSpec

import scala.concurrent.ExecutionContext

import cats.effect.{Effect, Resource}
import cats.syntax.functor._
import cats.syntax.flatMap._
import scalaz.{Equal, Show}
import scalaz.std.option._

abstract class IndexedStoreSpec[F[_]: Effect, I: Equal: Show, V: Equal: Show](
    implicit ec: ExecutionContext)
    extends EffectfulQSpec[F] {

  // Must not contain any entries.
  def emptyStore: Resource[F, IndexedStore[F, I, V]]

  // Must return distinct values
  def freshIndex: F[I]

  def valueA: V

  def valueB: V

  "indexed store" >> {
    "entries" >> {
      "empty when store is empty" >>* {
        emptyStore use { store =>
          store.entries.compile.last
          .map(_ must beNone)
        }
      }
    }

    "lookup" >> {
      "returns none when store is empty" >>* {
        emptyStore use { store => for {
          i <- freshIndex
          v <- store.lookup(i)
        } yield {
          v must beNone
        }
      }}
    }

    "insert" >> {
      "lookup returns inserted value" >>* {
        emptyStore use { store => for {
          i <- freshIndex
          _ <- store.insert(i, valueA)
          v <- store.lookup(i)
        } yield {
          v must_= Some(valueA)
        }}
      }

      "lookup returns replaced value" >>* {
        emptyStore use { store => for {
          i <- freshIndex
          _ <- store.insert(i, valueA)
          _ <- store.insert(i, valueB)
          v <- store.lookup(i)
        } yield {
          v must_= Some(valueB)
        }}
      }

      "entries returns inserted values" >>* {
        emptyStore use { store => for {

          ia <- freshIndex
          _ <- store.insert(ia, valueA)

          ib <- freshIndex
          _ <- store.insert(ib, valueB)

          es <- store.entries.compile.toList

          vs = List((ia, valueA), (ib, valueB))
        } yield {
          es must containTheSameElementsAs(vs)
        }}
      }
    }
    "remove" >> {

      "false when key does not exist" >>* {
        emptyStore use { store => for {
          i <- freshIndex
          d <- store.delete(i)
        } yield d must beFalse
      }}


      "lookup no longer returns value" >>* {
        emptyStore use { store => for {
          i <- freshIndex
          _ <- store.insert(i, valueA)
          vBefore <- store.lookup(i)
          d <- store.delete(i)
          vAfter <- store.lookup(i)
        } yield {
          vBefore must_= Some(valueA)
          d must beTrue
          vAfter must beNone
        }}
      }

      "entries no longer includes entry" >>* {
        emptyStore use { store => for {
          ia <- freshIndex
          _ <- store.insert(ia, valueA)

          ib <- freshIndex
          _ <- store.insert(ib, valueB)

          esBefore <- store.entries.compile.toList

          d <- store.delete(ia)

          esAfter <- store.entries.compile.toList

          vs = List((ia, valueA), (ib, valueB))
        } yield {
          esBefore must containTheSameElementsAs(vs)
          d must beTrue
          esAfter must containTheSameElementsAs(vs.drop(1))
        }}
      }
    }
  }
}
