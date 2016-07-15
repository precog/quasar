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
import quasar.fp.TaskRef
import quasar.fp.free._

import scala.math

import monocle.function.{Field1, Field2}
import monocle.std.tuple2._
import org.specs2.ScalaCheck
import org.specs2.mutable
import scalaz._
import scalaz.concurrent.{Strategy, Task}
import scalaz.syntax.foldable._
import scalaz.std.anyVal._
import scalaz.std.list._

final class KeyValueStoreSpec extends mutable.Specification with ScalaCheck {
  import KeyValueStoreOpArbitrary._

  type IDS = Map[Int, Double]
  type TestKvs[A] = KeyValueStore[Int, Double, A]
  type CachedKvs[A] = Coproduct[Cache[TestKvs, ?], TestKvs, A]

  val kvs = KeyValueStore.Ops[Int, Double, TestKvs]

  def runState[A](fa: Free[TestKvs, A]): State[(IDS, IDS), A] = {
    val runCache =
      KeyValueStore.toState[State[(IDS, IDS), ?]](Field1.first[(IDS, IDS), IDS])

    val runBacking =
      KeyValueStore.toState[State[(IDS, IDS), ?]](Field2.second[(IDS, IDS), IDS])

    fa.flatMapSuspension(KeyValueStore.fullWriteThrough[CachedKvs, Int, Double])
      .foldMap(Cache(runCache) :+: runBacking)
  }

  def runTask[A](
    cacheRef: TaskRef[IDS],
    backingRef: TaskRef[IDS],
    fa: Free[TestKvs, A]
  ): Task[A] = {
    val toRef = KeyValueStore.toAtomicRef[Int, Double]

    val runCache =
      foldMapNT(AtomicRef.fromTaskRef(cacheRef)) compose toRef

    val runBacking =
      foldMapNT(AtomicRef.fromTaskRef(backingRef)) compose toRef

    fa.flatMapSuspension(KeyValueStore.fullWriteThrough[CachedKvs, Int, Double])
      .foldMap(Cache(runCache) :+: runBacking)
  }

  "KeyValueStore" should {
    "full write-through cache" >> {
      "services 'keys' from the cache" ! prop { cache: IDS =>
        runState(kvs.keys).eval((cache, Map())) must
          containTheSameElementsAs(cache.keys.toList)
      }

      "services 'get' from the cache" ! prop {
        (cache: IDS, idx: Short) => (cache.nonEmpty) ==> {

        val ks = cache.keys.toVector
        val k  = ks(math.abs(idx.toInt) % ks.length)

        runState(kvs.get(k).run).eval((cache, Map())) must beSome(cache(k))
      }}

      "'get' when key not in cache but in backing returns none" ! prop { (k: Int, v: Double) =>
        runState(kvs.get(k).run).eval((Map(), Map(k -> v))) must beNone
      }

      "cache and backing should agree for all operations when initialized identically" ! prop {
        (testOps: List[Free[TestKvs, Unit]], init: IDS) =>

        val (cacheRes, backingRes) = runState(testOps.sequence_).exec((init, init))

        cacheRes.toList must containTheSameElementsAs(backingRes.toList)
      }

      "cache and backing should agree during concurrent operations" ! prop {
        (init: IDS, testOps: List[Free[TestKvs, Unit]]) =>

        val res = for {
          cref  <- TaskRef(init)
          bref  <- TaskRef(init)
          tasks =  testOps map { fu =>
                     Task.fork(runTask(cref, bref, fu))(Strategy.DefaultExecutorService)
                   }
          _     <- Nondeterminism[Task].aggregateCommutative(tasks)
          cres  <- cref.read
          bres  <- bref.read
        } yield cres.toList must containTheSameElementsAs(bres.toList)

        res.unsafePerformSync
      }
    }
  }
}
