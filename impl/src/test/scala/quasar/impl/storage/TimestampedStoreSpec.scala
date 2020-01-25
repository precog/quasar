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

import slamdata.Predef._

import quasar.{concurrent => qc}
import quasar.impl.cluster.Timestamped

import cats.effect.{IO, Resource, Timer}
import cats.effect.concurrent.{Ref, Deferred}

import scalaz.std.string._

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

final class TimestampedStoreSpec extends IndexedStoreSpec[IO, String, String] {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(ec)

  type Persistence = ConcurrentHashMap[String, Timestamped[String]]
  type UnderlyingStore = IndexedStore[IO, String, Timestamped[String]]

  val blocker = qc.Blocker.cached("timestamped-spec-pool")

  val underlying: Resource[IO, IndexedStore[IO, String, Timestamped[String]]] =
    Resource.liftF[IO, Persistence](IO(new ConcurrentHashMap[String, Timestamped[String]]()))
      .map(ConcurrentMapIndexedStore.unhooked[IO, String, Timestamped[String]](_, blocker))

  val emptyStore: Resource[IO, IndexedStore[IO, String, String]] = for {
    u <- underlying
    res <- TimestampedStore(u)
  } yield res

  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "timestamp store" >> {
    "lookup for timestamped" >>* {
      val resource = for {
        us <- underlying
        bar <- Resource.liftF(Timestamped.tagged[IO, String]("bar"))
        _ <- Resource.liftF(us.insert("foo", bar))
        ts <- TimestampedStore(us)
        res <- Resource.liftF(ts.lookup("foo"))
      } yield {
        res mustEqual Some("bar")
      }
      resource.use(IO.pure(_))
    }
    "inserted values are timestamps" >>* {
      val resource = for {
        us <- underlying
        ts <- TimestampedStore(us)
        _ <- Resource.liftF(ts.insert("foo", "bar"))
        bar <- Resource.liftF(us.lookup("foo"))
      } yield {
        bar.flatMap(Timestamped.raw(_)) mustEqual Some("bar")
      }
      resource.use(IO.pure(_))
    }
    "deletion preserves tombstones" >>* {
      val resource = for {
        us <- underlying
        start <- Resource.liftF(timer.clock.realTime(MILLISECONDS))
        ts <- TimestampedStore(us)
        _ <- Resource.liftF(ts.insert("foo", "bar"))
        _ <- Resource.liftF(ts.delete("foo"))
        t <- Resource.liftF(us.lookup("foo"))
        stop <- Resource.liftF(timer.clock.realTime(MILLISECONDS))
      } yield {
        t must beLike {
          case Some(Timestamped.Tombstone(stamp)) =>
            stamp must be_>=(start)
            stamp must be_<=(stop)
        }
      }
      resource.use(IO.pure(_))
    }
    "timestamps works" >>* {
      val resource = for {
        us <- underlying
        ts <- TimestampedStore(us)
        _ <- Resource.liftF(for {
          _ <- ts.insert("foo", "bar")
          _ <- ts.delete("foo")
          _ <- ts.insert("foo", "baz")
          _ <- ts.insert("bar", "foo")
        } yield ())
        foo <- Resource.liftF(us.lookup("foo"))
        bar <- Resource.liftF(us.lookup("bar"))
        timestamps <- Resource.liftF(ts.timestamps)
      } yield {
        timestamps.size mustEqual 2
        timestamps.get("foo") mustEqual(foo.map(Timestamped.timestamp(_)))
        timestamps.get("bar") mustEqual(bar.map(Timestamped.timestamp(_)))
      }
      resource.use(IO.pure(_))
    }
    "updates stream works" >>* {
      for {
        (ts, finish) <- underlying.flatMap(TimestampedStore(_)).allocated
        ref <- Ref.of[IO, List[(String, Timestamped[String])]](List())
        streamIsFinished <- Deferred[IO, Unit]
        _ <- ts.updates.evalMap(x => ref.modify(lst => (x :: lst, ()))).onFinalize(streamIsFinished.complete(())).compile.drain.start
        _ <- ts.insert("foo", "bar")
        _ <- ts.delete("foo")
        _ <- ts.insert("foo", "baz")
        _ <- ts.insert("bar", "foo")
        _ <- finish
        // the stream is finished gracefully (it handles leftover updates from queue instead of force termination)
        // so we need to wait for them
        _ <- streamIsFinished.get
        lst <- ref.get.map(_.reverse)
      } yield {
        lst.size mustEqual 4
        lst.map(_._1) mustEqual List("foo", "foo", "foo", "bar")
        lst.map(x => Timestamped.raw(x._2)) mustEqual List(Some("bar"), None, Some("baz"), Some("foo"))
      }
    }
  }
}
