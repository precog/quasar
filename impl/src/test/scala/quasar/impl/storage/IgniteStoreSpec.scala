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

import quasar.concurrent.BlockingContext
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Ref
import cats.effect.concurrent.Deferred
import cats.syntax.functor._
import cats.syntax.parallel._
import cats.syntax.traverse._
import cats.instances.list._

import scalaz.std.anyVal._
import scalaz.std.string._
import scala.util.Random

import java.nio.file.{Paths, Path => NPath, Files}
import java.util.concurrent.ConcurrentHashMap
import org.apache.ignite._

final class IgniteStoreSpec extends IndexedStoreSpec[IO, Int, String] {
  sequential

  java.lang.System.setProperty(org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET, "false");
  val lm = java.util.logging.LogManager.getLogManager
  val root = lm.getLogger("")
  root.setLevel(java.util.logging.Level.OFF)

  val blockingPool: BlockingContext = BlockingContext.cached("ignite-indexed-store-spec")
  val freshIndex: IO[Int] = IO(Random.nextInt)

  val uniqueName: IO[String] = IO(java.util.UUID.randomUUID.toString)

  val emptyStore: Resource[IO, IndexedStore[IO, Int, String]] =
    Resource.liftF(uniqueName) map { i =>
      Ignite.IgniteCfg(i, 5555, List[String]())
    } flatMap (Ignite.resource(_)) evalMap { (ig: Ignite) =>
      val cache = ig.getOrCreateCache[Int, String]("test")
      IO(IgniteStore[IO, Int, String](cache, blockingPool))
    }
  val valueA = "A"
  val valueB = "B"

  private[this] def parallelResource[A](lst: List[Resource[IO, A]]): Resource[IO, List[A]] = {
    val allocated: IO[List[(A, IO[Unit])]] = lst parTraverse { (res: Resource[IO, A]) => res.allocated }
    val unzipped: IO[(List[A], List[IO[Unit]])] = allocated map { (lst: List[(A, IO[Unit])]) =>
      lst.foldLeft((List[A](), List[IO[Unit]]())) {
        case ((aAccum, ioAccum), (a, iounit)) => (a :: aAccum, iounit :: ioAccum)
      }
    }
    val finalizerPar: IO[(List[A], IO[Unit])] =
      unzipped map {
        case (as, ios) => (as, ios.parSequence as (()))
      }

    val pairResource = Resource.make(finalizerPar) {
      case (_, io) => io
    }
    pairResource map (_._1)
  }

  val counter: Ref[IO, Int] = Ref.unsafe[IO, Int](5000)
  val newPort: IO[Int] = counter.modify(x => (x + 1, x + 1))
/*
  "syncing" >>* {
    val ioPorts: IO[List[Int]] = List(newPort, newPort).sequence
    val ioNodes = ioPorts map { (ports: List[Int]) => ports map { p =>
      val addresses = ports filter { pp => p != pp} map { i => s"127.0.0.1:${i}" }
      Ignite.IgniteCfg(s"ignite-${p.toString}", p, addresses)
    }}

    val store0: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-0", 5001, List[String]())) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val store1: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-1", 5002, List("localhost:5001"))) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val stores: Resource[IO, List[IndexedStore[IO, String, String]]] = List(store0, store1).sequence

    stores use { (ss: List[IndexedStore[IO, String, String]]) => for {
      _ <- ss(0).insert("foo", "bar")
      foos <- ss parTraverse { s => s.lookup("foo") }
    } yield {
      foos === List(Some("bar"), Some("bar"))
    }}
  }
 */
  "12 - 2 - 12" >>* {
    val addresses = List("localhost:5001", "localhost:5002")

    val resource0: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-0", 5001, addresses)) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val resource1: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-1", 5002, addresses)) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }

    for {
      (store0, finish0) <- resource0.allocated
      (store1, finish1) <- resource1.allocated
      _ <- store0.insert("foo", "bar")
      _ <- finish1
      _ <- store0.insert("bar", "baz")
      (store1, finish1) <- resource1.allocated
      _ <- store1.insert("baz", "quux")
      ss = List(store0, store1)
      foos <- ss.traverse(_.lookup("foo"))
      bars <- ss.traverse(_.lookup("bar"))
      bazs <- ss.traverse(_.lookup("baz"))
      _ <- finish0
      _ <- finish1
    } yield {
      foos === List(Some("bar"), Some("bar"))
      bars === List(Some("baz"), Some("baz"))
      bazs === List(Some("quux"), Some("quux"))
    }
  }

  "01 - 0 - 023 - 3 - 13" >>* {
    val addresses = List(
      "localhost:5001",
      "localhost:5002",
      "localhost:5003",
      "localhost:5004")

    val resource0: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-0", 5001, addresses)) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val resource1: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-1", 5002, List("localhost:5001", "localhost:5004"))) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val resource2: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-2", 5003, addresses)) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }
    val resource3: Resource[IO, IndexedStore[IO, String, String]] =
      Ignite.resource[IO](Ignite.IgniteCfg("ignite-3", 5004, addresses)) evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("simultaneous", ig) map (IgniteStore[IO, String, String](_, blockingPool))
      }

    for {
      // 0
      (store0, finish0) <- resource0.allocated
      // 01
      (store1, finish1) <- resource1.allocated
      _ <- store1.insert("a", "b")
      // 0
      _ <- finish1
      _ <- store0.insert("b", "c")
      // 02
      (store2, finish2) <- resource2.allocated
      // 023
      (store3, finish3) <- resource3.allocated
      _ <- store2.insert("c", "d")
      // 03
      _ <- finish2
      // 3
      _ <- finish0
      // 13
      (store1, finish1) <- resource1.allocated
      as <- List(store1, store3).traverse(_.lookup("a"))
      bs <- List(store1, store3).traverse(_.lookup("b"))
      cs <- List(store1, store3).traverse(_.lookup("c"))
      // 3
      _ <- finish1
      // _
      _ <- finish3
    } yield {
      as mustEqual List(Some("b"), Some("b"))
      bs mustEqual List(Some("c"), Some("c"))
      cs mustEqual List(Some("d"), Some("d"))
    }
  }

  "mapdb persistent" >> {
    "0" >>* {
      val addresses: List[String] = List()
      val mapStore: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
      mapStore.put("predefined", "ooo")
      val resource0: Resource[IO, IndexedStore[IO, String, String]] =
        Ignite.resource[IO](Ignite.IgniteCfg("ignite-0", 5001, addresses)) evalMap { (ig: Ignite) =>
          Ignite.mapStoreCache[IO, String, String]("simultaneous", ig, mapStore) map (IgniteStore[IO, String, String](_, blockingPool))
        }

      resource0.use { store0 => for {
        predefined <- store0.lookup("predefined")
        _ <- store0.insert("foo", "bar")
        inMap <- IO(mapStore.get("foo"))
      } yield {
        predefined mustEqual Some("ooo")
        inMap mustEqual "bar"
      }}
    }
    "0 - 01" >>* {
      val addresses: List[String] = List(
        "localhost:5001",
        "localhost:5002")
      val mapStore0: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
      val mapStore1: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()

      mapStore0.put("predefined", "ooo")

      val resource0: Resource[IO, IndexedStore[IO, String, String]] =
        Ignite.resource[IO](Ignite.IgniteCfg("ignite-0", 5001, addresses)) evalMap { (ig: Ignite) =>
          Ignite.mapStoreCache[IO, String, String]("simultaneous", ig, mapStore0) map (IgniteStore[IO, String, String](_, blockingPool))
        }

      val resource1: Resource[IO, IndexedStore[IO, String, String]] =
        Ignite.resource[IO](Ignite.IgniteCfg("ignite-1", 5002, addresses)) evalMap { (ig: Ignite) =>
          Ignite.mapStoreCache[IO, String, String]("simultaneous", ig, mapStore1) map (IgniteStore[IO, String, String](_, blockingPool))
        }

      for {
        (store0, finish0) <- resource0.allocated
        (store1, finish1) <- resource1.allocated
        p0 <- store0.lookup("predefined")
        p1 <- store1.lookup("predefined")
        _ <- store0.insert("foo", "bar")
        foo0 <- store0.lookup("foo")
        foo1 <- store1.lookup("foo")
        _ <- store1.insert("bar", "baz")
        bar0 <- store0.lookup("bar")
        bar1 <- store1.lookup("bar")
        _ <- finish0
        _ <- finish1
      } yield {
        foo0 mustEqual Some("bar")
        foo1 mustEqual Some("bar")
        bar0 mustEqual Some("baz")
        bar1 mustEqual Some("baz")
        println(mapStore0)
        println(mapStore1)
        true
      }
    }
  }
/*
  "native persistence" >> {
    def mkResource(ix: Int, addresses: List[String]): Resource[IO, IndexedStore[IO, String, String]] = {
      val igniteR = Ignite.persistentResource[IO](
        Ignite.IgniteCfg(s"ignite-${ix}", 5000 + ix, addresses),
        Paths.get(s"ignite-${ix}"))
      igniteR.evalMap { (ig: Ignite) =>
        Ignite.createCache[IO, String, String]("native", ig).map(IgniteStore[IO, String, String](_, blockingPool))
      }
    }
    "0" >>* {
      val addresses: List[String] = List()
      val resource0: Resource[IO, IndexedStore[IO, String, String]] = mkResource(0, addresses)
      for {
        (store0, finish0) <- resource0.allocated
        _ <- store0.insert("foo", "bar")
        _ <- finish0
        (store0, finish0) <- resource0.allocated
        foo <- store0.lookup("foo")
        _ <- finish0
      } yield {
        foo mustEqual Some("bar")
      }
    }
    "0 - 01 - _ - 1 - 01" >>* {
      val addresses: List[String] = List(
        "localhost:5000",
        "localhost:5001")
      val resource0: Resource[IO, IndexedStore[IO, String, String]] = mkResource(0, addresses)
      val resource1: Resource[IO, IndexedStore[IO, String, String]] = mkResource(1, addresses)
      for {
        (store0, finish0) <- resource0.allocated
        _ <- store0.insert("foo", "bar")
        (store1, finish1) <- resource1.allocated
        foos0 <- List(store0, store1).traverse(_.lookup("foo"))
        _ <- finish0
        _ <- finish1
        (store1, finish1) <- resource1.allocated
        foo1 <- store1.lookup("foo")
        (store0, finish0) <- resource0.allocated
        foo2 <- store0.lookup("foo")
        _ <- finish0
        _ <- finish1
      } yield {
        foos0 mustEqual List(Some("bar"), Some("bar"))
        foo1 mustEqual Some("bar")
        foo2 mustEqual Some("bar")
      }
    }
    "0 - _ - 1 - 01 - _ - 1" >>* {
      val addresses: List[String] = List(
        "localhost:5000",
        "localhost:5001")
      val resource0: Resource[IO, IndexedStore[IO, String, String]] = mkResource(10, addresses)
      val resource1: Resource[IO, IndexedStore[IO, String, String]] = mkResource(11, addresses)
      for {
        (store0, finish0) <- resource0.allocated
        _ <- store0.insert("foo", "bar")
        _ <- finish0
        (store1, finish1) <- resource1.allocated
        foo0 <- store1.lookup("foo")
        (store0, finish0) <- resource0.allocated
        _ <- store1.insert("foo", "baz")
        foo1 <- store1.lookup("foo")
        _ <- finish0
        _ <- finish1
        (store1, finish1) <- resource1.allocated
        foo2 <- store1.lookup("foo")
        _ <- finish1
      } yield {
        foo0 mustEqual None
        foo1 mustEqual Some("baz")
        foo2 mustEqual Some("baz")
        true
      }
    }
  }
 */
}
