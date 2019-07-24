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
import quasar.impl.cluster.{Timestamped, Atomix, Message}, Atomix.NodeInfo, Message._

import cats.effect.{IO, Resource, Timer}
import cats.syntax.contravariant._
import cats.syntax.parallel._
import cats.syntax.traverse._
import cats.instances.list._

import scalaz.std.string._

import scodec._
import scodec.codecs._

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import shims._

final class AntiEntropyStoreSpec extends IndexedStoreSpec[IO, String, String] {
  sequential

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val strCodec: Codec[String] = utf8_32
  implicit val timer: Timer[IO] = IO.timer(ec)

  val pool = BlockingContext.cached("antientropy-spec-pool")
  val defaultNode = NodeInfo("default", "localhost", 6000)

  val sleep: IO[Unit] = timer.sleep(new FiniteDuration(1000, MILLISECONDS))

  type Persistence = ConcurrentHashMap[String, Timestamped[String]]
  type UnderlyingStore = IndexedStore[IO, String, Timestamped[String]]
  type Store = IndexedStore[IO, String, String]

  def clusterify(
      me: NodeInfo,
      seeds: List[NodeInfo])(
      underlying: UnderlyingStore)
      : Resource[IO, Store] = for {
    atomix <- Atomix.resource[IO](me, seeds)
    storage <- Resource.liftF(IO(new ConcurrentHashMap[String, Timestamped[String]]()))
    timestamped = TimestampedStore[IO, String, String](underlying)
    cluster = Atomix.cluster[IO](atomix, pool).contramap(printMessage(_))
    store <- AntiEntropyStore[IO, String, String]("default", cluster, timestamped, pool)
  } yield store

  val underlyingResource: Resource[IO, UnderlyingStore] =
    Resource.liftF[IO, Persistence](IO(new ConcurrentHashMap[String, Timestamped[String]]()))
      .map(ConcurrentMapIndexedStore.unhooked[IO, String, Timestamped[String]](_, pool))

  def mkStore(me: NodeInfo, seeds: List[NodeInfo]): Resource[IO, Store] =
    underlyingResource.flatMap(clusterify(me, seeds))

  def parallelResource[A](list: List[Resource[IO, A]]): Resource[IO, List[A]] = {
    val ioPairs: IO[List[(A, IO[Unit])]] =
      list.parTraverse(_.allocated)
    val rawRes: Resource[IO, List[(A, IO[Unit])]] =
      Resource.make(ioPairs)((lst: List[(A, IO[Unit])]) => lst.parTraverse_(_._2))
    rawRes.map(_.map(_._1))
  }

  val emptyStore: Resource[IO, Store] = mkStore(defaultNode, List())
  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "clustering" >> {
    "data propagated" >>* {
      val node0: NodeInfo = NodeInfo("0", "localhost", 6000)
      val node1: NodeInfo = NodeInfo("1", "localhost", 6001)
      val node2: NodeInfo = NodeInfo("2", "localhost", 6002)
      for {
        (store0, finish0) <- mkStore(node0, List(node0, node1)).allocated
        _ <- store0.insert("a", "b")
        (store1, finish1) <- mkStore(node1, List(node0, node1)).allocated
        _ <- sleep
        a0 <- store0.lookup("a")
        a1 <- store1.lookup("a")
        _ <- store0.insert("b", "c")
        _ <- sleep
        b0 <- store0.lookup("b")
        b1 <- store1.lookup("b")
        _ <- finish0
        _ <- finish1
      } yield {
        a0 mustEqual Some("b")
        a1 mustEqual Some("b")
        b0 mustEqual Some("c")
        b1 mustEqual Some("c")
      }
    }
    "persistence" >>* {
      val node0: NodeInfo = NodeInfo("0", "localhost", 6000)
      underlyingResource.use { (underlying: UnderlyingStore) => for {
        (store0, finish0) <- clusterify(node0, List())(underlying).allocated
        _ <- store0.insert("a", "b")
        _ <- finish0
        (store0, finish0) <- clusterify(node0, List())(underlying).allocated
        someB <- store0.lookup("a")
        _ <- finish0
      } yield {
        someB mustEqual Some("b")
      }}
    }
    "1234 - 12 - 34 - 1234" >>* {
      val node0: NodeInfo = NodeInfo("0", "localhost", 6000)
      val node1: NodeInfo = NodeInfo("1", "localhost", 6001)
      val node2: NodeInfo = NodeInfo("2", "localhost", 6002)
      val node3: NodeInfo = NodeInfo("3", "localhost", 6003)
      val nodes: List[NodeInfo] = List(node0, node1, node2, node3)

      val underlying: Resource[IO, List[(UnderlyingStore, NodeInfo)]] =
        List(underlyingResource, underlyingResource, underlyingResource, underlyingResource)
          .sequence
          .map(_.zip(nodes))

      val resourceFromPair: (UnderlyingStore, NodeInfo) => Resource[IO, Store] = {
        case (s, i) => clusterify(i, nodes)(s)
      }

      underlying.use { (underlyings: List[(UnderlyingStore, NodeInfo)]) => for {
        (stores, finish) <- parallelResource(underlyings.map(resourceFromPair)).allocated
        _ <- stores(0).insert("foo", "bar")
        _ <- sleep
        foos0 <- stores.parTraverse(_.lookup("foo"))
        _ <- finish
        (stores, finish) <- parallelResource(List(underlyings(0), underlyings(1)).map(resourceFromPair)).allocated
        _ <- stores(0).insert("bar", "baz")
        _ <- sleep
        foos1 <- stores.parTraverse(_.lookup("foo"))
        bars1 <- stores.parTraverse(_.lookup("bar"))
        _ <- finish
        (stores, finish) <- parallelResource(List(underlyings(2), underlyings(3)).map(resourceFromPair)).allocated
        _ <- stores(0).insert("bar", "quux")
        _ <- sleep
        foos2 <- stores.parTraverse(_.lookup("foo"))
        bars2 <- stores.parTraverse(_.lookup("bar"))
        _ <- finish
        (stores, finish) <- parallelResource(underlyings.map(resourceFromPair)).allocated
        _ <- sleep
        foos3 <- stores.parTraverse(_.lookup("foo"))
        bars3 <- stores.parTraverse(_.lookup("bar"))
        _ <- finish
      } yield {
        foos0 mustEqual List(Some("bar"), Some("bar"), Some("bar"), Some("bar"))
        foos1 mustEqual List(Some("bar"), Some("bar"))
        bars1 mustEqual List(Some("baz"), Some("baz"))
        foos2 mustEqual List(Some("bar"), Some("bar"))
        bars2 mustEqual List(Some("quux"), Some("quux"))
        foos3 mustEqual List(Some("bar"), Some("bar"), Some("bar"), Some("bar"))
        bars3 mustEqual List(Some("quux"), Some("quux"), Some("quux"), Some("quux"))
      }}
    }
    "seed list might be incomplete" >>* {
      val node0: NodeInfo = NodeInfo("0", "localhost", 6000)
      val node1: NodeInfo = NodeInfo("1", "localhost", 6001)
      val node2: NodeInfo = NodeInfo("2", "localhost", 6002)

      val seeds: List[NodeInfo] =
        List(node0)
      val nodes: List[NodeInfo] =
        List(node0, node1, node2)
      val storesR: Resource[IO, List[Store]] =
        parallelResource(nodes.map(mkStore(_, seeds)))

      storesR.use { (stores: List[Store]) => for {
        _ <- stores(0).insert("0", "0")
        _ <- stores(1).insert("1", "1")
        _ <- stores(2).insert("2", "2")
        _ <- sleep
        zeros <- stores.parTraverse(_.lookup("0"))
        ones <- stores.parTraverse(_.lookup("1"))
        twos <- stores.parTraverse(_.lookup("2"))
      } yield {
        zeros mustEqual List(Some("0"), Some("0"), Some("0"))
        ones mustEqual List(Some("1"), Some("1"), Some("1"))
        twos mustEqual List(Some("2"), Some("2"), Some("2"))
      }}
    }
  }
}
