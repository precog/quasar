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

import slamdata.Predef._

import quasar.{concurrent => qc}
import quasar.impl.cluster.{Timestamped, Atomix, Message}, Atomix.NodeInfo, Message._

import cats.effect.{IO, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.contravariant._
import cats.syntax.flatMap._
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

final class AntiEntropyStoreSpec extends IndexedStoreSpec[IO, String, String] {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val strCodec: Codec[String] = utf8_32
  implicit val timer: Timer[IO] = IO.timer(ec)

  val portRef: Ref[IO, Int] = Ref.unsafe[IO, Int](7000)
  def mkNode(id: String): IO[NodeInfo] = for {
    port <- portRef.modify((x: Int) => (x + 1, x + 1))
  } yield NodeInfo(id, "localhost", port)

  val blocker = qc.Blocker.cached("antientropy-spec-pool")
  val sleep: IO[Unit] = timer.sleep(new FiniteDuration(4000, MILLISECONDS))

  type Persistence = ConcurrentHashMap[String, Timestamped[String]]
  type UnderlyingStore = IndexedStore[IO, String, Timestamped[String]]
  type Store = IndexedStore[IO, String, String]

  def clusterify(
      me: NodeInfo,
      seeds: List[NodeInfo])(
      underlying: UnderlyingStore)
      : Resource[IO, Store] = for {
    atomix <- Atomix.resource[IO](me, seeds.map(_.address))
    storage <- Resource.liftF(IO(new ConcurrentHashMap[String, Timestamped[String]]()))
    timestamped <- TimestampedStore[IO, String, String](underlying)
    cluster = Atomix.cluster[IO](atomix, blocker).contramap(printMessage(_))
    store <- AntiEntropyStore.default[IO, String, String]("default", cluster, timestamped, blocker)
  } yield store

  val underlyingResource: Resource[IO, UnderlyingStore] =
    Resource.liftF[IO, Persistence](IO(new ConcurrentHashMap[String, Timestamped[String]]()))
      .map(ConcurrentMapIndexedStore.unhooked[IO, String, Timestamped[String]](_, blocker))

  def mkStore(me: NodeInfo, seeds: List[NodeInfo]): Resource[IO, Store] =
    underlyingResource.flatMap(clusterify(me, seeds))

  def parallelResource[A](list: List[Resource[IO, A]]): Resource[IO, List[A]] = {
    val ioPairs: IO[List[(A, IO[Unit])]] =
      list.parTraverse(_.allocated)
    val rawRes: Resource[IO, List[(A, IO[Unit])]] =
      Resource.make(ioPairs)((lst: List[(A, IO[Unit])]) => lst.parTraverse_(_._2))
    rawRes.map(_.map(_._1))
  }

  val emptyStore: Resource[IO, Store] = Resource.liftF(mkNode("default")).flatMap(mkStore(_, List()))
  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "clustering" >> {
    "data propagated" >>* {
      for {
        node0 <- mkNode("0")
        node1 <- mkNode("1")
        (a0, a1, b0, b1) <- mkStore(node0, List(node0, node1)).use { store0 => for {
          _ <- store0.insert("a", "b")
          (a0, a1, b0, b1) <- mkStore(node1, List(node0, node1)).use { store1 => for {
            _ <- sleep
            a0 <- store0.lookup("a")
            a1 <- store1.lookup("a")
            _ <- store0.insert("b", "c")
            _ <- sleep
            b0 <- store0.lookup("b")
            b1 <- store1.lookup("b")
          } yield (a0, a1, b0, b1) }
        } yield (a0, a1, b0, b1) }
      } yield {
        a0 mustEqual Some("b")
        a1 mustEqual Some("b")
        b0 mustEqual Some("c")
        b1 mustEqual Some("c")
      }
    }
    "persistence" >>* {
      underlyingResource.use { (underlying: UnderlyingStore) => for {
        node0 <- mkNode("0")
        _ <- clusterify(node0, List())(underlying).use { store => store.insert("a", "b") }
        someB <- clusterify(node0, List())(underlying).use { store => store.lookup("a") }
      } yield {
        someB mustEqual Some("b")
      }}
    }
    "1234 - 12 - 34 - 1234" >>* {
      val underlying: Resource[IO, List[(UnderlyingStore, NodeInfo)]] = {
        val nodesR: Resource[IO, List[NodeInfo]] =
          Resource.liftF(List("0", "1", "2", "3").traverse(mkNode(_)))
        val underlyingsR: Resource[IO, List[UnderlyingStore]] =
          List(underlyingResource, underlyingResource, underlyingResource, underlyingResource)
            .sequence
        for {
          underlyings <- underlyingsR
          nodes <- nodesR
        } yield underlyings.zip(nodes)
      }

      val mkResource: (List[NodeInfo]) => (UnderlyingStore, NodeInfo) => Resource[IO, Store] = nodes => {
        case (s, i) => clusterify(i, nodes)(s)
      }

      underlying.use { (underlyings: List[(UnderlyingStore, NodeInfo)]) => for {
        resourceFromPair <- IO(mkResource(underlyings.map(_._2)))
        foos0 <- parallelResource(underlyings.map(resourceFromPair)).use { stores => for {
          _ <- stores(0).insert("foo", "bar")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
        } yield foos }
        (foos1, bars1) <- parallelResource(List(underlyings(0), underlyings(1)).map(resourceFromPair)).use { stores => for {
          _ <- stores(0).insert("bar", "baz")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
          bars <- stores.parTraverse(_.lookup("bar"))
        } yield (foos, bars) }
        (foos2, bars2) <- parallelResource(List(underlyings(2), underlyings(3)).map(resourceFromPair)).use { stores => for {
          _ <- stores(0).insert("bar", "quux")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
          bars <- stores.parTraverse(_.lookup("bar"))
        } yield (foos, bars) }
        (foos3, bars3) <- parallelResource(underlyings.map(resourceFromPair)).use { stores => for {
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
          bars <- stores.parTraverse(_.lookup("bar"))
        } yield (foos, bars) }
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
    "1234 - 12 - 34 - 1234 -- deletion" >>* {
      val underlying: Resource[IO, List[(UnderlyingStore, NodeInfo)]] = {
        val nodesR: Resource[IO, List[NodeInfo]] =
          Resource.liftF(List("0", "1", "2", "3").traverse(mkNode(_)))
        val underlyingsR: Resource[IO, List[UnderlyingStore]] =
          List(underlyingResource, underlyingResource, underlyingResource, underlyingResource)
            .sequence
        for {
          underlyings <- underlyingsR
          nodes <- nodesR
        } yield underlyings.zip(nodes)
      }

      val mkResource: (List[NodeInfo]) => (UnderlyingStore, NodeInfo) => Resource[IO, Store] = nodes => {
        case (s, i) => clusterify(i, nodes)(s)
      }

      underlying.use { (underlyings: List[(UnderlyingStore, NodeInfo)]) => for {
        resourceFromPair <- IO(mkResource(underlyings.map(_._2)))
        foos0 <- parallelResource(underlyings.map(resourceFromPair)).use { stores => for {
          _ <- stores(0).insert("foo", "bar")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
        } yield foos }
        foos1 <- parallelResource(List(underlyings(0), underlyings(1)).map(resourceFromPair)).use { stores => for {
          _ <- stores(0).insert("foo", "quux")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
        } yield foos }
        foos2 <- parallelResource(List(underlyings(2), underlyings(3)).map(resourceFromPair)).use { stores => for {
          _ <- stores(0).delete("foo")
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
        } yield foos }
        foos3 <- parallelResource(underlyings.map(resourceFromPair)).use { stores => for {
          _ <- sleep
          foos <- stores.parTraverse(_.lookup("foo"))
        } yield foos }
      } yield {
        foos0 mustEqual List(Some("bar"), Some("bar"), Some("bar"), Some("bar"))
        foos1 mustEqual List(Some("quux"), Some("quux"))
        foos2 mustEqual List(None, None)
        foos3 mustEqual List(None, None, None, None)
      }}
    }

    "seed list might be incomplete" >>* {
      val storesR: Resource[IO, List[Store]] = {
        val ioRes: IO[Resource[IO, List[Store]]] = for {
          node0 <- mkNode("0")
          node1 <- mkNode("1")
          node2 <- mkNode("2")
          seeds = List(node0)
          nodes = List(node0, node1, node2)
        } yield parallelResource(nodes.map(mkStore(_, seeds)))
        Resource.liftF(ioRes).flatten
      }

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
