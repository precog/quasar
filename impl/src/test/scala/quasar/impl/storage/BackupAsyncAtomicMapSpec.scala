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

/*
package quasar.impl.storage

import slamdata.Predef._

import cats.effect.{IO, Resource}
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import cats.syntax.parallel._
import cats.syntax.traverse._
import cats.instances.list._

import quasar.concurrent.BlockingContext

import io.atomix.core._
import io.atomix.core.election._
import io.atomix.core.map._
import io.atomix.cluster.discovery._
import io.atomix.cluster._
import io.atomix.protocols.backup.partition._
import io.atomix.protocols.raft.partition._
import io.atomix.protocols.raft.storage.RaftStorage
import io.atomix.storage.StorageLevel
import org.mapdb._

import scalaz.std.string._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import java.nio.file.{Paths, Path => NPath}
import java.util.concurrent.ConcurrentHashMap

import org.specs2.specification._

import shims._

import AtomixSetup._

final class BackupAsyncAtomicMapSpec extends IndexedStoreSpec[IO, String, String] {
  // parallel initialization of too much raft storages leads to OOM
  sequential
  val pool = BlockingContext.cached("mapdb-async-atomic-map")

  def mkNodeInfo(port: Int): NodeInfo = NodeInfo(s"node-${port}", "localhost", port)

  val DefaultMapName = "default"
  val AtomixThreads = "atomix-check-thread"

  val counter: Ref[IO, Int] = Ref.unsafe[IO, Int](5000)

  val newPort: IO[Int] = counter.modify(x => (x + 1, x + 1))

  val emptyStore: Resource[IO, IndexedStore[IO, String, String]] = for {
    port <- Resource.liftF(newPort)
    node = mkNodeInfo(port)
    atomix <- AtomixSetup[IO](node, List(node), Paths.get(s"default-${port}.raft"), AtomixThreads)
    store <- Resource.liftF { for {
      backup <- IO(new ConcurrentHashMap[String, String]())
      result <- BackupStore[IO, String, String](atomix, DefaultMapName, backup, pool)
    } yield result }
  } yield store


  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

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


  "simultaneous" >>* {
    val ioPorts: IO[List[Int]] = List(newPort, newPort).sequence
    val ioNodes: IO[List[NodeInfo]] = ioPorts map { (ports: List[Int]) => ports map (mkNodeInfo(_)) }
    val stores: Resource[IO, List[IndexedStore[IO, String, String]]] = for {
      nodes <- Resource.liftF(ioNodes)
      parStores = nodes map { (node: NodeInfo) =>
        AtomixSetup[IO](node, nodes, Paths.get(s"simultanesous-${node.port}.raft"), AtomixThreads) evalMap { atomix => for {
          backup <- IO(new ConcurrentHashMap[String, String]())
          _ <- IO(backup.put("prefilled", "prefilled"))
          result <- BackupStore[IO, String, String](atomix, "simultaneous", backup, pool)
        } yield result}
      }
      stores <- parallelResource(parStores)
    } yield stores
    stores use { (ss: List[IndexedStore[IO, String, String]]) => for {
      _ <- ss(0).insert("foo", "bar")
      foos <- ss parTraverse { s => s.lookup("foo") }
      prefilleds <- ss parTraverse { s => s.lookup("prefilled") }
    } yield {
      foos === List(Some("bar"), Some("bar"))
      prefilleds === List(Some("prefilled"), Some("prefilled"))
    }}
  }

  "12 - _ - 2 - 12" >>* {
    val backup1 = new ConcurrentHashMap[String, String]()
    val backup2 = new ConcurrentHashMap[String, String]()
    val node1: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync
    val node2: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync
    val resource1: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node1, List(node1, node2), Paths.get(s"reconnect-1.raft"), AtomixThreads) evalMap { atomix =>
        BackupStore[IO, String, String](atomix, "reconnect", backup1, pool)
      }
    val resource2: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node2, List(node1, node2), Paths.get(s"reconnect-2.raft"), AtomixThreads) evalMap { atomix =>
        BackupStore[IO, String, String](atomix, "reconnect", backup2, pool)
      }

    for {
      _ <- parallelResource(List(resource1, resource2)) use { stores =>
        stores(0).insert("a", "b")
      }
      result <- resource2 use { store2 => for {
        _ <- store2.insert("b", "c")
        inner <- resource1 use { store1 => for {
          as <- List(store1, store2) parTraverse (_.lookup("a"))
          bs <- List(store1, store2) parTraverse (_.lookup("b"))
        } yield (as, bs) }
      } yield inner }
    } yield {
      result._1 === List(Some("b"), Some("b"))
      result._2 === List(Some("c"), Some("c"))
    }
  }

  "12 - 1 - 134 - 4 - 24" >>* {
    val backup1 = new ConcurrentHashMap[String, String]()
    val backup2 = new ConcurrentHashMap[String, String]()
    val backup3 = new ConcurrentHashMap[String, String]()
    val backup4 = new ConcurrentHashMap[String, String]()
    val node1: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync
    val node2: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync
    val node3: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync
    val node4: NodeInfo = (newPort map (mkNodeInfo(_))).unsafeRunSync

    val resource1: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node1, List(node1, node2, node3, node4), Paths.get(s"reconnect-1.raft"), AtomixThreads) evalMap { atomix =>
        BackupStore[IO, String, String](atomix, "reconnect", backup1, pool)
      }
    val resource2: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node2, List(node1, node2, node3, node4), Paths.get(s"reconnect-2.raft"), AtomixThreads) evalMap { atomix =>
        BackupStore[IO, String, String](atomix, "reconnect", backup2, pool)
      }
    val resource3: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node3, List(node1, node2, node3, node4), Paths.get(s"reconnect-3.raft"), AtomixThreads) evalMap { atomix =>
        println("RESOURCE 3")
        BackupStore[IO, String, String](atomix, "reconnect", backup3, pool)
      }
    val resource4: Resource[IO, IndexedStore[IO, String, String]] =
      AtomixSetup[IO](node4, List(node1, node2, node3, node4), Paths.get(s"reconnect-4.raft"), AtomixThreads) evalMap { atomix =>
        println("RESOURCE 4")
        BackupStore[IO, String, String](atomix, "reconnect", backup4, pool)
      }

    for {
      pairs12 <- List(resource1, resource2, resource3) parTraverse (_.allocated)
      store1 = pairs12(0)._1
      finish1 = pairs12(0)._2
      store2 = pairs12(1)._1
      finish2 = pairs12(1)._2
      store3 = pairs12(2)._1
      finish3 = pairs12(2)._2
      _ <- store1.insert("a", "b")
      _ <- finish2
      _ <- IO(println("2 finished"))

      pairs34 <- List(resource4, resource2) parTraverse (_.allocated)
      _ <- IO(println("34 started"))
      store4 = pairs34(0)._1
      finish4 = pairs34(0)._2
      store2 = pairs12(1)._1
      finish2 = pairs12(1)._2
      _ <- store3.insert("b", "c")
      _ <- IO(println("3 inserted"))
      as1 <- List(store1, store3, store4) parTraverse (_.lookup("a"))
      _ <- IO(println("134 a"))
      bs1 <- List(store1, store3, store4) parTraverse (_.lookup("b"))
      _ <- IO(println("134 b"))
      _ <- List(finish1, finish3, finish4, finish2).parSequence
    } yield {
      as1 === List(Some("b"), Some("b"), Some("b"))
      bs1 === List(Some("c"), Some("c"), Some("c"))
    }

  }

/*
  "restore" >>* {
    val atomix1 = mkAtomix("member-1", "localhost", 5004)
      .build()

    val prefilled = {
      val res = new ConcurrentHashMap[String, String]()
      res.put("prefilled-key", "prefilled")
      res
    }

    for {
      _ <- IO(atomix1.start().join())
      store1 <- mkStore(atomix1, "map", prefilled, true)
      _ <- store1.restore
      pv <- store1.lookup("prefilled-key")
      _ <- IO(atomix1.stop().join())
    } yield {
      pv === Some("prefilled")
    }
  }

  "joining and rejoining" >>* {
    val atomix1 =
      Atomix.builder()
        .withMemberId("111")
        .withAddress("localhost", 5010)
        .withManagementGroup(PrimaryBackupPartitionGroup.builder("system").withNumPartitions(1).build())
        .withPartitionGroups(RaftPartitionGroup
          .builder("data")
          .withMembers("111")
          .withStorageLevel(StorageLevel.DISK)
          .withDataDirectory(Paths.get(java.util.UUID.randomUUID().toString().concat(".raft")).toFile)
          .withNumPartitions(32)
          .build())
        .withMembershipProvider(BootstrapDiscoveryProvider.builder()
          .withNodes(Node.builder().withAddress("localhost", 5011).build()).build())
        .build()

    val atomix2 =
      Atomix.builder()
        .withMemberId("222")
        .withAddress("localhost", 5011)
        .withManagementGroup(PrimaryBackupPartitionGroup.builder("system").withNumPartitions(1).build())
        .withPartitionGroups(RaftPartitionGroup
          .builder("data")
          .withMembers("111", "222")
          .withStorageLevel(StorageLevel.DISK)
          .withDataDirectory(Paths.get(java.util.UUID.randomUUID().toString().concat(".raft")).toFile)
          .withNumPartitions(32)
          .build())
        .build

    for {
      persistent1 <- IO(new ConcurrentHashMap[String, String]())
      persistent2 <- IO(new ConcurrentHashMap[String, String]())

      _ <- AsyncAtomicIndexedStore.toF[IO, java.lang.Void](atomix1.start)
      _ <- IO(println("ATOMIX 1"))
      store1 <- mkStore(atomix1, "map", persistent1)
      a <- IO(java.lang.System.currentTimeMillis())
      _ <- store1.insert("a", "b")
      b <- IO(java.lang.System.currentTimeMillis())
      _ <- IO(println(b - a))

      _ <- AsyncAtomicIndexedStore.toF[IO, java.lang.Void](atomix2.start)
      _ <- IO(println("ATOMIX 2"))
      store2 <- mkStore(atomix2, "map", persistent2)


      a <- IO(java.lang.System.currentTimeMillis())
      o1 <- store1.lookup("a")
      b <- IO(java.lang.System.currentTimeMillis())
      o2 <- store2.lookup("a")
      _ <- IO(println(b - a))
      _ <- IO(println(s"o1 ::: ${o1}    o2 ::: ${o2}"))

      a <- IO(java.lang.System.currentTimeMillis())
      _ <- store1.insert("b", "c")
      b <- IO(java.lang.System.currentTimeMillis())
      _ <- IO(println(b - a))

      a <- IO(java.lang.System.currentTimeMillis())
      _ <- store1.lookup("a")
      b <- IO(java.lang.System.currentTimeMillis())
      _ <- IO(println(b - a))

      o1 <- store1.lookup("a")
      o2 <- store2.lookup("a")
      _ <- IO(println(s"o1 ::: ${o1}    o2 ::: ${o2}"))
      o1 <- store1.lookup("b")
      o2 <- store2.lookup("b")
      _ <- IO(println(s"o1 ::: ${o1}    o2 ::: ${o2}"))

      _ <- AsyncAtomicIndexedStore.toF[IO, java.lang.Void](atomix2.stop)
      _ <- AsyncAtomicIndexedStore.toF[IO, java.lang.Void](atomix1.stop)
    } yield {
      true
    }
  }


  "stop and run" >>* {
      def mkAtomix(id: String, port: Int, connects: List[(String, Int)]): Atomix = {
      val ids: List[String] = connects map (_._1)
      val ports: List[Int] = connects map (_._2)
      val nodes = connects map { case (id, port) => Node.builder.withId(id).withAddress("localhost", port).build }
      Atomix.builder
        .withMemberId(id)
        .withAddress("localhost", port)
        .withManagementGroup(PrimaryBackupPartitionGroup.builder("system").withNumPartitions(1).build())
        .withPartitionGroups(RaftPartitionGroup
          .builder("data")
          .withMembers("1")
          .withStorageLevel(StorageLevel.DISK)
          .withDataDirectory(Paths.get(s"${java.util.UUID.randomUUID.toString}.raft").toFile)
          .withNumPartitions(32)
          .build)
        .withMembershipProvider(BootstrapDiscoveryProvider.builder.withNodes(nodes:_*).build)
        .build
    }

    val ports = List(("1", 5020), ("2", 5021), ("3", 5022), ("4", 5023))

    val atomix1 = () => mkAtomix("1", 5020, ports)
    val atomix2 = () => mkAtomix("2", 5021, ports)
    val atomix3 = () => mkAtomix("3", 5022, ports)
    val atomix4 = () => mkAtomix("4", 5023, ports)

    def start(a: Atomix): IO[Atomix] = AsyncAtomicIndexedStore.toF[IO, java.lang.Void](a.start) as a
    def stop(a: Atomix): IO[Unit] = AsyncAtomicIndexedStore.toF[IO, java.lang.Void](a.stop) as (())

    for {
      persistent1 <- IO(new ConcurrentHashMap[String, String]())
      persistent2 <- IO(new ConcurrentHashMap[String, String]())
      persistent3 <- IO(new ConcurrentHashMap[String, String]())
      persistent4 <- IO(new ConcurrentHashMap[String, String]())

      as <- List(start(atomix1()), start(atomix2())).parSequence
      _ <- IO(println("uno"))
      store1 <- mkStore(as(0), "map", persistent1)
      store2 <- mkStore(as(1), "map", persistent2)
      a2 <- List(store1.lookup("foo"), store2.lookup("foo")).parSequence
      _ <- IO(println(s"a2 ::: ${a2}"))
      _ <- store1.insert("foo", "2")
      a2 <- List(store1.lookup("foo"), store2.lookup("foo")).parSequence
      _ <- IO(println(s"a2 ::: ${a2}"))
      _ <- (as map (stop(_))).parSequence
      _ <- IO(println("duo"))

      as <- List(start(atomix1()), start(atomix2()), start(atomix3()), start(atomix4())).parSequence
      store1 <- mkStore(as(0), "map", persistent1, true)
      store2 <- mkStore(as(1), "map", persistent2)
      store3 <- mkStore(as(2), "map", persistent3)
      store4 <- mkStore(as(3), "map", persistent4)

      _ <- store3.insert("foo", "1")
      a1 <- List(store1.lookup("foo"), store2.lookup("foo"), store3.lookup("foo"), store4.lookup("foo")).parSequence
      _ <- (as map (stop(_))).parSequence
      _ <- IO(println(s"a1 ::: ${a1}"))

      as <- List(start(atomix1()), start(atomix2())).parSequence
      _ <- IO(println("SECOND"))
      _ <- IO(println(persistent1.entrySet))
      store1 <- mkStore(as(0), "map", persistent1, true)
      store2 <- mkStore(as(1), "map", persistent2)
      a2 <- List(store1.lookup("foo"), store2.lookup("foo")).parSequence
      _ <- IO(println(s"a2 ::: ${a2}"))
      _ <- store1.insert("foo", "2")
      a3 <- List(store1.lookup("foo"), store2.lookup("foo")).parSequence
      _ <- IO(println(s"a3 ::: ${a3}"))
      _ <- (as map (stop(_))).parSequence
    } yield true

  }

  "three stores connected" >>* {
    val atomix1 = mkAtomix("member-1", "localhost", 5001)
      .build()

    val atomix2 = mkAtomix("member-2", "localhost", 5002)
      .withMembershipProvider(BootstrapDiscoveryProvider.builder()
        .withNodes(Node.builder().withAddress("localhost", 5001).build()).build())
      .build()

    val atomix3 = mkAtomix("member-3", "localhost", 5003)
      .withMembershipProvider(BootstrapDiscoveryProvider.builder()
        .withNodes(Node.builder().withAddress("localhost", 5001).build()).build())
      .build()


    for {
      _ <- IO(atomix1.start().join())
      _ <- IO(atomix2.start().join())
      store1 <- mkStore(atomix1, "map", new ConcurrentHashMap())
      store2 <- mkStore(atomix2, "map", new ConcurrentHashMap())

      _ <- store1.insert("foo", "bar")
      _ <- store2.insert("foo", "baz")

      _ <- IO(atomix3.start().join())
      store3 <- mkStore(atomix3, "map", new ConcurrentHashMap())
      o <- store3.lookup("foo")
      _ <- store3.insert("foo", "quux")

      a1 <- store1.lookup("foo")
      a2 <- store2.lookup("foo")
      a3 <- store3.lookup("foo")
      _ <- IO(atomix1.stop().join())
      _ <- IO(atomix2.stop().join())
      _ <- IO(atomix3.stop().join())
    } yield {
      a1 === Some("quux")
      a2 === Some("quux")
      a3 === Some("quux")
    }
  }
 */
}
 */
