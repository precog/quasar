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

import cats.effect.IO
import cats.syntax.functor._

import quasar.concurrent.BlockingContext

import io.atomix.core._
import io.atomix.core.map._
import io.atomix.utils.time.Versioned
import io.atomix.cluster.discovery._
import io.atomix.cluster.Node
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

final class MapDBAsyncAtomicMapSpec extends IndexedStoreSpec[IO, String, String] with AfterAll with BeforeAll{
  val pool = BlockingContext.cached("mapdb-async-atomic-map")

  def mkAtomix(memberId: String, host: String, port: Int) =
    Atomix.builder()
      .withMemberId(memberId)
      .withAddress(host, port)
      .withManagementGroup(PrimaryBackupPartitionGroup.builder("system").withNumPartitions(1).build())
      .withPartitionGroups(PrimaryBackupPartitionGroup.builder("data").withNumPartitions(32).build())

  val atomix: Atomix =
    mkAtomix("member-1", "localhost", 5000).build()

  def beforeAll: Unit = {
    atomix.start().join()
  }
  def afterAll: Unit = atomix.stop().join()

  def mkStore(atomix: Atomix, mapName: String, mMap: ConcurrentHashMap[String, Versioned[String]]): IO[IndexedStore[IO, String, String]] = {
    val mkMap = for {
      aMap <- IO{
        atomix.atomicMapBuilder[String, String](mapName)
          .build()
          .async()
      }
      map <- MapDBAsyncAtomicMap[IO, String, String](aMap, mMap, pool)
    } yield map
    mkMap flatMap {
      case None => emptyStore
      case Some(m) => IO(AsyncAtomicIndexedStore(m))
    }
  }

  val emptyStore = IO(java.util.UUID.randomUUID.toString()) flatMap (mkStore(atomix, _, new ConcurrentHashMap()))

  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "restore" >>* {
    val atomix1 = mkAtomix("member-1", "localhost", 5004)
      .build()

    val prefilled = {
      val res = new ConcurrentHashMap[String, Versioned[String]]()
      res.put("prefilled-key", new Versioned("prefilled", 1, java.lang.System.currentTimeMillis()))
      res
    }

    for {
      _ <- IO(atomix1.start().join())
      store1 <- mkStore(atomix1, "map", prefilled)
      pv <- store1.lookup("prefilled-key")
      _ <- IO(atomix1.stop().join())
    } yield {
      pv === Some("prefilled")
    }
  }

  "split brain" >>* {
    val atomix1 = mkAtomix("member-1", "localhost", 5005)
      .build()
    val atomix2 = mkAtomix("member-2", "localhost", 5006)
      .withMembershipProvider(BootstrapDiscoveryProvider.builder()
        .withNodes(Node.builder().withAddress("localhost", 5005).build()).build())
      .build()
    val atomix3 = mkAtomix("member-3", "localhost", 5007)
      .withMembershipProvider(BootstrapDiscoveryProvider.builder()
        .withNodes(Node.builder().withAddress("localhost", 5005).build()).build())
      .build()

    val prefilled = {
      val res = new ConcurrentHashMap[String, Versioned[String]]()
      res.put("prefilled-key", new Versioned("prefilled", 4, java.lang.System.currentTimeMillis() - 1000))
      res
    }
    val newerPrefilled = {
      val res = new ConcurrentHashMap[String, Versioned[String]]()
      res.put("prefilled-key", new Versioned("prefilled-newer", 2, java.lang.System.currentTimeMillis()))
      res
    }
    val empty = new ConcurrentHashMap[String, Versioned[String]]()

    for {
      _ <- IO(atomix1.start().join())
      _ <- IO(atomix2.start().join())
      _ <- IO(atomix3.start().join())

      store1 <- mkStore(atomix1, "map", newerPrefilled)
      store2 <- mkStore(atomix2, "map", prefilled)
      store3 <- mkStore(atomix3, "map", newerPrefilled)

      a1 <- store1.lookup("prefilled-key")
      a2 <- store2.lookup("prefilled-key")
      a3 <- store3.lookup("prefilled-key")

      _ <- IO(println(s"a1 ::: ${a1}    a2 ::: ${a2}    a3 ::: ${a3}"))

      _ <- IO(atomix1.stop().join())
      _ <- IO(atomix2.stop().join())
      _ <- IO(atomix3.stop().join())
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
      o === Some("baz")
      a1 === Some("quux")
      a2 === Some("quux")
      a3 === Some("quux")
    }
  }
}
