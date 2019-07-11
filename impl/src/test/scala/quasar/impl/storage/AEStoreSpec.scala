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

import cats.effect.{IO, Resource, Timer}
import cats.syntax.functor._

import io.atomix.cluster._

import monocle.Prism

import scalaz.std.string._

import scodec._
import scodec.codecs._

import java.util.concurrent.ConcurrentHashMap
import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import shims._

import AtomixSetup._

final class AEStoreSpec extends IndexedStoreSpec[IO, String, String] {
  sequential

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val strCodec: Codec[String] = utf8_32
  implicit val timer: Timer[IO] = IO.timer(ec)

  val pool = BlockingContext.cached("aestore-pool")

  val defaultNode = AtomixSetup.NodeInfo("default", "localhost", 6000)

  val bytesStringP: Prism[Array[Byte], String] =
    Prism((bytes: Array[Byte]) => Some(new String(bytes, StandardCharsets.UTF_8)))(_.getBytes)

  def startAtomix(me: NodeInfo, seeds: List[NodeInfo]): IO[AtomixCluster] = for {
    atomix <- AtomixSetup.mkAtomix[IO](me, me :: seeds)
    _ <- IO.suspend(cfToAsync[IO, java.lang.Void](atomix.start))
  } yield atomix

  def stopAtomix(atomix: AtomixCluster): IO[Unit] =
    IO.suspend(cfToAsync[IO, java.lang.Void](atomix.stop) as (()))

  def mkStore(me: NodeInfo, seeds: List[NodeInfo]): Resource[IO, IndexedStore[IO, String, String]] = for {
    atomix <- Resource.make(startAtomix(me, seeds))(stopAtomix(_))
    storage <- Resource.pure[IO, ConcurrentHashMap[String, MapValue[String]]](new ConcurrentHashMap[String, MapValue[String]]())
    underlying <- Resource.pure[IO, IndexedStore[IO, String, MapValue[String]]](ConcurrentMapIndexedStore.unhooked[IO, String, MapValue[String]](
      storage, pool))
    store <- AEStore[IO, String, String](s"default", atomix.getCommunicationService(), atomix.getMembershipService(), underlying, pool)
  } yield store

  val emptyStore: Resource[IO, IndexedStore[IO, String, String]] = mkStore(defaultNode, List())
  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "foo" >>* {

    val node0: NodeInfo = NodeInfo("0", "localhost", 6000)
    val node1: NodeInfo = NodeInfo("1", "localhost", 6001)
    val node2: NodeInfo = NodeInfo("2", "localhost", 6002)
    for {
      (store0, finish0) <- mkStore(node0, List(node0, node1)).allocated
      _ <- store0.insert("a", "b")
      (store1, finish1) <- mkStore(node1, List(node0, node1)).allocated
      _ <- timer.sleep(new FiniteDuration(1000, MILLISECONDS))
      start <- timer.clock.monotonic(MILLISECONDS)
      a0 <- store0.lookup("a")
      a1 <- store1.lookup("a")
      _ <- store0.insert("b", "c")
      end <- timer.clock.monotonic(MILLISECONDS)
      _ <- IO(println(s"two lookups and one insert ::: ${end - start}"))
      _ <- timer.sleep(new FiniteDuration(1000, MILLISECONDS))
      start <- timer.clock.monotonic(MILLISECONDS)
      b0 <- store0.lookup("b")
      b1 <- store1.lookup("b")
      end <- timer.clock.monotonic(MILLISECONDS)
      _ <- IO(println(s"two lookups ::: ${end - start}"))
      _ <- finish0
      _ <- finish1
    } yield {
      a0 mustEqual Some("b")
      a1 mustEqual Some("b")
      b0 mustEqual Some("c")
      b1 mustEqual Some("c")
    }
  }

  "bar" >>* {
    val node0: NodeInfo = NodeInfo("0", "localhost", 6006)
    val node1: NodeInfo = NodeInfo("1", "localhost", 6008)
    for {
      atomix0 <- startAtomix(node0, List(node0, node1))
      atomix1 <- startAtomix(node1, List(node0, node1))
      a0 <- IO(atomix0.getMembershipService.getMembers)
      a1 <- IO(atomix1.getMembershipService.getMembers)
      _ <- stopAtomix(atomix0)
      _ <- stopAtomix(atomix1)
    } yield {
      a0.size mustEqual 2
      a1.size mustEqual 2
    }
  }

}
