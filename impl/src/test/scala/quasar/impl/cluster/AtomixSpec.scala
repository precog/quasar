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

package quasar.impl.cluster

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.EffectfulQSpec

import cats.effect.{IO, Resource, Timer, Concurrent}
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.instances.list._

import fs2.Stream

import io.atomix.cluster.{AtomixCluster, Member}

import scodec._
import scodec.codecs._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import Atomix._

class AtomixSpec (implicit ec: ExecutionContext) extends EffectfulQSpec[IO]{
  sequential
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val strCodec: Codec[String] = utf8_32

  def memberId(m: Member): String = m.id.id

  val waitABit: IO[Unit] = timer.sleep(new FiniteDuration(100, MILLISECONDS))
  val waitMore: IO[Unit] = timer.sleep(new FiniteDuration(1000, MILLISECONDS))

  "atomix resource" >> {
    "nodes discovery should work" >>* {
      val node0 = NodeInfo("0", "localhost", 6000)
      val node1 = NodeInfo("1", "localhost", 6001)
      val node2 = NodeInfo("2", "localhost", 6002)
      val seeds = List(node0)
      val nodes = List(node0, node1, node2)
      val clusterRes: Resource[IO, List[AtomixCluster]] =
        nodes.map(Atomix.resource[IO](_, seeds)).sequence

      clusterRes.use { (cs: List[AtomixCluster]) => for {
        _ <- waitABit
        ms0 <- IO(cs(0).getMembershipService.getMembers)
        ms1 <- IO(cs(1).getMembershipService.getMembers)
        ms2 <- IO(cs(2).getMembershipService.getMembers)
      } yield {
        ms0.asScala.toSet.map(memberId(_)) mustEqual Set("0", "1", "2")
        ms1.asScala.toSet.map(memberId(_)) mustEqual Set("0", "1", "2")
        ms2.asScala.toSet.map(memberId(_)) mustEqual Set("0", "1", "2")
      }}
    }
  }
  "atomix cluster" >> {
    val pool = BlockingContext.cached("atomix-spec-pool")
    def resource(me: NodeInfo, seeds: List[NodeInfo]): Resource[IO, Cluster[IO, String]] =
      Atomix.resource[IO](me, seeds).map(Atomix.cluster(_, pool))
    def threeNodeCluster: Resource[IO, List[Cluster[IO, String]]] = {
      val node0 = NodeInfo("0", "localhost", 6000)
      val node1 = NodeInfo("1", "localhost", 6001)
      val node2 = NodeInfo("2", "localhost", 6002)
      val nodes = List(node0, node1, node2)
      nodes.map(resource(_, nodes)).sequence.evalMap { (x: List[Cluster[IO, String]]) =>
        waitABit as x
      }
    }
    def oneSecondStream[A](inp: Stream[IO, A]): Stream[IO, A] =
      inp.interruptAfter(new FiniteDuration(1000, MILLISECONDS))

    "broadcast" >>* {
      threeNodeCluster.use { (clusters: List[Cluster[IO, String]]) => for {
        counter <- Ref.of[IO, Int](0)
        handle = (ix: Int) =>
          clusters(ix).subscribe("broadcast", Int.MaxValue).map(oneSecondStream(_)).map(_.evalMap {
            case (id, _) =>
              counter.modify((x: Int) => (x + 1, ()))
          })
        stream0 <- handle(0)
        stream1 <- handle(1)
        stream2 <- handle(2)

        _ <- Concurrent[IO].start(stream0.compile.drain)
        _ <- Concurrent[IO].start(stream1.compile.drain)
        _ <- Concurrent[IO].start(stream2.compile.drain)

        _ <- clusters(0).broadcast("broadcast", "0")
        _ <- waitABit
        res <- counter.get
      } yield {
        res mustEqual 3
      }}
    }

    "unicast" >>* {
      threeNodeCluster.use { (clusters: List[Cluster[IO, String]]) => for {
        unicasts <- clusters(0).subscribe("unicast", Int.MaxValue).map(oneSecondStream(_))

        handle = (ix: Int) => {
          val cl = clusters(ix)
          cl.subscribe[String]("handshake", Int.MaxValue).map(_.evalMap {
            case (id, _) => cl.unicast("unicast", "0", id)
          })
        }
        stream1 <- handle(1)
        stream2 <- handle(2)
        _ <- Concurrent[IO].start(oneSecondStream(stream1).compile.drain)
        _ <- Concurrent[IO].start(oneSecondStream(stream2).compile.drain)

        _ <- clusters(0).broadcast("handshake", "")
        received <- unicasts.compile.toList
      } yield {
        received.map(_._2) mustEqual List("0", "0")
      }}
    }
    "gossip" >>* {
      threeNodeCluster.use { (clusters: List[Cluster[IO, String]]) => for {
        messages <- Ref.of[IO, Set[String]](Set())
        handle = (ix: Int) =>
          clusters(ix).subscribe[String]("gossip", Int.MaxValue).map(oneSecondStream(_)).map(_.evalMap {
            case (id, msg) => messages.modify((old: Set[String]) => (old + msg, ()))
          })
        stream0 <- handle(0)
        stream1 <- handle(1)
        stream2 <- handle(2)

        _ <- Concurrent[IO].start(stream0.compile.drain)
        _ <- Concurrent[IO].start(stream1.compile.drain)
        _ <- Concurrent[IO].start(stream2.compile.drain)

        _ <- clusters(0).gossip("gossip", "one")
        _ <- clusters(1).gossip("gossip", "two")
        _ <- clusters(2).gossip("gossip", "three")
        _ <- waitMore
        res <- messages.get
      } yield {
        res mustEqual Set("one", "two", "three")
      }}
    }
    "subscription event stream is limited" >>* {
      threeNodeCluster.use { (clusters: List[Cluster[IO, String]]) => for {
        stream <- clusters(0).subscribe[String]("limited", 2).map(oneSecondStream(_))
        _ <- clusters(1).broadcast("limited", "foo")
        _ <- clusters(1).broadcast("limited", "bar")
        _ <- clusters(2).broadcast("limited", "baz")
        _ <- clusters(2).broadcast("limited", "quux")
        _ <- clusters(0).broadcast("limited", "ololo")
        _ <- clusters(0).broadcast("limited", "trololo")
        _ <- waitMore
        res <- stream.compile.toList
      } yield {
        // It is not guaranteed to be exactly 2 (e.g. it can be `1 + numOfSimultaneousEvents`), but in most cases it is 2
        res.length mustEqual 2
      }}
    }
  }
}
