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

import quasar.{concurrent => qc}
import quasar.EffectfulQSpec

import cats.effect.{IO, Resource, Timer, Concurrent}
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import cats.syntax.flatMap._
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

  val waitABit: IO[Unit] = timer.sleep(new FiniteDuration(300, MILLISECONDS))
  val waitMore: IO[Unit] = timer.sleep(new FiniteDuration(2000, MILLISECONDS))

  val portRef: Ref[IO, Int] = Ref.unsafe[IO, Int](6000)

  def mkNode(id: String): IO[NodeInfo] = for {
    port <- portRef.modify((x: Int) => (x + 1, x + 1))
  } yield NodeInfo(id, "localhost", port)

  "atomix resource" >> {
    "nodes discovery should work" >>* {
      val clusterRes: Resource[IO, List[AtomixCluster]] = {
        val ioRes: IO[Resource[IO, List[AtomixCluster]]] =for {
          node0 <- mkNode("0")
          node1 <- mkNode("1")
          node2 <- mkNode("2")
          seeds = List(node0)
          nodes = List(node0, node1, node2)
        } yield nodes.map(Atomix.resource[IO](_, seeds.map(_.address))).sequence
        Resource.liftF(ioRes).flatten
      }

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
    val blocker = qc.Blocker.cached("atomix-spec-pool")
    def resource(me: NodeInfo, seeds: List[NodeAddress]): Resource[IO, Cluster[IO, String]] =
      Atomix.resource[IO](me, seeds).map(Atomix.cluster(_, blocker))
    def threeNodeCluster: Resource[IO, List[Cluster[IO, String]]] = {
      val ioRes: IO[Resource[IO, List[Cluster[IO, String]]]] = for {
        node0 <- mkNode("0")
        node1 <- mkNode("1")
        node2 <- mkNode("2")
        nodes = List(node0, node1, node2)
      } yield nodes.map(resource(_, nodes.map(_.address))).sequence
      Resource.liftF(ioRes).flatten.evalMap { (x: List[Cluster[IO, String]]) =>
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
        _ <- waitABit
        _ <- clusters(1).broadcast("limited", "bar")
        _ <- waitABit
        _ <- clusters(2).broadcast("limited", "baz")
        _ <- waitABit
        _ <- clusters(2).broadcast("limited", "quux")
        _ <- waitABit
        _ <- clusters(0).broadcast("limited", "ololo")
        _ <- waitABit
        _ <- clusters(0).broadcast("limited", "trololo")
        _ <- waitMore
        res <- stream.compile.toList
      } yield {
        res.length mustEqual 2
      }}
    }
  }
}
