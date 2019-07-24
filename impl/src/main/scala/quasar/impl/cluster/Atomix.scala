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

import cats.effect.{Sync, Async, ConcurrentEffect, ContextShift, IO, Resource}
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.instances.int._
import cats.instances.list._

import fs2.Stream
import fs2.concurrent.InspectableQueue

import scalaz.syntax.tag._

import io.atomix.cluster.{AtomixCluster, MemberId, Member, ClusterMembershipService, Node, ClusterConfig}
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider
import io.atomix.cluster.messaging.ClusterCommunicationService
import io.atomix.utils.net.Address

import org.slf4s.Logging

import scodec.{Codec, Attempt}
import scodec.bits.{ByteVector, BitVector}

import java.net.InetAddress
import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function.BiConsumer

import scala.collection.JavaConverters._

object Atomix extends Logging {
  final case class NodeInfo(id: String, host: String, port: Int)

  private def atomixNode(node: NodeInfo): Node =
    Node.builder.withAddress(node.host, node.port).withId(node.id).build()

  def resource[F[_]: Async: ContextShift](me: NodeInfo, seeds: List[NodeInfo]): Resource[F, AtomixCluster] =
    Resource.make(atomix[F](me, seeds).flatMap((ax: AtomixCluster) => start(ax) as ax))(stop(_))

  private def atomix[F[_]: Sync](me: NodeInfo, seeds: List[NodeInfo]): F[AtomixCluster] = Sync[F].delay {
    AtomixCluster.builder(new ClusterConfig())
      .withMemberId(me.id)
      .withAddress(me.host, me.port)
      .withMembershipProvider(BootstrapDiscoveryProvider
        .builder
        .withNodes(seeds.map(atomixNode(_)):_*)
        .build())
      .build()
  }

  private def start[F[_]: Async: ContextShift](cluster: AtomixCluster): F[Unit] =
    Sync[F].suspend(cfToAsync(cluster.start).void)

  private def stop[F[_]: Async: ContextShift](cluster: AtomixCluster): F[Unit] =
    Sync[F].suspend(cfToAsync(cluster.stop).void)

  private def cfToAsync[F[_]: Async: ContextShift, A](cf: CompletableFuture[A]): F[A] =
    if (cf.isDone) cf.get.pure[F]
    else Async[F].async { (cb: Either[Throwable, A] => Unit) =>
      val _ = cf.whenComplete((res: A, t: Throwable) => cb(Option(t).toLeft(res)))
    } productL ContextShift[F].shift


  private def membership[F[_]: Sync](service: ClusterMembershipService): Membership[F, MemberId] = new Membership[F, MemberId] {
    val F = Sync[F]
    def localId: F[MemberId] =
      F.delay(service.getLocalMember.id)

    def peers: F[Set[MemberId]] =
      F.delay(service.getMembers.asScala.to[Set].map(_.id))

    def random: F[Set[MemberId]] =
      peers

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def byAddress(addr: InetAddress, port: Int): F[Option[MemberId]] = {
      def theSame(check: Member): F[Boolean] = for {
        checkAddr <- F.delay(check.address.address(true))
        addressMatch <- F.delay(checkAddr.equals(addr))
      } yield addressMatch && check.address.port === port
      service.getMembers.asScala.toList.findM(theSame(_)).map(_.map(_.id))
    }
  }

  private def communication[F[_]: ConcurrentEffect: ContextShift](
      service: ClusterCommunicationService,
      membership: Membership[F, MemberId],
      pool: BlockingContext)
      : Communication[F, MemberId, String] = new Communication[F, MemberId, String] {
    val F = ConcurrentEffect[F]

    def unicast[P: Codec](tag: String, payload: P, target: MemberId): F[Unit] =
      Codec[P].encode(payload).map((b: BitVector) => {
        cfToAsync(service.unicast(
          tag,
          b.toByteArray,
          target)).void
      }).getOrElse(F.delay {
        log.warn(s"incorrect message was sent by unicast ::: ${payload}, to ::: ${target}")
      })

    def multicast[P: Codec](tag: String, payload: P, targets: Set[MemberId]): F[Unit] =
      if (targets.isEmpty) F.delay(())
      else
        Codec[P].encode(payload).map((b: BitVector) => {
          F.delay(service.multicast(
            tag,
            b.toByteArray,
            targets.asJava))
        }).getOrElse(F.delay {
          log.warn(s"incorrect message sent by multicast ::: ${payload}, to ::: ${targets}")
        })

    def subscribe[P: Codec](tag: String, limit: Int): F[Stream[F, (MemberId, P)]] =
      InspectableQueue.unbounded[F, (MemberId, P)]
        .map((queue: InspectableQueue[F, (MemberId, P)]) => enqueue[P](queue, tag, limit) *> queue.dequeue)
        .map(_.onFinalize(F.delay(service.unsubscribe(tag))))

    private def enqueue[P: Codec](q: InspectableQueue[F, (MemberId, P)], eventType: String, maxItems: Int): Stream[F, Unit] = {
      Stream.eval(handler(eventType, { (addr: Address, a: BitVector) => Codec[P].decode(a) match {
        case Attempt.Failure(_) => ()
        case Attempt.Successful(d) => run(for {
          netAddr <- F.delay(addr.address(true))
          port <- F.delay(addr.port)
          mid <- membership.byAddress(netAddr, port)
          size <- q.getSize
          _ <- mid match {
            case None => F.delay {
              log.warn(s"incorrect message received\neventType ::: ${eventType}\nsource ::: ${addr}\nbits ::: ${a.toHex}")
            }
            case Some(_) if size >= maxItems => F.delay {
              log.warn(s"there are too much events in subscription queue\neventType ::: ${eventType}\nsize ::: ${size}")
            }
            case Some(id) => q.enqueue1((id, d.value))
          }
        } yield())
      }}))
    }
    private def handler(eventName: String, cb: (Address, BitVector) => Unit): F[Unit] = {
      val biconsumer: BiConsumer[Address, Array[Byte]] = (addr, bytes) => {
        cb(addr, ByteVector(bytes).bits)
      }
      cfToAsync(service.subscribe[Array[Byte]](
        eventName,
        biconsumer,
        blockingContextExecutor(pool))).void
    }

    private def run(action: F[Unit]) =
      F.runAsync(action)(_ => IO.unit).unsafeRunSync
  }

  def cluster[F[_]: ConcurrentEffect: ContextShift](
      atomix: AtomixCluster,
      pool: BlockingContext)
      : Cluster[F, String] = new Cluster[F, String] {

    type Id = MemberId

    val membership: Membership[F, Id] = Atomix.membership[F](atomix.getMembershipService())
    val communication: Communication[F, Id, String] = Atomix.communication[F](atomix.getCommunicationService(), membership, pool)

    def gossip[P: Codec](msg: String, p: P): F[Unit] = for {
      targets <- membership.random
      _ <- communication.multicast(msg, p, targets)
      _ <- ContextShift[F].shift
    } yield ()

    def unicast[P: Codec](msg: String, p: P, id: Id): F[Unit] =
      communication.unicast(msg, p, id) *> ContextShift[F].shift

    def broadcast[P: Codec](msg: String, p: P): F[Unit] = for {
      targets <- membership.peers
      _ <- communication.multicast(msg, p, targets)
      _ <- ContextShift[F].shift
    } yield ()

    def subscribe[P: Codec](msg: String, limit: Int): F[Stream[F, (Id, P)]] =
      communication.subscribe(msg, limit)
  }

  private def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }
}
