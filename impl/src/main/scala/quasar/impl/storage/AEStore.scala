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

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.Ref
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.functor._

import fs2.Stream
import fs2.concurrent.InspectableQueue
import scalaz.syntax.tag._

import io.atomix.cluster.ClusterMembershipService
import io.atomix.cluster.messaging.ClusterCommunicationService
import io.atomix.cluster.{Member, MemberId}

import scala.collection.JavaConverters._
import java.util.concurrent.{Executors, Executor, ConcurrentMap}
import scala.concurrent.ExecutionContext
import java.util.function.Consumer
import scala.concurrent.duration._

final class AEStore[F[_]: ConcurrentEffect: ContextShift, K, V](
    name: String,
    communication: ClusterCommunicationService,
    membership: ClusterMembershipService,
    sendingRef: Ref[F, AEStore.Pending],
    underlying: ConcurrentMap[K, V],
    blockingPool: BlockingContext)(
    implicit timer: Timer[F])
    extends IndexedStore[F, K, V] {


  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](blockingPool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s.translate(new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    })

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.eval(evalOnPool(Sync[F].delay(underlying.entrySet.iterator.asScala)))
    entry <- evalStreamOnPool(
      Stream.fromIterator[F, java.util.Map.Entry[K, V]](iterator))
  } yield (entry.getKey, entry.getValue)

  def lookup(k: K): F[Option[V]] =
    evalOnPool(Sync[F].delay(Option(underlying.get(k))))

  def insert(k: K, v: V): F[Unit] = evalOnPool(for {
    _ <- Sync[F].delay(underlying.put(k, v))
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify {
      case AEStore.Pending(lst, time, firstAdded) =>
        (AEStore.Pending(Array[Byte]() :: lst, time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield ())

  def delete(k: K): F[Boolean] = evalOnPool(for {
    res <- Sync[F].delay(Option(underlying.remove(k)).nonEmpty)
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    _ <- sendingRef.modify {
      case AEStore.Pending(lst, time, firstAdded) =>
        (AEStore.Pending(Array[Byte]() :: lst, time, if (lst.isEmpty) currentTime else firstAdded), ())
    }
  } yield res)

  private def peers: F[Set[Member]] = Sync[F].delay {
    membership.getMembers.asScala.to[Set]
  }

  def advertisement: F[Stream[F, Unit]] = Sync[F].delay {
    val sendAndWait = Stream.eval(sendAd) *> Stream.sleep(new FiniteDuration(100, MILLISECONDS))
    Stream.sleep(new FiniteDuration(5000, MILLISECONDS)) *> sendAndWait.repeat
  }
  private def sendAd: F[Unit] = Sync[F].delay {
    println("SENDING AD")
  }

  def purgeTombstones: F[Stream[F, Unit]] =
    Sync[F].delay((Stream.sleep(new FiniteDuration(100, MILLISECONDS)) *> Stream.eval(purge)).repeat)

  private def purge: F[Unit] = Sync[F].delay {
    println("PURGING TOMBSTONES")
  }

  private def run(action: F[Unit]) =
    ConcurrentEffect[F].runAsync(action)(_ => IO.unit).unsafeRunSync

  private def handler[A](eventName: String, cb: A => Unit): Unit = {
    val consumer: Consumer[A] = (a) => cb(a)
    communication.subscribe(
      eventName,
      consumer,
      AEStore.blockingContextExecutor(blockingPool))
    ()
  }

  private def enqueue[A](q: InspectableQueue[F, A], eventName: String, maxItems: Int): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(handler[A](eventName, { (a: A) => run {
      q.getSize.flatMap((size: Int) => q.enqueue1(a).whenA(size < maxItems))
    }})))

  private def unsubscribe(eventName: String): F[Unit] = Sync[F].delay {
    communication.unsubscribe(eventName)
  }

  val BootstrapMessage: String = s"aestore::bootstrapping::${name}"
  def bootstrapping: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, BootstrapMessage, 256) *> queue.dequeue
    }
  def bootstrapHandler(as: Array[Byte]): F[Unit] = Sync[F].delay {
    println("bootstrap received")
  }

  val InitMessage: String = s"aestore::initializing::${name}"
  def initializing: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, InitMessage, 32) *> queue.dequeue
    }
  def initHandler(as: Array[Byte]): F[Unit] = Sync[F].delay {
    println("init received")
  }

  val UpdateRequestMessage: String = s"aestore::update-request::${name}"
  def updateRequesing: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, UpdateRequestMessage, 128) *> queue.dequeue
    }
  def updateRequestedHandler(as: Array[Byte]): F[Unit] = Sync[F].delay {
    println("update requested")
  }

  val UpdateReceivedMessage: String = s"aestore::updated::${name}"
  def updateReceived: F[Stream[F, Array[Byte]]] =
    InspectableQueue.unbounded[F, Array[Byte]].map { (queue: InspectableQueue[F, Array[Byte]]) =>
      enqueue(queue, UpdateReceivedMessage, 128) *> queue.dequeue
    }
  def updateReceivedHandler(as: Array[Byte]): F[Unit] = Sync[F].delay {
    println("update received")
  }

  private def sendToAll: F[Unit] = for {
    pids <- peers.map(_.map(_.id))
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    msg <- sendingRef.modify {
      case x@AEStore.Pending(lst, currentTime, firstAdded) => (AEStore.Pending(List(), currentTime, 0L), lst)
    }
    _ <- Sync[F].delay(communication.multicast(
      UpdateReceivedMessage,
      msg,
      pids.to[scala.collection.mutable.Set].asJava))
  } yield ()

  def notifyPeersF: F[Unit] = for {
    currentTime <- timer.clock.monotonic(MILLISECONDS)
    AEStore.Pending(lst, lastTime, firstAdded) <- sendingRef.get
    _ <- if (lst.isEmpty) {
      sendingRef.set(AEStore.Pending(lst, currentTime, firstAdded))
    } else if (currentTime - firstAdded > 50) {
      sendToAll *> sendingRef.set(AEStore.Pending(List(), currentTime, 0L))
    } else if (lst.length > 50) {
      sendToAll *> sendingRef.set(AEStore.Pending(List(), currentTime, 0L))
    } else if (currentTime - lastTime > 200) {
      sendToAll *> sendingRef.set(AEStore.Pending(List(), currentTime, 0L))
    } else { Sync[F].delay(()) }
  } yield ()

  def notifyingPeers: Stream[F, Unit] =
    (Stream.eval(notifyPeersF) *> Stream.sleep(new FiniteDuration(10, MILLISECONDS))).repeat


}

object AEStore {
  final case class Pending(items: List[Array[Byte]], lastSend: Long, firstAdded: Long) extends Product with Serializable

  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  /* OK, so basically we need the following things
   * x) event streams which ignores events if there are too much of them, they are handled on blocking pool
   * x) timed streams which sends advertisements and purge tombstone commands, they are evaled on blocking pool, preferrable single threaded
   * x) batch sender, that is the thing that accumulates events and sends them together to all peers selected by specific function.
   * Note that default peer selection is either one random node or all nodes (it's 6a.m. I'm a bit tired), and this should be somehow
   * adjustable: cluster size?, config?
   * The sender should work on blocking pool too.
   *
   * The `items` field in the atomix lib should be injectable via constructor and be kinda parameterizable
   * 1) We need a function from `Map[K, V]` to `Map[Array[Byte], Array[Byte]]` at least for strings
   * 2) After loading instead of requesting updates with only memberid we can request data with loaded values. We might also bootstrap things
   * using a couple of, this also changes updates communication a bit.
   * 3) `Serializer[A]`, `MapValue` serializer
   *
   *  There is no need in any builder or other fancy interfaces, `AtomixCluster` have all services needed here.
   */

  def apply[F[_]: ConcurrentEffect: ContextShift, K, V](
      name: String,
      communication: ClusterCommunicationService,
      membership: ClusterMembershipService,
      underlying: ConcurrentMap[K, V],
      handlePool: BlockingContext,
      sendingPool: ExecutionContext,
      syncContext: ExecutionContext)(
      implicit timer: Timer[F])
      : F[IndexedStore[F, K, V]] = {
    val BootstrapMessage: String = s"aestore::bootstrapping::${name}"
    val InitMessage: String = s"aestore::initializing::${name}"
    val AntiEntropyMessage: String = s"aestore::anti-entropy::${name}"
    val UpdateRequestMessage: String = s"aestore::update-request::${name}"
    val UpdateReceivedMessage: String = s"aestore::updated::${name}"
    for {
      sendingRef <- Ref.of[F, Pending](Pending(List(), 0L, 0L))
      store <- Sync[F].delay(new AEStore[F, K, V](name, communication, membership, sendingRef, underlying, handlePool))
    } yield store
  }
}
