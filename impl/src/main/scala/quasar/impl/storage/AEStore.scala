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

import cats.effect._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.Stream
import scalaz.syntax.tag._

import io.atomix.cluster.ClusterMembershipService
import io.atomix.cluster.messaging.ClusterCommunicationService
import io.atomix.cluster.{Member, MemberId}

import scala.collection.JavaConverters._
import java.util.concurrent.{Executors, Executor}
import scala.concurrent.ExecutionContext
import java.util.function.Consumer
import scala.concurrent.duration._

final class AEStore[F[_]: Async: ContextShift, K, V](
    name: String,
    communication: ClusterCommunicationService,
    membership: ClusterMembershipService)
    extends IndexedStore[F, K, V] {

  def entries: Stream[F, (K, V)] = ???
  def lookup(k: K): F[Option[V]] = ???
  def insert(k: K, v: V): F[Unit] = ???
  def delete(k: K): F[Boolean] = ???

  private def peers: F[Set[Member]] = Sync[F].delay {
    membership.getMembers.asScala.to[Set]
  }

  private def sendAd: F[Unit] = Sync[F].delay {
    println("SENDING AD")
  }
  private def purgeTombstones: F[Unit] = Sync[F].delay {
    println("PURGING TOMBSTONES")
  }
  private def handleBootstrap(id: MemberId): F[Unit] = Sync[F].delay {
    println("HANDLING BOOTSTRAP")
  }
  private def init(bs: Array[Byte]): F[Unit] = Sync[F].delay {
    println("INITIALIZING")
  }
  private def requestUpdate(id: MemberId): F[Unit] = Sync[F].delay {
    println("REQUEST UPDATE")
  }
  private def updateReceived(bs: Array[Byte]): F[Unit] = Sync[F].delay {
    println("UPDATE RECEIVED")
  }
  private def antiEntropy(bs: Array[Byte]): F[Unit] = Sync[F].delay {
    println("HANDLE ANTY ENTROPY")
  }
  private def sendUpdate: F[Unit] = Sync[F].delay {
    println("SENDING UPDATE")
  }
}

object AEStore {
  def blockingContextExecutor(pool: BlockingContext): Executor = new Executor {
    def execute(r: java.lang.Runnable) = pool.unwrap.execute(r)
  }

  /* OK, so basically we need the following things
   * 1) event streams which ignores events if there are too much of them, they are handled on blocking pool
   * 2) timed streams which sends advertisements and purge tombstone commands, they are evaled on blocking pool, preferrable single threaded
   * 3) batch sender, that is the thing that accumulates events and sends them together to all peers selected by specific function.
   * Note that default peer selection is either one random node or all nodes (it's 6a.m. I'm a bit tired), and this should be somehow
   * adjustable: cluster size?, config?
   * The sender should work on blocking pool too.
   *
   * The `items` field in the atomix lib should be injectable via constructor and be kinda parameterizable
   * 1) We need a function from `Map[K, V]` to `Map[Array[Byte], Array[Byte]]` at least for strings
   * 2) After loading instead of requesting updates with only memberid we can request data with loaded values. We might also bootstrap things
   * using a couple of, this also changes updates communication a bit.
   * 3) `Serializer[A]`, `MapValue` serializer
   */

  def apply[F[_]: ConcurrentEffect: ContextShift, K, V](
      name: String,
      communication: ClusterCommunicationService,
      membership: ClusterMembershipService,
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
      store <- Sync[F].delay(new AEStore[F, K, V](name, communication, membership))
      _ <- Concurrent[F].start {
        def repeat: F[Unit] =
          store.sendAd *> timer.sleep(new FiniteDuration(100, MILLISECONDS)) *> ContextShift[F].shift *> repeat
        ContextShift[F].evalOn(syncContext)(repeat)
      }
      _ <- Concurrent[F].start {
        def repeat: F[Unit] =
          store.purgeTombstones *> timer.sleep(new FiniteDuration(400, MILLISECONDS)) *> ContextShift[F].shift *> repeat
        ContextShift[F].evalOn(syncContext)(repeat)
      }
      bootstrapConsumer: Consumer[MemberId] = (m: MemberId) =>
        Effect[F].runAsync(store.handleBootstrap(m))(x => IO.unit).unsafeRunSync
      _ <- Sync[F].delay(communication.subscribe(
        BootstrapMessage,
        bootstrapConsumer,
        blockingContextExecutor(handlePool)))
      initConsumer: Consumer[Array[Byte]] = (a: Array[Byte]) =>
        Effect[F].runAsync(store.init(a))(x => IO.unit).unsafeRunSync
      _ <- Sync[F].delay(communication.subscribe(
        InitMessage,
        initConsumer,
        blockingContextExecutor(handlePool)))
      requestConsumer: Consumer[MemberId] = (m: MemberId) =>
        Effect[F].runAsync(store.requestUpdate(m))(x => IO.unit).unsafeRunSync
      _ <- Sync[F].delay(communication.subscribe(
        UpdateRequestMessage,
        requestConsumer,
        blockingContextExecutor(handlePool)))
      updateConsumer: Consumer[Array[Byte]] = (a: Array[Byte]) =>
        Effect[F].runAsync(store.updateReceived(a))(x => IO.unit).unsafeRunSync
      _ <- Sync[F].delay(communication.subscribe(
        UpdateReceivedMessage,
        updateConsumer,
        blockingContextExecutor(handlePool)))
      adConsumer: Consumer[Array[Byte]] = (a: Array[Byte]) =>
        Effect[F].runAsync(store.updateReceived(a))(x => IO.unit).unsafeRunSync
      _ <- Sync[F].delay(communication.subscribe(
        AntiEntropyMessage,
        adConsumer,
        blockingContextExecutor(handlePool)))
      _ <- Concurrent[F].start(ContextShift[F].evalOn(handlePool.unwrap)(store.sendUpdate))

    } yield store
  }
}
