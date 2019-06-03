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

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.{Applicative, MonadError}
import cats.instances.list._
import cats.kernel.instances.int._
import cats.syntax.eq._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import cats.syntax.applicativeError._

import fs2.io.tcp.Socket

import quasar.concurrent.NamedDaemonThreadFactory

import io.atomix.core._
import io.atomix.cluster._
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup
import io.atomix.protocols.raft.partition.RaftPartitionGroup
import io.atomix.storage.StorageLevel

import java.util.concurrent.CompletableFuture
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.spi.AsynchronousChannelProvider
import java.nio.file.Path

object AtomixSetup {
  final case class NodeInfo(memberId: String, host: String, port: Int) extends Product with Serializable

  val SystemGroupName: String = "system"
  val SystemPartitionsNum: Int = 1
  val DataGroupName: String = "data"
  val DataPartitionsNum: Int = 32
  val AuxGroupName: String = "aux"

  def nodeAddress(node: NodeInfo): InetSocketAddress =
    new InetSocketAddress(node.host, node.port)

  def atomixNode(node: NodeInfo): Node =
    Node.builder().withAddress(node.host, node.port).withId(node.memberId).build()

  def apply[F[_]: Concurrent: ContextShift: MonadError[?[_], Throwable]](
      thisNode: NodeInfo,
      seeds: List[NodeInfo],
      logPath: Path,
      threadPrefix: String)
      : Resource[F, Atomix] = {
    implicit val asyncChannelGroup: AsynchronousChannelGroup =
      AsynchronousChannelProvider.provider()
        .openAsynchronousChannelGroup(8, NamedDaemonThreadFactory(threadPrefix))

    def checkNode(node: NodeInfo): F[Option[NodeInfo]] = {
      val canConnectPair: Resource[F, Boolean] =
        Socket.client[F](nodeAddress(node))
          .map((x: Socket[F]) => true)
          .handleError(e => false)

      canConnectPair use { (x: Boolean) =>
        (if (x) Some(node) else None).pure[F]
      }
    }

    def mkRun(seed: NodeInfo, d: Deferred[F, List[NodeInfo]], ref: Ref[F, List[Option[NodeInfo]]]): F[Unit] = for {
      opt <- checkNode(seed)
      consed <- ref.modify { x =>
        val consed = opt :: x
        (consed, consed)
      }
      _ <- if (consed.length === seeds.length) {
        val compacted: List[NodeInfo] = consed collect { case Some(x) => x }
        d.complete(compacted)
      } else Applicative[F].point(())

    } yield ()

    val fConnected: F[List[NodeInfo]] =  for {
      d <- Deferred.tryable[F, List[NodeInfo]]
      ref <- Ref.of[F, List[Option[NodeInfo]]](List[Option[NodeInfo]]())
      _ <- seeds traverse { x => Concurrent[F].start(mkRun(x, d, ref)) }
      res <- d.get
    } yield res

    def fAtomix: F[Atomix] = for {
      _ <- recursiveDelete(logPath.toFile)
      connected <- fConnected
      atomix <- mkAtomix(thisNode, (thisNode :: connected), logPath)
      _ <- cfToAsync(atomix.start)
      postConnected <- fConnected
      result <- if (connected.length < postConnected.length / 2) for {
        _ <- cfToAsync(atomix.stop)
        res <- Sync[F].suspend(fAtomix)
      } yield res
      else Sync[F].delay(atomix)
    } yield result

    Resource.make(fAtomix) { atomix => Sync[F].suspend(cfToAsync(atomix.stop)) as (()) }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def recursiveDelete[F[_]: Sync](file: java.io.File): F[Unit] = {
    val delete: F[Unit] = Sync[F].delay(file.delete()) as (())

    if (file.isDirectory) {
      Option(file.listFiles()) match {
        case None =>
          println("no files")
          delete
        case Some(xs) =>
          xs.toList.traverse(recursiveDelete(_)) *> delete
      }
    }
    else delete
  }

  private[this] def mkAtomix[F[_]: Sync](
      thisNode: NodeInfo,
      connectedSeeds: List[NodeInfo],
      path: Path)
      : F[Atomix] = Sync[F].delay {
    val nodeList: List[Node] =
      connectedSeeds map (atomixNode(_))

    val raftPartition = RaftPartitionGroup
      .builder(DataGroupName)
      .withMembers((connectedSeeds map (_.memberId)):_*)
      .withStorageLevel(StorageLevel.DISK)
      .withDataDirectory(path.toFile)
      .withNumPartitions(DataPartitionsNum)
      .build()

    Atomix.builder()
      .withMemberId(thisNode.memberId)
      .withAddress(thisNode.host, thisNode.port)
      .withManagementGroup(PrimaryBackupPartitionGroup
        .builder(SystemGroupName)
        .withNumPartitions(SystemPartitionsNum)
        .build())
      .withPartitionGroups(raftPartition)
      .withMembershipProvider(BootstrapDiscoveryProvider
        .builder()
        .withNodes(nodeList:_*)
        .build())
      .build()
  }

  def cfToAsync[F[_]: Async: ContextShift, A](cf: CompletableFuture[A]): F[A] = {
    if (cf.isDone)
      cf.get.pure[F]
    else {
      Async[F].async { (cb: Either[Throwable, A] => Unit)  =>
        val _ = cf.whenComplete { (res: A, t: Throwable) => cb(Option(t).toLeft(res)) }
      } productL ContextShift[F].shift
    }
  }
}
