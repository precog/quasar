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
import cats.syntax.applicative._
import cats.syntax.apply._

import io.atomix.cluster._
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider

import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture

object AtomixSetup {
  final case class NodeInfo(memberId: String, host: String, port: Int) extends Product with Serializable

  def nodeAddress(node: NodeInfo): InetSocketAddress =
    new InetSocketAddress(node.host, node.port)

  def atomixNode(node: NodeInfo): Node =
    Node.builder().withAddress(node.host, node.port).withId(node.memberId).build()

  def mkAtomix[F[_]: Sync](
      thisNode: NodeInfo,
      connectedSeeds: List[NodeInfo])
      : F[AtomixCluster] = Sync[F].delay {
    val nodeList: List[Node] =
      connectedSeeds map (atomixNode(_))

    AtomixCluster.builder()
      .withMemberId(thisNode.memberId)
      .withAddress(thisNode.host, thisNode.port)
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
      Async[F].suspend(Async[F].async { (cb: Either[Throwable, A] => Unit)  =>
        val _ = cf.whenComplete { (res: A, t: Throwable) =>
          cb(Option(t).toLeft(res)) }
      } productL ContextShift[F].shift)
    }
  }
}
