/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.mimir

import quasar.blueeyes.util.Clock
import quasar.niflheim.{Chef, CookedBlockFormat, V1CookedBlockFormat, V1SegmentFormat, VersionedSegmentFormat, VersionedCookedBlockFormat}
import quasar.precog.common.accounts.AccountFinder
import quasar.precog.common.security.{APIKeyFinder, APIKeyManager, DirectAPIKeyFinder, InMemoryAPIKeyManager, PermissionsFinder}
import quasar.yggdrasil.table.{Slice, VFSColumnarTableModule}
import quasar.yggdrasil.vfs.{ActorVFSModule, SecureVFSModule}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.{CustomRouterConfig, ActorRefRoutee, RouterConfig, RoundRobinGroup, RoundRobinRoutingLogic, Routee, Router}

import scalaz.Monad
import scalaz.std.scalaFuture.futureInstance

import java.io.File
import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.IndexedSeq

object Precog
    extends SecureVFSModule[Future, Slice]
    with ActorVFSModule
    with VFSColumnarTableModule {

  object Config {
    val howManyChefsInTheKitchen: Int = 4
    val cookThreshold: Int = 20000
    val storageTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val quiescenceTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val maxOpenPaths: Int = 500
    val dataDir: File = new File("/tmp")
  }

  // Members declared in quasar.yggdrasil.vfs.ActorVFSModule
  private val emptyAPIKeyManager: APIKeyManager[Future] =
    new InMemoryAPIKeyManager[Future](Clock.System)

  private val apiKeyFinder: APIKeyFinder[Future] =
    new DirectAPIKeyFinder[Future](emptyAPIKeyManager)

  private val accountFinder: AccountFinder[Future] = AccountFinder.Empty[Future]

  def permissionsFinder: PermissionsFinder[Future] =
    new PermissionsFinder(apiKeyFinder, accountFinder, Instant.EPOCH)

  private val actorSystem: ActorSystem =
    ActorSystem("nihdbExecutorActorSystem")

  private def chefs(system: ActorSystem): IndexedSeq[Routee] = (1 to Config.howManyChefsInTheKitchen).map { _ =>
    ActorRefRoutee(system.actorOf(
      Props(Chef(
        VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
        VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))))
  }

  private val routerConfig: RouterConfig = new CustomRouterConfig {
    def createRouter(system: ActorSystem): Router =
      Router(RoundRobinRoutingLogic(), chefs(system))
  }

  private val masterChef: ActorRef =
    actorSystem.actorOf(Props[Chef].withRouter(routerConfig))

  private val clock: Clock = Clock.System

  def resourceBuilder: ResourceBuilder =
    new ResourceBuilder(actorSystem, clock, masterChef, Config.cookThreshold, Config.storageTimeout)

  // Members declared in quasar.yggdrasil.table.ColumnarTableModule
  implicit def M: Monad[Future] = futureInstance

  // Members declared in quasar.yggdrasil.TableModule
  sealed trait TableCompanion extends VFSColumnarTableCompanion 
  object Table extends TableCompanion

  // Members declared in quasar.yggdrasil.table.VFSColumnarTableModule
  private val projectionsActor: ActorRef =
    actorSystem.actorOf(Props(
      new PathRoutingActor(Config.dataDir, Config.storageTimeout, Config.quiescenceTimeout, Config.maxOpenPaths, clock)))

  private val actorVFS: ActorVFS =
    new ActorVFS(projectionsActor, Config.storageTimeout, Config.storageTimeout)

  val vfs: SecureVFS = new SecureVFS(actorVFS, permissionsFinder, clock)
}
