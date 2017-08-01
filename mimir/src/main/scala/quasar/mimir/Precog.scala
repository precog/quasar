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
import quasar.niflheim.{Chef, V1CookedBlockFormat, V1SegmentFormat, VersionedSegmentFormat, VersionedCookedBlockFormat}
import quasar.precog.common.accounts.AccountFinder

import quasar.precog.common.security.{
  APIKey,
  APIKeyFinder,
  APIKeyManager,
  DirectAPIKeyFinder,
  InMemoryAPIKeyManager,
  PermissionsFinder
}

import quasar.yggdrasil.table.VFSColumnarTableModule
import quasar.yggdrasil.vfs.SerialVFS

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.{
  ActorRefRoutee,
  CustomRouterConfig,
  RouterConfig,
  RoundRobinRoutingLogic,
  Routee,
  Router
}

import delorean._

import fs2.async
import fs2.interop.scalaz._

import org.slf4s.Logging

import scalaz.Monad
import scalaz.concurrent.Task
import scalaz.std.scalaFuture.futureInstance

import java.io.File
import java.time.Instant
import java.util.concurrent.CountDownLatch

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.collection.immutable.IndexedSeq

// calling this constructor is a side-effect; you must always shutdown allocated instances
final class Precog private (dataDir0: File)
    extends VFSColumnarTableModule
    with TablePagerModule
    with StdLibModule[Future] {

  object Library extends StdLib

  object Config {
    val howManyChefsInTheKitchen: Int = 4
    val quiescenceTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val maxOpenPaths: Int = 500
    val dataDir: File = dataDir0
  }

  val CookThreshold: Int = 20000
  val StorageTimeout: FiniteDuration = 300.seconds

  private var _vfs: SerialVFS = _
  def vfs = _vfs

  private val vfsLatch = new CountDownLatch(1)

  private val vfsShutdownSignal =
    async.signalOf[Task, Option[Unit]](Some(())).unsafePerformSync

  {
    // setup VFS stuff as a side-effect (and a race condition!)
    val vfsStr = SerialVFS(dataDir0).evalMap { v =>
      Task delay {
        _vfs = v
        vfsLatch.countDown()
      }
    }

    val gated = vfsStr.mergeHaltBoth(vfsShutdownSignal.discrete.noneTerminate.drain)

    Precog.startTask(gated.run, vfsLatch.countDown()).unsafePerformSync
    vfsLatch.await()      // sigh....
  }

  // for the time being, do everything with this key
  def RootAPIKey: Future[APIKey] = emptyAPIKeyManager.rootAPIKey

  // Members declared in quasar.yggdrasil.vfs.ActorVFSModule
  private lazy val emptyAPIKeyManager: APIKeyManager[Future] =
    new InMemoryAPIKeyManager[Future](Clock.System)

  private val apiKeyFinder: APIKeyFinder[Future] =
    new DirectAPIKeyFinder[Future](emptyAPIKeyManager)

  private val accountFinder: AccountFinder[Future] = AccountFinder.Singleton(RootAPIKey)

  def permissionsFinder: PermissionsFinder[Future] =
    new PermissionsFinder(apiKeyFinder, accountFinder, Instant.EPOCH)

  val actorSystem: ActorSystem =
    ActorSystem("nihdbExecutorActorSystem")

  private val props: Props = Props(Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))

  private def chefs(system: ActorSystem): IndexedSeq[Routee] =
    (1 to Config.howManyChefsInTheKitchen).map { _ =>
      ActorRefRoutee(system.actorOf(props))
    }

  private val routerConfig: RouterConfig = new CustomRouterConfig {
    def createRouter(system: ActorSystem): Router =
      Router(RoundRobinRoutingLogic(), chefs(system))
  }

  // needed for nihdb
  val masterChef: ActorRef =
    actorSystem.actorOf(props.withRouter(routerConfig))

  private val clock: Clock = Clock.System

  // Members declared in quasar.yggdrasil.table.ColumnarTableModule
  implicit def M: Monad[Future] = futureInstance

  // Members declared in quasar.yggdrasil.TableModule
  sealed trait TableCompanion extends VFSColumnarTableCompanion
  object Table extends TableCompanion

  def shutdown: Future[Unit] = {
    for {
      _ <- vfsShutdownSignal.set(None).unsafeToFuture
      _ <- actorSystem.terminate.map(_ => ())
    } yield ()
  }
}

object Precog extends Logging {

  def apply(dataDir: File): Task[Precog] =
    Task.delay(new Precog(dataDir))

  // utility function for running a Task in the background
  def startTask(ta: Task[_], cb: => Unit): Task[Unit] =
    Task.delay(ta.unsafePerformAsync(_.fold(
      ex => {
        log.error(s"exception in background task", ex)
        cb
      },
      _ => ())))
}
