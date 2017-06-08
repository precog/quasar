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

import quasar.blueeyes.json.JValue
import quasar.blueeyes.util.Clock
import quasar.niflheim.{Chef, CookedBlockFormat, V1CookedBlockFormat, V1SegmentFormat, VersionedSegmentFormat, VersionedCookedBlockFormat}
import quasar.precog.common.Path
import quasar.precog.common.ingest.{EventId, IngestMessage, IngestRecord, StreamRef}
import quasar.precog.common.accounts.AccountFinder

import quasar.precog.common.security.{
  APIKey,
  APIKeyFinder,
  APIKeyManager,
  Authorities,
  DirectAPIKeyFinder,
  InMemoryAPIKeyManager,
  PermissionsFinder
}

import quasar.yggdrasil.PathMetadata
import quasar.yggdrasil.table.{Slice, VFSColumnarTableModule}
import quasar.yggdrasil.vfs.{ActorVFSModule, ResourceError, SecureVFSModule}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.{CustomRouterConfig, ActorRefRoutee, RouterConfig, RoundRobinGroup, RoundRobinRoutingLogic, Routee, Router}

import scalaz.{EitherT, Monad, StreamT}
import scalaz.std.scalaFuture.futureInstance
import scalaz.syntax.show._

import java.io.File
import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.IndexedSeq

// calling this constructor is a side-effect; you must always shutdown allocated instances
class Precog(dataDir0: File)
    extends SecureVFSModule[Future, Slice]
    with ActorVFSModule
    with VFSColumnarTableModule {

  object Config {
    val howManyChefsInTheKitchen: Int = 4
    val cookThreshold: Int = 20000
    val storageTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val quiescenceTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val maxOpenPaths: Int = 500
    val dataDir: File = dataDir0
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

  private val actorSystem: ActorSystem =
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

  private val masterChef: ActorRef =
    actorSystem.actorOf(props.withRouter(routerConfig))

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

  def showContents(path: Path): EitherT[Future, ResourceError, Set[PathMetadata]] =
    actorVFS.findDirectChildren(path)

  val vfs: SecureVFS = new SecureVFS(actorVFS, permissionsFinder, clock)

  // TODO this could be trivially rewritten with fs2.Stream
  def ingest(path: Path, chunks: StreamT[Future, Vector[JValue]]): Future[Unit] = {
    val streamId = java.util.UUID.randomUUID()

    val stream = StreamT.unfoldM((0, chunks)) {
      case (pseudoOffset, chunks) =>
        chunks.uncons flatMap {
          case Some((chunk, tail)) =>
            val ingestRecords = chunk.zipWithIndex map {
              case (v, i) => IngestRecord(EventId(pseudoOffset, i), v)
            }

            log.debug("Persisting %d stream records (from slice of size %d) to %s".format(ingestRecords.size, chunk.length, path))

            for {
              terminal <- tail.isEmpty

              streamRef = StreamRef.Replace(streamId, terminal)

              apiKey <- RootAPIKey

              msg =
                IngestMessage(
                  apiKey,
                  path,
                  Authorities(AccountFinder.DefaultId),
                  ingestRecords,
                  None,
                  clock.instant(),
                  streamRef)

              // use the vfs inside of SecureVFS
              par <- actorVFS.writeAllSync(Seq((pseudoOffset, msg))).run
            } yield {
              par.fold(
                errors => {
                  log.error("Unable to complete persistence of result stream to %s: %s".format(path.path, errors.shows))
                  None
                },
                _ => Some((chunk, (pseudoOffset + 1, tail)))
              )
            }

          case None =>
            log.debug("Persist stream writing to %s complete.".format(path.path))
            Future.successful(None)
        }
    }

    stream.foldLeft(())((_, _) => ())
  }

  def shutdown: Future[Unit] = actorSystem.terminate.map(_ => ())
}
