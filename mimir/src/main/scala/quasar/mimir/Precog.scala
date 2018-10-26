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

package quasar.mimir

import quasar.Disposable
import quasar.contrib.cats.effect._
import quasar.niflheim.{Chef, V1CookedBlockFormat, V1SegmentFormat, VersionedCookedBlockFormat, VersionedSegmentFormat}
import quasar.yggdrasil.table.VFSColumnarTableModule
import quasar.yggdrasil.vfs.{POSIXWithIO, SerialVFS}

import java.io.File
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection.immutable.IndexedSeq

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.{ActorRefRoutee, CustomRouterConfig, RoundRobinRoutingLogic, Routee, Router, RouterConfig}
import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.slf4s.Logging
import scalaz.syntax.apply._
import shims._

final class Precog private (
    dataDir0: File,
    val actorSystem: ActorSystem,
    val vfs: SerialVFS[IO])(
    implicit val ExecutionContext: ExecutionContext)
    extends VFSColumnarTableModule
    with StdLibModule {

  object Library extends StdLib

  val HowManyChefsInTheKitchen: Int = 4
  val CookThreshold: Int = 20000
  val StorageTimeout: FiniteDuration = 300.seconds

  private val props: Props = Props(Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))

  private def chefs(system: ActorSystem): IndexedSeq[Routee] =
    (1 to HowManyChefsInTheKitchen).map { _ =>
      ActorRefRoutee(system.actorOf(props))
    }

  private val routerConfig: RouterConfig = new CustomRouterConfig {
    def createRouter(system: ActorSystem): Router =
      Router(RoundRobinRoutingLogic(), chefs(system))
  }

  // needed for nihdb
  val masterChef: ActorRef =
    actorSystem.actorOf(props.withRouter(routerConfig))

  // Members declared in quasar.yggdrasil.TableModule
  sealed trait TableCompanion extends VFSColumnarTableCompanion
  object Table extends TableCompanion
}

object Precog extends Logging {

  def apply(
    dataDir: File,
    blockingPool: ExecutionContext)(
    implicit
    cs: ContextShift[IO],
    csPwIO: ContextShift[POSIXWithIO],
    ec: ExecutionContext)
      : IO[Disposable[IO, Precog]] = {

    for {
      vfsd <- SerialVFS[IO](dataDir, blockingPool)

      sysd <- IO {
        val sys = ActorSystem(
          "nihdbExecutorActorSystem",
          classLoader = Some(getClass.getClassLoader))

        Disposable(sys, IO.fromFutureShift(IO(sys.terminate.map(_ => ()))))
      }

      pcd <- IO((vfsd |@| sysd) {
        case (vfs, sys) => new Precog(dataDir, sys, vfs)
      })
    } yield pcd
  }

  def stream(
    dataDir: File,
    blockingPool: ExecutionContext)(
    implicit
    cs: ContextShift[IO],
    csPwIO: ContextShift[POSIXWithIO],
    ec: ExecutionContext)
      : Stream[IO, Precog] =
    Stream.bracket(apply(dataDir, blockingPool))(_.dispose).flatMap(d => Stream.emit(d.unsafeValue))
}
