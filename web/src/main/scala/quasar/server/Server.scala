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

package quasar.server

import slamdata.Predef._
import quasar.api.services._
import quasar.api.{redirectService, staticFileService, ResponseOr, ResponseT}
import quasar.cli.Cmd, Cmd._
import quasar.config._
import quasar.console.logErrors
import quasar.db.StatefulTransactor
import quasar.fp._, ski.κ
import quasar.fp.free._
import quasar.main._, config._, metastore._
import quasar.metastore.Schema

import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz.{Failure => _, _}
import Scalaz._
import scalaz.concurrent.Task

object Server {
  type ServiceStarter = (Int => Task[Unit]) => HttpService

  final case class QuasarConfig(
    cmd: Cmd,
    staticContent: List[StaticContent],
    redirect: Option[String],
    port: Option[Int],
    configPath: Option[FsFile],
    openClient: Boolean)

  object QuasarConfig {
    def fromArgs(args: Array[String]): MainTask[QuasarConfig] =
      CliOptions.parser.parse(args.toSeq, CliOptions.default)
        .cata(_.point[MainTask], MainTask.raiseError("couldn't parse options"))
        .flatMap(fromCliOptions)

    def fromCliOptions(opts: CliOptions): MainTask[QuasarConfig] =
      (StaticContent.fromCliOptions("/files", opts) ⊛
        opts.config.fold(none[FsFile].point[MainTask])(cfg =>
          FsPath.parseSystemFile(cfg)
            .toRight(s"Invalid path to config file: $cfg")
            .map(some))) ((content, cfgPath) =>
        QuasarConfig(
          opts.cmd, content.toList, content.map(_.loc), opts.port, cfgPath, opts.openClient))
  }

  def nonApiService(
    initialPort: Int,
    reload: Int => Task[Unit],
    staticContent: List[StaticContent],
    redirect: Option[String]
  ): HttpService = {
    val staticRoutes = staticContent map {
      case StaticContent(loc, path) => loc -> staticFileService(path)
    }

    Router(staticRoutes ::: List(
      "/"            -> redirectService(redirect getOrElse "/welcome"),
      "/server/info" -> info.service,
      "/server/port" -> control.service(initialPort, reload)
    ): _*)

  }

  def service(
    initialPort: Int,
    staticContent: List[StaticContent],
    redirect: Option[String],
    eval: CoreEff ~> ResponseOr
  ): ServiceStarter = {
    import RestApi._

    (reload: Int => Task[Unit]) =>
      finalizeServices(
        toHttpServices(liftMT[Task, ResponseT] :+: eval, coreServices[CoreEffIO]) ++
        additionalServices
      ) orElse nonApiService(initialPort, reload, staticContent, redirect)
  }

  def durableService(
    qConfig: QuasarConfig,
    port: Int,
    metastoreCtx: MetaStoreCtx
  ): (ServiceStarter, Task[Unit]) = {
    val f: QErrsTCnxIO ~> ResponseOr =
      liftMT[Task, ResponseT]                                                   :+:
      (liftMT[Task, ResponseT] compose metastoreCtx.metastore.transactor.trans) :+:
      qErrsToResponseOr

    val serv = service(
      port,
      qConfig.staticContent,
      qConfig.redirect,
      foldMapNT(f) compose metastoreCtx.interp)

    (serv, metastoreCtx.closeMnts *> metastoreCtx.metastore.shutdown)
  }

  def launchServer(
    args: Array[String],
    builder: (QuasarConfig, Int, MetaStoreCtx) => (ServiceStarter, Task[Unit])
  )(implicit
    configOps: ConfigOps[WebConfig]
  ): Task[Unit] = {
    def start(tx: StatefulTransactor, port: Int, qCfg: QuasarConfig) =
      for {
        _     <- verifySchema(Schema.schema, tx.transactor)
        msCtx <- metastoreCtx(tx)
        (srvc, sdown) = builder(qCfg, port, msCtx)
        _     <- Http4sUtils.startAndWait(port, srvc, qCfg.openClient).liftM[MainErrT]
        _     <- sdown.liftM[MainErrT]
      } yield ()

    logErrors(for {
      qCfg  <- QuasarConfig.fromArgs(args)
      wCfg  <- loadConfigFile[WebConfig](qCfg.configPath).liftM[MainErrT]
      msCfg <- wCfg.metastore.cata(Task.now, MetaStoreConfig.configOps.default).liftM[MainErrT]
      tx    <- metastoreTransactor(msCfg)
      _     <- EitherT((qCfg.cmd match {
                 case Start =>
                   start(tx, qCfg.port | wCfg.server.port, qCfg)
                 case InitUpdateMetaStore =>
                   initUpdateMigrate(Schema.schema, tx.transactor, qCfg.configPath)
               }).run.onFinish(κ(tx.shutdown)))
    } yield ())
  }

  def main(args: Array[String]): Unit =
    launchServer(args, durableService).unsafePerformSync
}
