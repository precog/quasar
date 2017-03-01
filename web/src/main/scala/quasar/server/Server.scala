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

import quasar.Predef._
import quasar.api.services._
import quasar.api.{redirectService, staticFileService, ResponseOr, ResponseT}
import quasar.config._
import quasar.console.{logErrors, stderr}
import quasar.fp.{liftMT, TaskRef}
import quasar.fp.free._
import quasar.fs.Empty
import quasar.fs.mount._, FileSystemDef.DefinitionResult
import quasar.main._

import argonaut.DecodeJson
import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task

object Server {
  type ServiceStarter = (Int => Task[Unit]) => HttpService

  final case class QuasarConfig(
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
        QuasarConfig(content.toList, content.map(_.loc), opts.port, cfgPath, opts.openClient))
  }

  /** Attempts to load the specified config file or one found at any of the
    * default paths. If an error occurs, it is logged to STDERR and the
    * default configuration is returned.
    */
  def loadConfigFile[C: DecodeJson](configFile: Option[FsFile])(implicit cfgOps: ConfigOps[C]): Task[C] = {
    import ConfigError._
    configFile.fold(cfgOps.fromDefaultPaths)(cfgOps.fromFile).run flatMap {
      case \/-(c) => c.point[Task]

      case -\/(FileNotFound(f)) => for {
        codec <- FsPath.systemCodec
        fstr  =  FsPath.printFsPath(codec, f)
        _     <- stderr(s"Configuration file '$fstr' not found, using default configuration.")
      } yield cfgOps.default

      case -\/(MalformedConfig(_, rsn)) =>
        stderr(s"Error in configuration file, using default configuration: $rsn")
          .as(cfgOps.default)
    }
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
    webConfig: WebConfig)(
    implicit
    ev1: ConfigOps[WebConfig]
  ): MainTask[ServiceStarter] =
    for {
      cfgRef       <- TaskRef(webConfig).liftM[MainErrT]
      hfsRef       <- TaskRef(Empty.fileSystem[HierarchicalFsEffM]).liftM[MainErrT]
      mntdRef      <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]).liftM[MainErrT]

      ephemeralMnt =  KvsMounter.interpreter[Task, QErrsIO](
                        KvsMounter.ephemeralMountConfigs[Task],
                        hfsRef, mntdRef)
      initMnts     =  foldMapNT(QErrsIO.toMainTask) compose ephemeralMnt
      // TODO: Still need to expose these in the HTTP API, see SD-1131
      failedMnts   <- attemptMountAll[Mounting](webConfig.mountings) foldMap initMnts
      _            <- failedMnts.toList.traverse_(logFailedMount).liftM[MainErrT]

      durableMnt   =  KvsMounter.interpreter[Task, QErrsIO](
                        writeConfig(WebConfig.mountings, cfgRef, qConfig.configPath),
                        hfsRef, mntdRef)
      toQErrsIOM   =  injectFT[Task, QErrsIO] :+: durableMnt :+: injectFT[QErrs, QErrsIO]
      runCore      <- CoreEff.runFs[QEffIO](hfsRef).liftM[MainErrT]

      qErrsIORor   =  liftMT[Task, ResponseT] :+: qErrsToResponseOr
      coreApi      =  foldMapNT(qErrsIORor) compose foldMapNT(toQErrsIOM) compose runCore
    } yield service(
      webConfig.server.port,
      qConfig.staticContent,
      qConfig.redirect,
      coreApi)

  def launchServer(args: Array[String], builder: (QuasarConfig, WebConfig) => MainTask[ServiceStarter])(implicit configOps: ConfigOps[WebConfig]): Task[Unit] =
    logErrors(for {
      qCfg    <- QuasarConfig.fromArgs(args)
      wCfg    <- loadConfigFile[WebConfig](qCfg.configPath).liftM[MainErrT]
                 // TODO: Find better way to do this
      updWCfg =  wCfg.copy(server = wCfg.server.copy(qCfg.port.getOrElse(wCfg.server.port)))
      srvc    <- builder(qCfg, updWCfg)
      _       <- Http4sUtils.startAndWait(updWCfg.server.port, srvc, qCfg.openClient).liftM[MainErrT]
    } yield ())

  def main(args: Array[String]): Unit =
    launchServer(args, durableService(_, _)).unsafePerformSync
}
