/*
 * Copyright 2014–2016 SlamData Inc.
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
import quasar.api.{redirectService, staticFileService, ResponseOr}
import quasar.config._
import quasar.console.{logErrors, stderr}
import quasar.fp.TaskRef
import quasar.main._

import argonaut.DecodeJson
import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task

object Server {
  final case class QuasarConfig(
    staticContent: List[StaticContent],
    redirect: Option[String],
    port: Option[Int],
    configPath: Option[FsFile],
    openClient: Boolean)

  object QuasarConfig {
    def fromArgs(args: Array[String]): MainTask[QuasarConfig] = for {
      opts    <- CliOptions.parser.parse(args, CliOptions.default)
                  .cata(_.point[MainTask], MainTask.raiseError("couldn't parse options"))
      content <- StaticContent.fromCliOptions("/files", opts)
      redirect = content.map(_.loc)
      cfgPath <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
                    FsPath.parseSystemFile(cfg)
                      .toRight(s"Invalid path to config file: $cfg")
                      .map(some))
    } yield QuasarConfig(content.toList, redirect, opts.port, cfgPath, opts.openClient)
  }

  /** Attempts to load the specified config file or one found at any of the
    * default paths. If an error occurs, it is logged to STDERR and the
    * default configuration is returned.
    */
  def loadConfigFile[C: DecodeJson](configOps: ConfigOps[C], configFile: Option[FsFile]): Task[C] = {
    import ConfigError._
    configFile.fold(configOps.fromDefaultPaths)(configOps.fromFile).run flatMap {
      case \/-(c) => c.point[Task]

      case -\/(FileNotFound(f)) => for {
        codec <- FsPath.systemCodec
        fstr  =  FsPath.printFsPath(codec, f)
        _     <- stderr(s"Configuration file '$fstr' not found, using default configuration.")
      } yield configOps.default

      case -\/(MalformedConfig(_, rsn)) =>
        stderr(s"Error in configuration file, using default configuration: $rsn")
          .as(configOps.default)
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
  ): (Int => Task[Unit]) => HttpService = {
    import RestApi._

    (reload: Int => Task[Unit]) =>
      finalizeServices(eval)(
        coreServices[CoreEff],
        additionalServices
      ) orElse nonApiService(initialPort, reload, staticContent, redirect)
  }

  def durableService(
    qConfig: QuasarConfig,
    webConfig: WebConfig)(
    implicit
    ev1: ConfigOps[WebConfig]
  ): MainTask[(Int => Task[Unit]) => HttpService] =
    for {
      cfgRef       <- TaskRef(webConfig).liftM[MainErrT]
      mntCfgsT     =  MntCfgsIO.write(cfgRef, qConfig.configPath)
      coreApi      <- CoreEff.interpreter[WebConfig](mntCfgsT).liftM[MainErrT]
      ephemeralApi =  CfgsErrsIO.toMainTask(MntCfgsIO.ephemeral) compose coreApi
      _            <- (mountAll[CoreEff](webConfig.mountings) foldMap ephemeralApi).flatMapF(_.point[Task])
      durableApi   =  toResponseOr(MntCfgsIO.durableFile[WebConfig](mntCfgsT)) compose coreApi
    } yield service(
      webConfig.server.port,
      qConfig.staticContent,
      qConfig.redirect,
      durableApi)

  def main(args: Array[String]): Unit = {
    implicit val configOps: ConfigOps[WebConfig] = WebConfig

    val main0 = for {
      qCfg    <- QuasarConfig.fromArgs(args)
      wCfg    <- loadConfigFile(WebConfig, qCfg.configPath).liftM[MainErrT]
                 // TODO: Find better way to do this
      updWCfg =  wCfg.copy(server = wCfg.server.copy(qCfg.port.getOrElse(wCfg.server.port)))
      srvc    <- durableService(qCfg, updWCfg)
      _       <- Http4sUtils.startAndWait(updWCfg.server.port, srvc, qCfg.openClient).liftM[MainErrT]
    } yield ()

    logErrors(main0).unsafePerformSync
  }

}
