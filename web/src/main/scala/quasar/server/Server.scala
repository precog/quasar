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
import quasar.cli.Cmd
import quasar.config._
import quasar.console.{logErrors, stdout}
import quasar.contrib.pathy.ADir
import quasar.contrib.scalaz._
import quasar.contrib.scopt._
import quasar.db.DbConnectionConfig
import quasar.fp._
import quasar.fp.free._
import quasar.main._
import quasar.server.Http4sUtils.{openBrowser, waitForUserEnter}

import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz.{Failure => _, _}
import Scalaz._
import scalaz.concurrent.Task

object Server {

  final case class WebCmdLineConfig(
      cmd: Cmd,
      staticContent: List[StaticContent],
      redirect: Option[String],
      port: Option[Int],
      configPath: Option[FsFile],
      loadConfig: BackendConfig,
      openClient: Boolean) {

    def toCmdLineConfig: CmdLineConfig = CmdLineConfig(configPath, loadConfig, cmd)
  }

  object WebCmdLineConfig {
    def fromArgs(args: Vector[String]): MainTask[WebCmdLineConfig] =
      CliOptions.parser.safeParse(args, CliOptions.default)
        .flatMap(fromCliOptions(_))

    def fromCliOptions(opts: CliOptions): MainTask[WebCmdLineConfig] = {
      import java.lang.RuntimeException

      val loadConfigM: Task[BackendConfig] = opts.loadConfig.fold(
        { plugins =>
          val err = Task.fail(new RuntimeException("plugin directory does not exist (or is a file)"))
          ADir.fromFile(plugins).getOrElseF(err).map(BackendConfig.JarDirectory(_))
        },
        backends => BackendConfig.fromBackends(IList.fromList(backends)))

      (StaticContent.fromCliOptions("/files", opts) ⊛
        opts.config.fold(none[FsFile].point[MainTask])(cfg =>
          FsPath.parseSystemFile(cfg)
            .toRight(s"Invalid path to config file: $cfg")
            .map(some)) ⊛
        loadConfigM.liftM[MainErrT]) ((content, cfgPath, loadConfig) =>
        WebCmdLineConfig(
          opts.cmd, content.toList, content.map(_.loc), opts.port, cfgPath, loadConfig, opts.openClient))
    }
  }

  def nonApiService(
    defaultPort: Int,
    reload: Int => Task[String \/ Unit],
    staticContent: List[StaticContent],
    redirect: Option[String]
  ): HttpService = {
    val staticRoutes = staticContent map {
      case StaticContent(loc, path) => loc -> staticFileService(path)
    }

    Router(staticRoutes ::: List(
      "/"            -> redirectService(redirect getOrElse "/welcome"),
      "/server/info" -> info.service,
      "/server/port" -> control.service(defaultPort, reload)
    ): _*)

  }

  def webInter[S[_]](eval: S ~> QErrs_TaskM): S ~> ResponseOr =
    foldMapNT(liftMT[Task, ResponseT] :+: qErrsToResponseT[Task]) compose eval

  def serviceStarter(
    defaultPort: Int,
    staticContent: List[StaticContent],
    redirect: Option[String],
    eval: CoreEff ~> QErrs_TaskM,
    persistPortChange: Int => MainTask[Unit]
  ): PortChangingServer.ServiceStarter = {
    import RestApi._

    (reload: Int => Task[String \/ Unit]) =>
      finalizeServices(
        toHttpServices(liftMT[Task, ResponseT] :+: webInter(eval), coreServices[CoreEffIO]) ++
        additionalServices
      ) orElse nonApiService(defaultPort, Kleisli(persistPortChange andThen (a => a.run)) >> Kleisli(reload), staticContent, redirect)
  }

  /**
    * Start Quasar server
    * @return A `Task` that can be used to shutdown the server
    */
  def startServer(
    quasarInter: CoreEff ~> QErrs_TaskM,
    port: Int,
    staticContent: List[StaticContent],
    redirect: Option[String],
    persistPortChange: Int => MainTask[Unit]
  ): Task[Task[Unit]] = {
    val starter = serviceStarter(defaultPort = port, staticContent, redirect, quasarInter, persistPortChange)
    PortChangingServer.start(initialPort = port, starter)
  }

  def persistMetaStore(configPath: Option[FsFile]): DbConnectionConfig => MainTask[Unit] =
    persistWebConfig(configPath, conn => _.copy(metastore = MetaStoreConfig(conn).some))

  def persistPortChange(configPath: Option[FsFile]): Int => MainTask[Unit] =
    persistWebConfig(configPath, port => _.copy(server = ServerConfig(port)))

  def persistWebConfig[A](configPath: Option[FsFile], modify: A => (WebConfig => WebConfig)): A => MainTask[Unit] = { a =>
    for {
      diskConfig <- EitherT(ConfigOps[WebConfig].fromOptionalFile(configPath).run.flatMap {
                      // Great, we were able to load the config file from disk
                      case \/-(config) => config.right.point[Task]
                      // The config file is malformed, let's warn the user and not make any changes
                      case -\/(configErr@ConfigError.MalformedConfig(_,_)) =>
                        (configErr: ConfigError).shows.left.point[Task]
                      // There is no config file, so let's create a new one with the default configuration
                      // to which we will apply this change
                      case -\/(ConfigError.FileNotFound(_)) => WebConfig.configOps.default.map(_.right)
                    })
      newConfig  =  modify(a)(diskConfig)
      _          <- EitherT(ConfigOps[WebConfig].toFile(newConfig, configPath).attempt).leftMap(_.getMessage)
    } yield ()
  }

  def safeMain(args: Vector[String]): Task[Unit] = {
    logErrors(for {
      webCmdLineCfg <- WebCmdLineConfig.fromArgs(args)
      _ <- initMetaStoreOrStart[WebConfig](
             webCmdLineCfg.toCmdLineConfig,
             (wCfg, quasarInter) => {
               val port = webCmdLineCfg.port | wCfg.server.port
               val persistPort = persistPortChange(webCmdLineCfg.configPath)
               (for {
                 shutdown <- startServer(quasarInter, port, webCmdLineCfg.staticContent, webCmdLineCfg.redirect, persistPort)
                 _        <- openBrowser(port).whenM(webCmdLineCfg.openClient)
                 _        <- stdout("Press Enter to stop.")
                 // If user pressed enter (after this main thread has been blocked on it),
                 // then we shutdown, otherwise we just run indefinitely until the JVM is killed
                 // If we don't call shutdown and this main thread completes, the application will
                 // continue to run indefinitely as `startServer` uses a non-daemon `ExecutorService`
                 // TODO: Figure out why it's necessary to use a `Task` that never completes to keep the main thread
                 // from completing instead of simply relying on the fact that the server is using a pool of
                 // non-daemon threads to ensure the application doesn't shutdown
                 _        <- waitForUserEnter.ifM(shutdown, Task.async[Unit](_ => ()))
               } yield ()).liftM[MainErrT]
             },
             persistMetaStore(webCmdLineCfg.configPath))
    } yield ())
  }

  def main(args: Array[String]): Unit =
    safeMain(args.toVector).unsafePerformSync
}
