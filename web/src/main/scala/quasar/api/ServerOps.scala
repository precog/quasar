/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.api

import quasar.Predef._
import quasar.{Mounter => _, _}
import quasar.api.services.RestApi
import quasar.console._
import quasar.config._
import quasar.effect._
import quasar.fp._
import quasar.fs._
import quasar.fs.mount._
import Http4sUtils._
import quasar.physical.mongodb._
import quasar.physical.mongodb.fs.mongoDbFileSystemDef

import java.io.File
import java.lang.System

import argonaut.{CodecJson, EncodeJson}
import monocle.Lens
import org.http4s
import org.http4s.server.{Server => Http4sServer, HttpService}
import org.http4s.server.blaze.BlazeBuilder
import scalaz.{Failure => _, Lens => _, _}
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.concurrent.Task

object ServerOps {
  type Builders = List[(Int, BlazeBuilder)]
  type Servers = List[(Int, Http4sServer)]
  type ServersErrors = List[(Int, Throwable \/ Http4sServer)]
  type Services = List[(String, HttpService)]

  final case class Options(
    config: Option[String],
    contentLoc: Option[String],
    contentPath: Option[String],
    contentPathRelative: Boolean,
    openClient: Boolean,
    port: Option[Int])

  final case class StaticContent(loc: String, path: String)
}

object Server {

  import ServerOps._
  import QueryFile.ResultHandle
  import FileSystemDef.DefinitionResult
  import hierarchical._
  import Mounting.PathTypeMismatch

  type MainErrT[F[_], A] = EitherT[F, String, A]
  type MainTask[A]       = MainErrT[Task, A]

  val mainTask = MonadError[EitherT[Task,?,?], String]

  // NB: This is a terrible thing.
  //     Is there a better way to find the path to a jar?
  val jarPath: Task[String] =
    Task.delay {
      val uri = getClass.getProtectionDomain.getCodeSource.getLocation.toURI
      val path0 = uri.getPath
      val path =
        java.net.URLDecoder.decode(
          Option(uri.getPath)
            .getOrElse(uri.toURL.openConnection.asInstanceOf[java.net.JarURLConnection].getJarFileURL.getPath),
          "UTF-8")
      (new java.io.File(path)).getParentFile().getPath() + "/"
    }

  // scopt's recommended OptionParser construction involves side effects
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  val optionParser = new scopt.OptionParser[Options]("quasar") {
    head("quasar")
    opt[String]('c', "config") action { (x, c) => c.copy(config = Some(x)) } text("path to the config file to use")
    opt[String]('L', "content-location") action { (x, c) => c.copy(contentLoc = Some(x)) } text("location where static content is hosted")
    opt[String]('C', "content-path") action { (x, c) => c.copy(contentPath = Some(x)) } text("path where static content lives")
    opt[Unit]('r', "content-path-relative") action { (_, c) => c.copy(contentPathRelative = true) } text("specifies that the content-path is relative to the install directory (not the current dir)")
    opt[Unit]('o', "open-client") action { (_, c) => c.copy(openClient = true) } text("opens a browser window to the client on startup")
    opt[Int]('p', "port") action { (x, c) => c.copy(port = Some(x)) } text("the port to run Quasar on")
    help("help") text("prints this usage text")
  }

  def interpretPaths(options: Options): MainTask[Option[StaticContent]] = {
    val defaultLoc = "/files"

    def path(p: String): Task[String] =
      if (options.contentPathRelative) jarPath.map(_ + p)
      else p.point[Task]

    (options.contentLoc, options.contentPath) match {
      case (None, None) =>
        none.point[MainTask]
      case (Some(_), None) =>
        mainTask.raiseError("content-location specified but not content-path")
      case (loc, Some(p)) =>
        path(p).map(p => some(StaticContent(loc.getOrElse(defaultLoc), p))).liftM[MainErrT]
    }
  }

  // Task \/ MonotonicSeqF \/ ViewStateF \/ MountedResultHF \/ HFSFailureF
  type FsEff0[A] = Coproduct[MountedResultHF, HFSFailureF, A]
  type FsEff1[A] = Coproduct[ViewStateF, FsEff0, A]
  type FsEff2[A] = Coproduct[MonotonicSeqF, FsEff1, A]
  type FsEff[A]  = Coproduct[Task, FsEff2, A]
  type FsEffM[A] = Free[FsEff, A]

  // NB: Will eventually be the lcd of all physical filesystems, or we limit
  //     to a fixed set of effects that filesystems must interpret into.
  val mongoDbFs: FileSystemDef[Task] = {
    type MongoEff[A] = Coproduct[Task, WorkflowExecErrF, A]

    mongoDbFileSystemDef[MongoEff].translate(free.foldMapNT[MongoEff, Task](
      free.interpret2[Task, WorkflowExecErrF, Task](
        NaturalTransformation.refl,
        Coyoneda.liftTF[WorkflowExecErr, Task](Failure.toTaskFailure[WorkflowExecutionError]))))
  }

  // TODO: Interpret HFSFailureF into `ResponseOr`
  def fsEffToTask(
    seqRef: TaskRef[Long],
    viewHandlesRef: TaskRef[ViewHandles],
    mntResRef: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
  ): FsEff ~> Task =
    free.interpret5[Task, MonotonicSeqF, ViewStateF, MountedResultHF, HFSFailureF, Task](
      NaturalTransformation.refl[Task],
      Coyoneda.liftTF[MonotonicSeq, Task](MonotonicSeq.fromTaskRef(seqRef)),
      Coyoneda.liftTF[ViewState, Task](KeyValueStore.fromTaskRef(viewHandlesRef)),
      Coyoneda.liftTF[MountedResultH, Task](KeyValueStore.fromTaskRef(mntResRef)),
      Coyoneda.liftTF[HFSFailure, Task](Failure.toTaskFailure[HierarchicalFileSystemError]))

  val evalMounter = EvaluatorMounter[Task, FsEff](mongoDbFs)
  import evalMounter.{EvalFSRef, EvalFSRefF}

  type MountedFs[A]  = AtomicRef[Mounts[DefinitionResult[Task]], A]
  type MountedFsF[A] = Coyoneda[MountedFs, A]

  // EvalFSRefF \/ Task \/ MountedFsF \/ MountedViewsF
  type EvalEff0[A] = Coproduct[MountedFsF, MountedViewsF, A]
  type EvalEff1[A] = Coproduct[Task, EvalEff0, A]
  type EvalEff[A]  = Coproduct[EvalFSRefF, EvalEff1, A]
  type EvalEffM[A] = Free[EvalEff, A]

  def evalEffToTask(
    evalRef: TaskRef[FileSystem ~> FsEffM],
    mntsRef: TaskRef[Mounts[DefinitionResult[Task]]],
    viewsRef: TaskRef[Views]
  ): EvalEff ~> Task =
    free.interpret4[EvalFSRefF, Task, MountedFsF, MountedViewsF, Task](
      Coyoneda.liftTF[EvalFSRef, Task](AtomicRef.fromTaskRef(evalRef)),
      NaturalTransformation.refl[Task],
      Coyoneda.liftTF[MountedFs, Task](AtomicRef.fromTaskRef(mntsRef)),
      Coyoneda.liftTF[MountedViews, Task](AtomicRef.fromTaskRef(viewsRef)))

  type MntEff[A]  = Coproduct[EvalEffM, MountConfigsF, A]
  type MntEffM[A] = Free[MntEff, A]

  // TODO: For some reason, scalac complains of a diverging implicits when
  //       attempting to resolve this.
  implicit val mntEffFunctor =
    Coproduct.coproductFunctor[EvalEffM, MountConfigsF](implicitly, implicitly)

  val mounter: Mounting ~> MntEffM =
    Mounter[EvalEffM, MntEff](
      evalMounter.mount[EvalEff](_),
      evalMounter.unmount[EvalEff](_))

  def evalFromRef(ref: TaskRef[FileSystem ~> FsEffM], f: FsEff ~> Task): FileSystem ~> Task =
    new (FileSystem ~> Task) {
      def apply[A](fs: FileSystem[A]) =
        ref.read.map(free.foldMapNT(f) compose _).flatMap(_.apply(fs))
    }

  final case class QuasarConfig(staticContent: List[StaticContent],
                                redirect: Option[String],
                                port: Option[Int],
                                configPath: Option[FsFile],
                                openClient: Boolean)

  type ApiEff0[A] = Coproduct[MountingF, FileSystem, A]
  type ApiEff1[A] = Coproduct[FileSystemFailureF, ApiEff0, A]
  type ApiEff[A]  = Coproduct[Task, ApiEff1, A]

  type ApiEffM[A] = Free[ApiEff, A]

  type TaskAndConfigs[A] = Coproduct[Task, MountConfigsF, A]
  type TaskAndConfigsM[A] = Free[TaskAndConfigs, A]

  // TODO: Include failure effects in output coproduct
  val evalApi: Task[ApiEff ~> Free[TaskAndConfigs, ?]] =
    for {
      startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
      seqRef     <- TaskRef(startSeq)
      viewHRef   <- TaskRef[ViewHandles](Map())
      mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
      evalFsRef  <- TaskRef(Empty.fileSystem[FsEffM])
      mntsRef    <- TaskRef(Mounts.empty[DefinitionResult[Task]])
      viewsRef   <- TaskRef(Views.empty)
    } yield {
      val f: FsEff ~> Task = fsEffToTask(seqRef, viewHRef, mntedRHRef)
      val g: EvalEff ~> Task = evalEffToTask(evalFsRef, mntsRef, viewsRef)

      val liftTask: Task ~> TaskAndConfigsM =
        injectFT[Task, TaskAndConfigs]

      val mnt: MntEff ~> TaskAndConfigsM =
        free.interpret2[EvalEffM, MountConfigsF, TaskAndConfigsM](
          liftTask.compose[EvalEffM](free.foldMapNT(g)),
          liftFT[TaskAndConfigs] compose injectNT[MountConfigsF, TaskAndConfigs])

      val mounting: MountingF ~> TaskAndConfigsM =
        Coyoneda.liftTF[Mounting, TaskAndConfigsM](free.foldMapNT(mnt) compose mounter)

      val throwFailure: FileSystemFailureF ~> Task =
        Coyoneda.liftTF[FileSystemFailure, Task](
          Failure.toTaskFailure[FileSystemError])

      free.interpret4[Task, FileSystemFailureF, MountingF, FileSystem, TaskAndConfigsM](
        liftTask,
        injectFT[Task, TaskAndConfigs] compose throwFailure,
        mounting,
        liftTask compose evalFromRef(evalFsRef, f))
    }

  def configsAsState: TaskAndConfigsM ~> Task = {
    type ST[A] = StateT[Task, Map[APath, MountConfig2], A]

    val toState: MountConfigs ~> ST =
      KeyValueStore.toState[StateT[Task, ?, ?]](Lens.id[Map[APath, MountConfig2]])

    val interpret: TaskAndConfigsM ~> ST =
      free.foldMapNT[TaskAndConfigs, ST](
        free.interpret2[Task, MountConfigsF, ST](
          liftMT[Task, StateT[?[_], Map[APath, MountConfig2], ?]],
          Coyoneda.liftTF(toState)))

    evalNT[Task, Map[APath, MountConfig2]](Map()) compose interpret
  }

  def configsWithPersistence[C:EncodeJson](ref: TaskRef[C], loc: Option[FsFile])(implicit configOps: ConfigOps[C])
      : TaskAndConfigsM ~> Task =
    free.foldMapNT[TaskAndConfigs, Task](
      free.interpret2[Task, MountConfigsF, Task](
        NaturalTransformation.refl,
        Coyoneda.liftTF[MountConfigs, Task](writeConfig(configOps)(ref, loc))))

  def mountAll(mc: MountingsConfig2): Free[ApiEff, String \/ Unit] = {
    val mnt = Mounting.Ops[ApiEff]
    type N[A] = EitherT[mnt.F, String, A]

    def toN(v: mnt.M[PathTypeMismatch \/ Unit]): N[Unit] =
      EitherT[mnt.F, String, Unit](
        v.fold(_.shows.left, _.fold(_.shows.left, _.right)))

    mc.toMap.toList.traverse_ { case (p, cfg) => toN(mnt.mount(p, cfg)) }.run
  }

  def configuration(args: Array[String]): MainTask[QuasarConfig] = for {
    opts <- optionParser.parse(args, Options(None, None, None, false, false, None))
              .cata(_.point[MainTask], mainTask.raiseError("couldn't parse options"))
    content <- interpretPaths(opts)
    redirect = content.map(_.loc)
    cfgPath <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
                  FsPath.parseSystemFile(cfg)
                    .toRight(s"Invalid path to config file: $cfg")
                    .map(some))
  } yield QuasarConfig(content.toList, redirect, opts.port, cfgPath, opts.openClient)

  def startWebServer(initialPort: Int,
                     staticContent: List[StaticContent],
                     redirect: Option[String],
                     openClient: Boolean,
                     eval: ApiEff ~> ResponseOr) = {
    val produceRoutes = (reload: (Int => Task[Unit])) =>
                      fullServer(RestApi(initialPort, reload).httpServices(eval), staticContent, redirect)
    startAndWait(initialPort, produceRoutes, openClient)
  }

  def fullServer(services: Map[String, HttpService],
                 staticContent: List[StaticContent],
                 redirect: Option[String]): Map[String, HttpService] = {
    services ++
    staticContent.map { case StaticContent(loc, path) => loc -> staticFileService(path) }.toListMap ++
    ListMap("/" -> redirectService(redirect.getOrElse("/welcome")))
  }

  def main(args: Array[String]): Unit = {
    implicit val configOps: ConfigOps[WebConfig] = WebConfig
    val exec: MainTask[Unit] = for {
      qConfig     <- configuration(args)
      config      <- WebConfig.get(qConfig.configPath).liftM[MainErrT]
                    // TODO: Find better way to do this
      updConfig   = config.copy(server = config.server.copy(qConfig.port.getOrElse(config.server.port)))
      api         <- evalApi.liftM[MainErrT]
      cfgRef      <- TaskRef(config).liftM[MainErrT]
      apiWithPersistence = configsWithPersistence(cfgRef, qConfig.configPath) compose api
      _           <- EitherT(mountAll(config.mountings) foldMap (configsAsState compose api))
      _           <- startWebServer(updConfig.server.port, qConfig.staticContent, qConfig.redirect, qConfig.openClient, liftMT[Task, ResponseT] compose apiWithPersistence).liftM[MainErrT]
    } yield ()

    logErrors(exec).run
  }
}
