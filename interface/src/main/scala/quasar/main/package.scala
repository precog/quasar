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

package quasar

import slamdata.Predef._
import quasar.cli.Cmd
import quasar.config.{ConfigOps, FsFile, ConfigError, MetaStoreConfig}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.connector.BackendModule
import quasar.effect._
import quasar.db._
import quasar.fp._, ski._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.cache.VCache, VCache.{VCacheExpR, VCacheExpW, VCacheKVS}
import quasar.fs.mount.hierarchical._
import quasar.fs.mount.module.Module
import quasar.physical._
import quasar.main.config.loadConfigFile
import quasar.metastore._

import java.io.File
import scala.util.control.NonFatal

import doobie.imports._
import eu.timepit.refined.auto._
import monocle.Lens
import org.slf4s.Logging
import pathy.Path, Path.posixCodec
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

/** Concrete effect types and their interpreters that implement the quasar
  * functionality.
  */
package object main extends Logging {
  import BackendDef.DefinitionResult
  import QueryFile.ResultHandle

  type MainErrT[F[_], A] = EitherT[F, String, A]
  type MainTask[A]       = MainErrT[Task, A]
  val MainTask           = MonadError[EitherT[Task, String, ?], String]

  final case class ClassName(value: String) extends AnyVal
  final case class ClassPath(value: IList[File]) extends AnyVal

  // all of the backends which are included in the core distribution
  private val CoreFS = IList(
    mimir.Mimir.definition translate injectFT[Task, PhysFsEff],
    skeleton.Skeleton.definition translate injectFT[Task, PhysFsEff]
  ).fold

  val QuasarAPI = QuasarAPIImpl[Free[CoreEff, ?]](liftFT)

  /**
   * The physical filesystems currently supported.  Please note that it
   * is really best if you only sequence this task ''once'' per runtime.
   * It won't misbehave, but it will waste resources if you run it multiple
   * times.  Thus, all uses of the value from this Task should be handled
   * by `Read[BackendDef[PhysFsEffM], ?]` (or an analogous `Kleisli`).
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  def physicalFileSystems(config: BackendConfig): Task[BackendDef[PhysFsEffM]] = {
    import java.lang.{
      ClassCastException,
      ClassNotFoundException,
      ExceptionInInitializerError,
      IllegalAccessException,
      IllegalArgumentException,
      NoSuchFieldException,
      NullPointerException
    }
    import java.net.URLClassLoader

    import BackendConfig._

    // this is a side-effect in the same way that `new` is a side-effect
    val ParentCL = this.getClass.getClassLoader

    def loadBackend(classname: String, classloader: ClassLoader): OptionT[Task, BackendDef[Task]] = {
      for {
        clazz <- OptionT(Task delay {
          try {
            Some(classloader.loadClass(classname))
          } catch {
            case cnf: ClassNotFoundException =>
              log.warn(s"could not locate class for backend module '$classname'", cnf)

              None
          }
        })

        module <- OptionT(Task delay {
          try {
            Some(clazz.getDeclaredField("MODULE$").get(null).asInstanceOf[BackendModule])
          } catch {
            case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
              log.warn(s"backend module '$classname' does not appear to be a singleton object", e)

              None

            case e: ExceptionInInitializerError =>
              log.warn(s"backend module '$classname' failed to load with exception", e)

              None

            case _: ClassCastException =>
              log.warn(s"backend module '$classname' is not actually a subtype of BackendModule")

              None
          }
        })
      } yield module.definition
    }

    val isolatedFS: Task[BackendDef[Task]] = config match {
      case PluginDirectory(directory) =>
        import java.util.jar.JarFile
        import java.io.File
        import java.nio.file.{Files, Paths, Path}
        import java.nio.charset.StandardCharsets
        import argonaut._, Argonaut._

        final case class Plugin(mainJar: Path, classPath: List[Path]) {
          private def absolute(relative: Path): Path = directory.toPath.toAbsolutePath.resolve(relative)
          def withAbsolutePaths = Plugin(absolute(mainJar), classPath.map(absolute))
        }

        object Plugin {
          implicit val pathDecodeJson:   DecodeJson[Path]  = optionDecoder(json => json.string.map(Paths.get(_)), "Path")
          implicit val pluginDecodeJson: DecodeJson[Plugin] = jdecode2L(Plugin.apply)("main_jar", "classpath")
        }

        def load(pluginFile: File): Task[Option[BackendDef[Task]]] = {
          val backend: OptionT[Task, BackendDef[Task]] = for {
            contents <- Task.delay(new String(Files.readAllBytes(pluginFile.toPath), StandardCharsets.UTF_8)).liftM[OptionT]
            plugin <- OptionT(Task.now(contents.decodeOption[Plugin].map(_.withAbsolutePaths)))
            classLoader <- Task.delay(new URLClassLoader(plugin.classPath.map(_.toUri.toURL).toArray, ParentCL)).liftM[OptionT]
            mainJar = new JarFile(plugin.mainJar.toFile)
            backendModuleAttr <- OptionT(Task.delay(Option(mainJar.getManifest.getMainAttributes.getValue("Backend-Module"))))
            backendModules = backendModuleAttr.split(" ").toList.toIList
            defs <- backendModules.traverse(loadBackend(_, classLoader))
          } yield defs.fold

          for {
            result <- backend.run
            _ <- if (result.isEmpty) {
              Task.delay(log.warn(s"unable to load any backends from $pluginFile; perhaps the file is invalid or the 'Backend-Module' attribute is not defined"))
            } else Task.now(())
          } yield result
        }

        val pluginFileExtension = ".plugin"

        for {
          children <- Task.delay(directory.listFiles().toList.toIList)
          pluginFiles = children.filter(_.getName.endsWith(pluginFileExtension))
          loaded <- pluginFiles.traverse(load)
        } yield loaded.flatMap(_.toIList).fold

      case ExplodedDirs(backends) =>
        val maybeDefinitionsM: Task[IList[Option[BackendDef[Task]]]] = backends traverse {
          case (ClassName(cn), ClassPath(paths)) =>
            val back = for {
              urls <- (paths traverse { file =>
                Task.delay(file.toURI.toURL)
              }).liftM[OptionT]

              cl <- Task.delay(new URLClassLoader(urls.toList.toArray, ParentCL)).liftM[OptionT]
              back <- loadBackend(cn, cl)
            } yield back

            back.run
        }

        maybeDefinitionsM.map(_.flatMap(opt => IList.fromList(opt.toList)).fold)
    }

    // merge the isolated filesystems with the core filesystems
    isolatedFS.map(_.translate(injectFT[Task, PhysFsEff])).map(IList(_, CoreFS).fold)
  }

  type FsAsk[A] = Read[BackendDef[PhysFsEffM], A]

  /** All possible types of failure in the system (apis + physical). */
  type QErrs[A]    = Coproduct[PhysErr, CoreErrs, A]

  object QErrs {
    def toCatchable[F[_]: Catchable]: QErrs ~> F =
      Failure.toRuntimeError[F, PhysicalError]             :+:
      Failure.toRuntimeError[F, Module.Error]              :+:
      Failure.toRuntimeError[F, Mounting.PathTypeMismatch] :+:
      Failure.toRuntimeError[F, MountingError]             :+:
      Failure.toRuntimeError[F, FileSystemError]

    def toMainErrT[F[_]: Catchable: Monad]: QErrs ~> MainErrT[F, ?] =
      liftMT[F, MainErrT].compose(QErrs.toCatchable[F])
  }

  /** Effect comprising the core Quasar apis.
    * NB: CoM is avoided due to significantly longer compile times at usage sites.
    */
  type CoreEffIO[A]   = Coproduct[Task, CoreEff, A]
  type CoreEffIOW[A]  = Coproduct[VCacheExpW, CoreEffIO, A]
  type CoreEffIORW[A] = Coproduct[VCacheExpR, CoreEffIOW, A]
  type CoreEff8[A] = Coproduct[Timing, CoreErrs, A]
  type CoreEff7[A] = Coproduct[VCacheKVS, CoreEff8, A]
  type CoreEff6[A] = Coproduct[ManageFile, CoreEff7, A]
  type CoreEff5[A] = Coproduct[WriteFile, CoreEff6, A]
  type CoreEff4[A] = Coproduct[ReadFile, CoreEff5, A]
  type CoreEff3[A] = Coproduct[QueryFile, CoreEff4, A]
  type CoreEff2[A] = Coproduct[Analyze, CoreEff3, A]
  type CoreEff1[A] = Coproduct[Mounting, CoreEff2, A]
  type CoreEff0[A] = Coproduct[Module, CoreEff1, A]
  type CoreEff[A]  = Coproduct[MetaStoreLocation, CoreEff0, A]

  object CoreEff {
    def defaultImpl(
     fsThing: FS,
     metaRef: TaskRef[MetaStore],
     persist: quasar.db.DbConnectionConfig => MainTask[Unit]
    ): Task[CoreEff ~> QErrs_CRW_TaskM] = {
      val vcacheInterp: VCacheKVS ~> QErrs_CRW_TaskM =
        foldMapNT(
          injectFT[VCacheExpW, QErrs_CRW_Task]           :+:
          (Inject[ManageFile, BackendEffect] andThen
           fsThing.core                      andThen
           mapSNT(injectNT[QErrs_Task, QErrs_CRW_Task])) :+:
          injectFT[FileSystemFailure, QErrs_CRW_Task]    :+:
          (connectionIOToTask(metaRef) andThen injectFT[Task, QErrs_CRW_Task])
        ) compose
          VCache.interp[(VCacheExpW :\: ManageFile :\: FileSystemFailure :/: ConnectionIO)#M]

      type Mounting_Backend[A] = Coproduct[Mounting, BackendEffect, A]
      for {
        fs <- runFsWithViewsAndModules(fsThing.core, vcacheInterp, fsThing.mounting)
      } yield {
        val module = Module.impl.default[Mounting_Backend] andThen
          foldMapNT((fsThing.mounting andThen mapSNT(injectNT[QErrs_Task, QErrs_CRW_Task])) :+: fs)
        (MetaStoreLocation.impl.default(metaRef, persist) andThen injectFT[Task, QErrs_CRW_Task]) :+:
        module                                                                                    :+:
        (fsThing.mounting andThen mapSNT(injectNT[QErrs_Task, QErrs_CRW_Task]))                   :+:
        (injectNT[Analyze, BackendEffect] andThen fs)                                             :+:
        (injectNT[QueryFile, BackendEffect] andThen fs)                                           :+:
        (injectNT[ReadFile, BackendEffect] andThen fs)                                            :+:
        (injectNT[WriteFile, BackendEffect] andThen fs)                                           :+:
        (injectNT[ManageFile, BackendEffect] andThen fs)                                          :+:
        vcacheInterp                                                                              :+:
        (Timing.toTask andThen injectFT[Task, QErrs_CRW_Task])                                    :+:
        injectFT[Module.Failure, QErrs_CRW_Task]                                                  :+:
        injectFT[PathMismatchFailure, QErrs_CRW_Task]                                             :+:
        injectFT[MountingFailure, QErrs_CRW_Task]                                                 :+:
        injectFT[FileSystemFailure, QErrs_CRW_Task]
      }
    }

    def runFsWithViewsAndModules(
      fs: BackendEffect ~> QErrs_TaskM,
      vc: VCacheKVS ~> QErrs_CRW_TaskM,
      mount: Mounting ~> QErrs_TaskM
    ): Task[BackendEffect ~> QErrs_CRW_TaskM] = {

      type V[A] = (VCacheKVS :\: Mounting :/: QErrs_Task)#M[A]

      overlayModulesViews[V, QErrs_Task](fs).map { toV =>
        val vToTask =
          vc                                                          :+:
          (mount andThen mapSNT(injectNT[QErrs_Task, QErrs_CRW_Task])) :+:
          injectFT[QErrs_Task, QErrs_CRW_Task]

        toV andThen foldMapNT(vToTask)
      }
    }
  }

  /** The types of failure from core apis. */
  type CoreErrs[A]   = Coproduct[Module.Failure, CoreErrs1, A]
  type CoreErrs1[A]  = Coproduct[PathMismatchFailure, CoreErrs0, A]
  type CoreErrs0[A]  = Coproduct[MountingFailure, FileSystemFailure, A]

  def connectionIOToTask(metaRef: TaskRef[MetaStore]): ConnectionIO ~> Task =
    λ[ConnectionIO ~> Task](io => metaRef.read.flatMap(t => t.trans.transactor.trans.apply(io)))

  //---- FileSystems ----

  /** The effects required by hierarchical FileSystem operations. */
  type HierarchicalFsEffM[A] = Free[HierarchicalFsEff, A]
  type HierarchicalFsEff[A]  = Coproduct[PhysFsEffM, HierarchicalFsEff0, A]
  type HierarchicalFsEff0[A] = Coproduct[MountedResultH, MonotonicSeq, A]

  object HierarchicalFsEff {
    def interpreter[S[_]](
      seqRef: TaskRef[Long],
      mntResRef: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): HierarchicalFsEff ~> Free[S, ?] = {
      val injTask = injectFT[Task, S]

      foldMapNT(liftFT compose PhysFsEff.inject[S])              :+:
      injTask.compose(KeyValueStore.impl.fromTaskRef(mntResRef)) :+:
      injTask.compose(MonotonicSeq.fromTaskRef(seqRef))
    }

    /** A dynamic `FileSystem` evaluator formed by internally fetching an
      * interpreter from a `TaskRef`, allowing for the behavior to change over
      * time as the ref is updated.
      */
    def dynamicFileSystem[S[_]](
      ref: TaskRef[BackendEffect ~> HierarchicalFsEffM],
      hfs: HierarchicalFsEff ~> Free[S, ?]
    )(implicit
      S: Task :<: S
    ): BackendEffect ~> Free[S, ?] =
      new (BackendEffect ~> Free[S, ?]) {
        def apply[A](fs: BackendEffect[A]) =
          lift(ref.read.map(free.foldMapNT(hfs) compose _))
            .into[S]
            .flatMap(_ apply fs)
      }
  }

  /** Effects that physical filesystems are permitted. */
  type PhysFsEffM[A] = Free[PhysFsEff, A]
  type PhysFsEff[A]  = Coproduct[Task, PhysErr, A]

  object PhysFsEff {
    def inject[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): PhysFsEff ~> S =
      S0 :+: S1

    /** Replace non-fatal failed `Task`s with a PhysicalError. */
    def reifyNonFatalErrors[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): Task ~> Free[S, ?] =
      λ[Task ~> Free[S, ?]](t => Free.roll(S0(t map (_.point[Free[S, ?]]) handle {
        case NonFatal(ex: Exception) => Failure.Ops[PhysicalError, S].fail(unhandledFSError(ex))
      })))
  }


  //--- Mounting ---

  /** Provides the mount handlers to update the hierarchical
    * filesystem whenever a mount is added or removed.
    */
  def mountHandler[S[_]](implicit S: FsAsk :<: S): Free[S, MountRequestHandler[PhysFsEffM, HierarchicalFsEff]] = {
    Read.Ops[BackendDef[PhysFsEffM], S].ask map { physicalFileSystems =>
      MountRequestHandler[PhysFsEffM, HierarchicalFsEff](
        physicalFileSystems translate flatMapSNT(
          PhysFsEff.reifyNonFatalErrors[PhysFsEff] :+: injectFT[PhysErr, PhysFsEff]))
    }
  }

  // copied out of MountRequestHandler to avoid dependent typing
  type HierarchicalFsRef[A] = AtomicRef[BackendEffect ~> Free[HierarchicalFsEff, ?], A]
  type MountedFsRef[A] = AtomicRef[Mounts[DefinitionResult[PhysFsEffM]], A]

  /** Effects required for mounting. */
  type MountEffM[A] = Free[MountEff, A]
  type MountEff[A]  = Coproduct[PhysFsEffM, MountEff0, A]
  type MountEff0[A] = Coproduct[HierarchicalFsRef, MountedFsRef, A]

  object MountEff {
    def interpreter[S[_]](
      hrchRef: TaskRef[BackendEffect ~> HierarchicalFsEffM],
      mntsRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): MountEff ~> Free[S, ?] = {
      val injTask = injectFT[Task, S]

      foldMapNT(liftFT compose PhysFsEff.inject[S])   :+:
      injTask.compose(AtomicRef.fromTaskRef(hrchRef)) :+:
      injTask.compose(AtomicRef.fromTaskRef(mntsRef))
    }
  }

  object KvsMounter {
    /** A `Mounting` interpreter that uses a `KeyValueStore` to store
      * `MountConfig`s.
      *
      * TODO: We'd like to not have to expose the `Mounts` `TaskRef`, but
      *       currently need to due to how we initialize the system using
      *       one mounting interpreter that updates these refs, but doesn't
      *       persist configs, and then switch to another that does persist
      *       configs post-initialization.
      *
      *       This should be unnecessary once we switch to lazy, on-demand
      *       mounting.
      *
      * @param cfgsImpl  a `KeyValueStore` interpreter to `Task`
      * @param hrchFsRef the current hierarchical FileSystem interpreter, updated whenever mounts change
      * @param mntdFsRef the current mapping of directories to filesystem definitions,
      *                  updated whenever mounts change
      */
    def interpreter[F[_], S[_]](
      cfgsImpl: MountConfigs ~> F,
      hrchFsRef: TaskRef[BackendEffect ~> HierarchicalFsEffM],
      mntdFsRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
    )(implicit
      S0: F :<: S,
      S1: Task :<: S,
      S2: PhysErr :<: S,
      S3: FsAsk :<: S
    ): Mounting ~> Free[S, ?] = {
      type G[A] = Coproduct[MountConfigs, MountEffM, A]

      val f: G ~> Free[S, ?] =
        injectFT[F, S].compose(cfgsImpl) :+:
        free.foldMapNT(MountEff.interpreter[S](hrchFsRef, mntdFsRef))

      val mounter: Free[S, Mounting ~> Free[G, ?]] =
        mountHandler map { handler =>
          quasar.fs.mount.Mounter.kvs[MountEffM, G](
            handler.mount[MountEff](_),
            handler.unmount[MountEff](_))
        }

      λ[Mounting ~> Free[S, ?]] { mounting =>
        mounter.flatMap(nt => (free.foldMapNT(f) compose nt)(mounting))
      }
    }

    def ephemeralMountConfigs[F[_]: Monad]: MountConfigs ~> F = {
      type S = Map[APath, MountConfig]
      evalNT[F, S](Map()) compose KeyValueStore.impl.toState[StateT[F, S, ?]](Lens.id[S])
    }
  }

  /** Mount all the mounts defined in the given configuration, returning
    * the paths that failed to mount along with the reasons why.
    */
  def attemptMountAll[S[_]](
    config: MountingsConfig
  )(implicit
    S: Mounting :<: S
  ): Free[S, Map[APath, String]] = {
    import Mounting.PathTypeMismatch
    import Failure.{mapError, toError}

    type T0[A] = Coproduct[MountingFailure, S, A]
    type T[A]  = Coproduct[PathMismatchFailure, T0, A]
    type Errs  = PathTypeMismatch \/ MountingError
    type M[A]  = EitherT[Free[S, ?], Errs, A]

    val mounting = Mounting.Ops[T]

    val runErrs: T ~> M =
      (toError[M, Errs] compose mapError[PathTypeMismatch, Errs](\/.left)) :+:
      (toError[M, Errs] compose mapError[MountingError, Errs](\/.right))   :+:
      (liftMT[Free[S, ?], EitherT[?[_], Errs, ?]] compose liftFT[S])

    val attemptMount: ((APath, MountConfig)) => Free[S, Map[APath, String]] = {
      case (path, cfg) =>
        mounting.mount(path, cfg).foldMap(runErrs).run map {
          case \/-(_)      => Map.empty
          case -\/(-\/(e)) => Map(path -> e.shows)
          case -\/(\/-(e)) => Map(path -> e.shows)
        }
    }

    config.toMap.toList foldMapM attemptMount
  }

  /** Prints a warning about the mount failure to the console. */
  val logFailedMount: ((APath, String)) => Task[Unit] = {
    case (path, err) => console.stderr(
      s"Warning: Failed to mount '${posixCodec.printPath(path)}' because '$err'."
    )
  }

  type QErrs_Task[A]  = Coproduct[Task, QErrs, A]
  type QErrs_TaskM[A] = Free[QErrs_Task, A]

  type QErrs_CRW_Task[A]  = (VCacheExpR :\: VCacheExpW :/: QErrs_Task)#M[A]
  type QErrs_CRW_TaskM[A] = Free[QErrs_CRW_Task, A]

  object QErrs_CRW_Task {
    def toMainTask: Task[QErrs_CRW_TaskM ~> MainTask] =
      TaskRef(Tags.Min(none[VCache.Expiration])) ∘ (r =>
        foldMapNT(
          (liftMT[Task, MainErrT] compose Read.fromTaskRef(r))  :+:
          (liftMT[Task, MainErrT] compose Write.fromTaskRef(r)) :+:
          liftMT[Task, MainErrT]                                :+:
          QErrs.toCatchable[MainTask]))

    val logFatalErrors: QErrs_CRW_Task ~> QErrs_CRW_TaskM =
      injectFT[VCacheExpR, QErrs_CRW_Task] :+:
      injectFT[VCacheExpW, QErrs_CRW_Task] :+:
      injectFT[Task, QErrs_CRW_Task] :+:
      logging.logPhysicalError[Task, QErrs_CRW_Task](log) :+:
      logging.logFatalModuleError[Task, QErrs_CRW_Task](log) :+:
      injectFT[PathMismatchFailure, QErrs_CRW_Task] :+:
      logging.logFatalMountingError[Task, QErrs_CRW_Task](log) :+:
      logging.logFatalFileSystemError[Task, QErrs_CRW_Task](log)
  }

  final case class CmdLineConfig(configPath: Option[FsFile], loadConfig: BackendConfig, cmd: Cmd)

  /** Either initialize the metastore or execute the start depending
    * on what command is provided by the user in the command line arguments
    */
  def initMetaStoreOrStart[C: argonaut.DecodeJson](
    config: CmdLineConfig,
    start: (C, CoreEff ~> QErrs_CRW_TaskM) => MainTask[Unit],
    persist: DbConnectionConfig => MainTask[Unit]
  )(implicit
    configOps: ConfigOps[C]
  ): MainTask[Unit] = {
    for {
      cfg  <- loadConfigFile[C](config.configPath).liftM[MainErrT]
      _     <- config.cmd match {
        case Cmd.Start =>
          for {
            quasarFs <- Quasar.initFromMetaConfig(
              config.loadConfig,
              configOps.metaStoreConfig.get(cfg),
              persist)

            _ <- start(cfg, quasarFs.interp).ensuring(κ(quasarFs.shutdown.liftM[MainErrT]))
          } yield ()

        case Cmd.InitUpdateMetaStore =>
          for {
            msCfg <- configOps.metaStoreConfig.get(cfg).cata(Task.now, MetaStoreConfig.default).liftM[MainErrT]
            trans =  simpleTransactor(DbConnectionConfig.connectionInfo(msCfg.database))
            _     <- initUpdateMigrate(quasar.metastore.Schema.schema, trans, config.configPath)
          } yield ()
      }
    } yield ()
  }

  /** Initialize or update MetaStore Schema and migrate mounts from config file
    */
  def initUpdateMigrate[A](
    schema: Schema[A], tx: Transactor[Task], cfgFile: Option[FsFile]
  ): MainTask[Unit] =
    for {
      j  <- EitherT(ConfigOps.jsonFromFile(cfgFile).fold(
              e => ConfigError.fileNotFound.getOption(e).cata(κ(none.right), e.shows.left),
              _.some.right))
      jʹ <- MetaStore.initializeOrUpdate(schema, tx, j).leftMap(_.message)
      _  <- EitherT.rightT(jʹ.traverse_(ConfigOps.jsonToFile(_, cfgFile)))
    } yield ()
}
