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
import quasar.fs.mount.cache.VCache
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
    // Couchbase.definition translate injectFT[Task, PhysFsEff],
    // marklogic.MarkLogic.definition translate injectFT[Task, PhysFsEff],
    mimir.Mimir.definition translate injectFT[Task, PhysFsEff],
    // mongodb.MongoDb.definition translate injectFT[Task, PhysFsEff],
    skeleton.Skeleton.definition translate injectFT[Task, PhysFsEff]// ,
    // sparkcore.fs.hdfs.SparkHdfsBackendModule.definition translate injectFT[Task, PhysFsEff],
    // sparkcore.fs.elastic.SparkElasticBackendModule.definition translate injectFT[Task, PhysFsEff],
    // sparkcore.fs.cassandra.SparkCassandraBackendModule.definition translate injectFT[Task, PhysFsEff],
    // sparkcore.fs.local.SparkLocalBackendModule.definition translate injectFT[Task, PhysFsEff]
  ).fold

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
      case JarDirectory(file) =>
        import java.util.jar.JarFile

        for {
          children <- Task.delay(file.listFiles().toList)
          jars = IList.fromList(children.filter(_.getName.endsWith(".jar")).toList)

          loaded <- jars traverse { jar =>
            val back = for {
              cl <- Task.delay(new URLClassLoader(Array(jar.toURI.toURL), ParentCL)).liftM[OptionT]
              manifest <- Task.delay(new JarFile(jar).getManifest()).liftM[OptionT]
              attrs <- Task.delay(manifest.getMainAttributes()).liftM[OptionT]

              moduleAttrValue <- OptionT(Task.delay(Option(attrs.getValue("Backend-Module"))))
              modules = IList.fromList(moduleAttrValue.split(" ").toList)

              defs <- modules.traverse(loadBackend(_, cl))
            } yield defs.fold

            for {
              result <- back.run

              _ <- if (result.isEmpty)
                Task.delay(log.warn(s"unable to load any backends from $jar; perhaps the 'Backend-Module' attribute is not defined"))
              else
                Task.now(())
            } yield result
          }
        } yield loaded.flatMap(r => IList.fromList(r.toList)).fold

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
  }

  /** Effect comprising the core Quasar apis. */
  type CoreEffIO[A] = Coproduct[Task, CoreEff, A]
  type CoreEff[A]   =
    (
      MetaStoreLocation :\: Module :\: Mounting :\: Analyze :\:
      QueryFile :\: ReadFile :\: WriteFile :\: ManageFile :\:
      VCache :\: Timing :/: CoreErrs
    )#M[A]

  object CoreEff {
    def runFs[S[_]](
      hfsRef: TaskRef[BackendEffect ~> HierarchicalFsEffM]
    )(
      implicit
      S0: Task :<: S,
      S1: Mounting :<: S,
      S2: PhysErr :<: S,
      S3: MountingFailure :<: S,
      S4: PathMismatchFailure :<: S,
      S5: FileSystemFailure :<: S,
      S6: Module.Failure    :<: S,
      S7: MetaStoreLocation :<: S,
      S8: Timing :<: S,
      S9: ConnectionIO :<: S
    ): Task[CoreEff ~> Free[S, ?]] = {
      def moduleInter(fs: BackendEffect ~> Free[S,?]): Module ~> Free[S, ?] = {
        val wtv: Coproduct[Mounting, BackendEffect, ?] ~> Free[S,?] = injectFT[Mounting, S] :+: fs
        flatMapSNT(wtv) compose Module.impl.default[Coproduct[Mounting, BackendEffect, ?]]
      }
      CompositeFileSystem.interpreter[S](hfsRef) >>= { compFs =>
        val vcacheInterp: VCache ~> Free[S, ?] =
          foldMapNT(
            (compFs compose Inject[ManageFile, BackendEffect]) :+:
            injectFT[FileSystemFailure, S]                     :+:
            injectFT[ConnectionIO, S]
          ) compose
            VCache.interp[(ManageFile :\: FileSystemFailure :/: ConnectionIO)#M]

        CompositeFileSystem.overlayModulesViews[Coproduct[VCache, S, ?], S](compFs) ∘ { compFs0 =>
          val compFs1: BackendEffect ~> Free[S, ?] = foldMapNT(vcacheInterp :+: liftFT[S]) compose compFs0

          injectFT[MetaStoreLocation, S]                      :+:
          moduleInter(compFs1)                                :+:
          injectFT[Mounting, S]                               :+:
          (compFs1 compose Inject[Analyze, BackendEffect])    :+:
          (compFs1 compose Inject[QueryFile, BackendEffect])  :+:
          (compFs1 compose Inject[ReadFile, BackendEffect])   :+:
          (compFs1 compose Inject[WriteFile, BackendEffect])  :+:
          (compFs1 compose Inject[ManageFile, BackendEffect]) :+:
          vcacheInterp                                        :+:
          injectFT[Timing, S]                                 :+:
          injectFT[Module.Failure, S]                         :+:
          injectFT[PathMismatchFailure, S]                    :+:
          injectFT[MountingFailure, S]                        :+:
          injectFT[FileSystemFailure, S]
        }
      }
    }
  }

  /** The types of failure from core apis. */
  type CoreErrs[A]   = Coproduct[Module.Failure, CoreErrs1, A]
  type CoreErrs1[A]  = Coproduct[PathMismatchFailure, CoreErrs0, A]
  type CoreErrs0[A]  = Coproduct[MountingFailure, FileSystemFailure, A]


  //---- FileSystems ----

  /** A FileSystem supporting views and physical filesystems mounted at various
    * points in the hierarchy.
    */
  object CompositeFileSystem {
    /** Interprets FileSystem given a TaskRef containing a hierarchical
      * FileSystem interpreter.
      *
      * TODO: The TaskRef is used as a communications channel so that
      *       the part of the system that deals with mounting can make
      *       new interpreters available to the part of the system that
      *       needs to interpret FileSystem operations.
      *
      *       This would probably be better served with a
      *       `Process[Task, FileSystem ~> HierarchicalFsEffM]` to allow
      *       for more flexible production of interpreters.
      */
    def interpreter[S[_]](
      hfsRef: TaskRef[BackendEffect ~> HierarchicalFsEffM]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S,
      S2: Mounting :<: S,
      S4: MountingFailure :<: S,
      S5: PathMismatchFailure :<: S
    ): Task[BackendEffect ~> Free[S, ?]] =
      for {
        startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
        seqRef     <- TaskRef(startSeq)
        mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
      } yield
        HierarchicalFsEff.dynamicFileSystem(
          hfsRef,
          HierarchicalFsEff.interpreter[S](seqRef, mntedRHRef))

    def overlayModulesViews[S[_], T[_]](
      f: BackendEffect ~> Free[T, ?]
    )(implicit
      S0: T :<: S,
      S1: Task :<: S,
      S2: VCache :<: S,
      S3: Mounting :<: S,
      S4: MountingFailure :<: S,
      S5: PathMismatchFailure :<: S
    ): Task[BackendEffect ~> Free[S, ?]] = {
      type V[A] = (
        VCache
          :\: ViewState
          :\: MonotonicSeq
          :\: Mounting
          :\: MountingFailure
          :\: PathMismatchFailure
          :/: BackendEffect
      )#M[A]

      for {
        startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
        seqRef     <- TaskRef(startSeq)
        viewHRef   <- TaskRef[ViewState.ViewHandles](Map())
      } yield {
        val compFs: V ~> Free[S, ?] =
          injectFT[VCache, S]                                                 :+:
          injectFT[Task, S].compose(KeyValueStore.impl.fromTaskRef(viewHRef)) :+:
          injectFT[Task, S].compose(MonotonicSeq.fromTaskRef(seqRef))         :+:
          injectFT[Mounting, S]                                               :+:
          injectFT[MountingFailure, S]                                        :+:
          injectFT[PathMismatchFailure, S]                                    :+:
          (foldMapNT(injectFT[T, S]) compose f)

        flatMapSNT(compFs) compose
          flatMapSNT(transformIn[BackendEffect, V, Free[V, ?]](module.backendEffect[V], liftFT)) compose
            view.backendEffect[V]
      }
    }
  }

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

  type QErrs_Task[A] = Coproduct[Task, QErrs, A]
  type QErrs_TaskM[A] = Free[QErrs_Task, A]

  object QErrs_Task {
    def toMainTask: QErrs_TaskM ~> MainTask = {
      foldMapNT(liftMT[Task, MainErrT] :+: QErrs.toCatchable[MainTask])
    }
  }

  final case class CmdLineConfig(configPath: Option[FsFile], loadConfig: BackendConfig, cmd: Cmd)

  /** Either initialize the metastore or execute the start depending
    * on what command is provided by the user in the command line arguments
    */
  def initMetaStoreOrStart[C: argonaut.DecodeJson](
    config: CmdLineConfig,
    start: (C, CoreEff ~> QErrs_TaskM) => MainTask[Unit],
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
      _  <- EitherT.right(jʹ.traverse_(ConfigOps.jsonToFile(_, cfgFile)))
    } yield ()
}
