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

package quasar.fs

import slamdata.Predef._
import quasar.{BackendCapability, BackendName, BackendRef, Data, TestConfig}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.fp._
import quasar.fp.free._
import quasar.fs.mount._, BackendDef.DefinitionResult
import quasar.effect._
import quasar.main.{KvsMounter, HierarchicalFsEffM, PhysFsEff, PhysFsEffM}
import quasar.mimir
import quasar.physical._
import quasar.physical.couchbase.Couchbase
import quasar.physical.marklogic.MarkLogic
import quasar.regression.{interpretHfsIO, HfsIO}

import scala.Either

import eu.timepit.refined.auto._
import monocle.Optional
import monocle.function.Index
import org.specs2.specification.core.{Fragment, Fragments}
import org.specs2.execute.{Failure => _, _}
import pathy.Path._
import scalaz.{EphemeralStream => EStream, Optional => _, Failure => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

/** Executes all the examples defined within the `fileSystemShould` block
  * for each file system in `fileSystems`.
  *
  * TODO: Currently, examples for a single filesystem are executed concurrently,
  *       but the suites themselves are executed sequentially due to the `step`s
  *       inserted for setup/teardown. It'd be nice if the tests for all
  *       filesystems would run concurrently.
  */
abstract class FileSystemTest[S[_]](
  val fileSystems: Task[IList[SupportedFs[S]]]
) extends quasar.Qspec {

  sequential

  type F[A]      = Free[S, A]
  type FsTask[A] = FileSystemErrT[Task, A]
  type Run       = F ~> Task

  def fileSystemShould(examples: (FileSystemUT[S], FileSystemUT[S]) => Fragment): Fragments =
    fileSystems.map { fss =>
      Fragments.foreach(fss.toList)(fs =>
        (fs.impl |@| fs.implNonChrooted.orElse(fs.impl)) { (f0, f1) =>
          s"${fs.ref.name.shows} FileSystem" >>
            Fragments(examples(f0, f1), step(f0.close.unsafePerformSync))
        } getOrElse {
          val confParamName = TestConfig.backendConfName(fs.ref.name)
          Fragments(s"${fs.ref.name.shows} FileSystem" >> 
            skipped(s"No connection uri found to test this FileSystem, set config parameter $confParamName in '${TestConfig.confFile}' in order to do so"))
        })
    }.unsafePerformSync

  /** Returns the given result if the filesystem supports the given capabilities or `Skipped` otherwise. */
  def ifSupports[A: AsResult](fs: FileSystemUT[S], capability: BackendCapability, capabilities: BackendCapability*)(a: => A): Result =
    NonEmptyList(capability, capabilities: _*)
      .traverse_(c => fs.supports(c).fold(().successNel[BackendCapability], c.failureNel))
      .as(AsResult(a))
      .valueOr(cs => skipped(s"Doesn't support: ${cs.map(_.shows).intercalate(", ")}"))

  def pendingFor[A: AsResult](fs: FileSystemUT[S])(toPend: Set[String])(a: => A): Result = {
    val name: String = fs.ref.name.name
    toPend.contains(name).fold(pending(s"PENDING: Not supported for $name."), AsResult(a))
  }

  def pendingForF[A: AsResult](fs: FileSystemUT[S])(toPend: Set[String])(fa: => F[A])(implicit run: Run): Result =
    pendingFor(fs)(toPend)(run(fa).unsafePerformSync)

  def runT(run: Run): FileSystemErrT[F, ?] ~> FsTask =
    Hoist[FileSystemErrT].hoist(run)

  def runLog[A](run: Run, p: Process[F, A]): Task[Vector[A]] =
    p.translate[Task](run).runLog

  def runLogT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[Vector[A]] =
    p.translate[FsTask](runT(run)).runLog

  def execT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[Unit] =
    p.translate[FsTask](runT(run)).run

  ////

  implicit class FSExample(s: String) {
    def >>*[A: AsResult](fa: => F[A])(implicit run: Run): Fragment =
      s >> run(fa).unsafePerformSync
  }

  implicit class RunFsTask[A](fst: FsTask[A]) {
    import Leibniz.===

    def run_\/ : FileSystemError \/ A =
      fst.run.unsafePerformSync

    def runEither: Either[FileSystemError, A] =
      fst.run.unsafePerformSync.toEither

    def runOption(implicit ev: A === Unit): Option[FileSystemError] =
      fst.run.unsafePerformSync.swap.toOption

    def runVoid(implicit ev: A === Unit): Unit =
      fst.run.void.unsafePerformSync
  }
}

object FileSystemTest {

  val oneDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("a" -> Data.Int(1))))

  val anotherDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("b" -> Data.Int(2))))

  def manyDocs(n: Int): EStream[Data] =
    EStream.range(0, n) map (n => Data.Obj(ListMap("a" -> Data.Int(n))))

  def vectorFirst[A]: Optional[Vector[A], A] =
    Index.index[Vector[A], Int, A](0)

  //--- FileSystems to Test ---

  def allFsUT: Task[IList[SupportedFs[BackendEffect]]] =
    (localFsUT |@| externalFsUT) { (loc, ext) =>
      (loc ::: ext) map (sf => sf.copy(impl = sf.impl.map(ut => ut.contramapF(chroot.backendEffect[BackendEffect](ut.testDir)))))
    }

  def fsTestConfig(fsType: FileSystemType, fsDef: BackendDef[Free[filesystems.Eff, ?]])
    : PartialFunction[(MountConfig, ADir), Task[(BackendEffect ~> Task, Task[Unit])]] = {
    case (MountConfig.FileSystemConfig(FileSystemType(fsType.value), uri), dir) =>
      filesystems.testFileSystem(uri, dir, fsDef.apply(fsType, uri).run)
  }

  def externalFsUT = {
    val marklogicDef =
      MarkLogic(10000L, 10000L).definition translate injectFT[Task, filesystems.Eff]

    TestConfig.externalFileSystems {
      fsTestConfig(couchbase.fs.FsType,       Couchbase.definition translate injectFT[Task, filesystems.Eff]) orElse
      fsTestConfig(marklogic.fs.FsType,       marklogicDef) orElse
      fsTestConfig(mimir.Mimir.Type,          mimir.Mimir.definition translate injectFT[Task, filesystems.Eff]) orElse
      fsTestConfig(mongodb.fs.FsType,         mongodb.fs.definition) orElse
      fsTestConfig(mongodb.fs.QScriptFsType,  mongodb.fs.qscriptDefinition) orElse
      fsTestConfig(sparkcore.fs.hdfs.FsType,  sparkcore.fs.hdfs.definition) orElse
      fsTestConfig(sparkcore.fs.local.FsType, sparkcore.fs.local.definition) orElse
      fsTestConfig(sparkcore.fs.elastic.FsType, sparkcore.fs.elastic.definition) orElse
      fsTestConfig(sparkcore.fs.cassandra.FsType,  sparkcore.fs.cassandra.definition)
    }
  }

  def localFsUT: Task[IList[SupportedFs[BackendEffect]]] =
    (inMemUT |@| hierarchicalUT |@| nullViewUT) { (mem, hier, nullUT) =>
      IList(
        SupportedFs(mem.ref, mem.some),
        SupportedFs(hier.ref, hier.some),
        SupportedFs(nullUT.ref, nullUT.some)
      )
    }

  def nullViewUT: Task[FileSystemUT[BackendEffect]] =
    (
      inMemUT                                             |@|
      TaskRef(0L)                                         |@|
      ViewState.toTask(Map())                             |@|
      TaskRef(Map[APath, MountConfig]())                  |@|
      TaskRef(Empty.backendEffect[HierarchicalFsEffM])       |@|
      TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]])
    ) {
      (mem, seqRef, viewState, cfgsRef, hfsRef, mntdRef) =>

      val mounting: Mounting ~> Task = {
        val toPhysFs = KvsMounter.interpreter[Task, PhysFsEff](
          KeyValueStore.impl.fromTaskRef(cfgsRef), hfsRef, mntdRef)

        foldMapNT(reflNT[Task] :+: Failure.toRuntimeError[Task, PhysicalError])
          .compose(toPhysFs)
      }

      type ViewBackendEffect[A] = (
        Mounting
          :\: PathMismatchFailure
          :\: MountingFailure
          :\: ViewState
          :\: MonotonicSeq
          :/: BackendEffect
      )#M[A]

      val memPlus: ViewBackendEffect ~> Task = mounting :+:
      Failure.toRuntimeError[Task, Mounting.PathTypeMismatch] :+:
      Failure.toRuntimeError[Task, MountingError] :+:
      viewState :+:
      MonotonicSeq.fromTaskRef(seqRef) :+:
      mem.testInterp

      val fs = foldMapNT(memPlus) compose view.backendEffect[ViewBackendEffect]
      val ref = BackendRef.name.set(BackendName("No-view"))(mem.ref)

      FileSystemUT(ref, fs, fs, mem.testDir, mem.close)
    }

  def hierarchicalUT: Task[FileSystemUT[BackendEffect]] = {
    val mntDir: ADir = rootDir </> dir("mnt") </> dir("inmem")

    def fs(f: HfsIO ~> Task, r: BackendEffect ~> Task) =
      foldMapNT[HfsIO, Task](f) compose
        hierarchical.backendEffect[Task, HfsIO](Mounts.singleton(mntDir, r))

    (interpretHfsIO |@| inMemUT)((f, mem) =>
      FileSystemUT(
        BackendRef.name.set(BackendName("hierarchical"))(mem.ref),
        fs(f, mem.testInterp),
        fs(f, mem.setupInterp),
        mntDir,
        mem.close))
  }

  def inMemUT: Task[FileSystemUT[BackendEffect]] = {
    val ref = BackendRef(BackendName("in-memory"), ISet singleton BackendCapability.write())

    InMemory.runStatefully(InMemory.InMemState.empty)
      .map(_ compose InMemory.fileSystem)
      .map(f => Empty.analyze[Task] :+: f)
      .map(f => FileSystemUT(ref, f, f, rootDir, ().point[Task]))
  }
}
