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

package quasar.fs

import quasar.Predef._
import quasar.{BackendName, Data, TestConfig}
import quasar.fp.{eitherTCatchable, TaskRef}
import quasar.fp.free._
import quasar.fs.mount._
import quasar.effect._
import quasar.physical._
import quasar.regression.{interpretHfsIO, HfsIO}

import scala.Either

import monocle.Optional
import monocle.function.Index
import monocle.std.vector._
import org.specs2.specification.core.Fragments
import org.specs2.mutable.Specification
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
  val fileSystems: Task[IList[FileSystemUT[S]]]
) extends Specification {

  args.report(showtimes = true)

  type F[A]      = Free[S, A]
  type FsTask[A] = FileSystemErrT[Task, A]
  type Run       = F ~> Task

  def fileSystemShould(examples: FileSystemUT[S] => Fragments): Unit =
    fileSystems.map(_ traverse_[Id] { fs =>
      s"${fs.name.name} FileSystem" should examples(fs)

      step(fs.close.unsafePerformSync)

      ()
    }).unsafePerformSync

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
    def >>*[A: AsResult](fa: => F[A])(implicit run: Run) =
      s >> run(fa).unsafePerformSync
  }

  implicit class RunFsTask[A](fst: FsTask[A]) {
    import Leibniz.===

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

  def allFsUT: Task[IList[FileSystemUT[FileSystem]]] =
    (localFsUT |@| externalFsUT) { (loc, ext) =>
      (loc ::: ext) map (ut => ut.contramapF(chroot.fileSystem[FileSystem](ut.testDir)))
    }

  def fsTestConfig(fsType: FileSystemType, fsDef: FileSystemDef[Free[filesystems.Eff, ?]])
    : PartialFunction[(MountConfig, ADir), Task[(FileSystem ~> Task, Task[Unit])]] = {
    case (MountConfig.FileSystemConfig(FileSystemType(fsType.value), uri), dir) =>
      filesystems.testFileSystem(uri, dir, fsDef.apply(fsType, uri).run)
  }

  def externalFsUT = TestConfig.externalFileSystems {
    fsTestConfig(mongodb.fs.MongoDBFsType, mongodb.fs.mongoDbFileSystemDef) orElse
    fsTestConfig(skeleton.fs.FsType, skeleton.fs.definition)                orElse
    fsTestConfig(postgresql.fs.FsType, postgresql.fs.definition)
  }

  def localFsUT: Task[IList[FileSystemUT[FileSystem]]] =
    IList(
      inMemUT,
      hierarchicalUT,
      nullViewUT
    ).sequence

  def nullViewUT: Task[FileSystemUT[FileSystem]] =
    (inMemUT |@| TaskRef(0L) |@| ViewState.toTask(Map())) {
      (mem, seqRef, viewState) =>

      val memPlus: ViewFileSystem ~> Task =
        interpretViewFileSystem(
          KeyValueStore.fromTaskRef(TaskRef(Map.empty[APath, MountConfig]).unsafePerformSync),
          viewState,
          MonotonicSeq.fromTaskRef(seqRef),
          mem.testInterp)

      val fs = foldMapNT(memPlus) compose view.fileSystem[ViewFileSystem]

      FileSystemUT(BackendName("No-view"), fs, fs, mem.testDir, mem.close)
    }

  def hierarchicalUT: Task[FileSystemUT[FileSystem]] = {
    val mntDir: ADir = rootDir </> dir("mnt") </> dir("inmem")

    def fs(f: HfsIO ~> Task, r: FileSystem ~> Task) =
      foldMapNT[HfsIO, Task](f) compose
        hierarchical.fileSystem[Task, HfsIO](Mounts.singleton(mntDir, r))

    (interpretHfsIO |@| inMemUT)((f, mem) =>
      FileSystemUT(
        BackendName("hierarchical"),
        fs(f, mem.testInterp),
        fs(f, mem.setupInterp),
        mntDir,
        mem.close))
  }

  def inMemUT: Task[FileSystemUT[FileSystem]] = {
    InMemory.runStatefully(InMemory.InMemState.empty)
      .map(_ compose InMemory.fileSystem)
      .map(f => FileSystemUT(BackendName("in-memory"), f, f, rootDir, ().point[Task]))
  }
}
