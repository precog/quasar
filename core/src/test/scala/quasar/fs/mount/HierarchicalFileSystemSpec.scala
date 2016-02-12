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

package quasar.fs.mount

import quasar.Predef._
import quasar.{Data, LogicalPlan}
import quasar.effect._
import quasar.fp.{hoistFree, liftMT, free, zoomNT}
import quasar.fs._
import quasar.recursionschemes.Fix
import quasar.std.IdentityLib.Squash
import quasar.std.SetLib.Take

import monocle.Lens
import org.specs2.mutable
import pathy.Path._
import scalaz.{Lens => _, Failure => _, _}, Id.Id
import scalaz.syntax.either._
import scalaz.std.list._

class HierarchicalFileSystemSpec extends mutable.Specification with FileSystemFixture {
  import InMemory.InMemState, FileSystemError._, PathError2._
  import hierarchical.{HFSErrT, HierarchicalFileSystemError, HFSFailure, HFSFailureF, MountedResultH, MountedResultHF}
  import ManageFile.MoveSemantics, QueryFile.ResultHandle, LogicalPlan._

  val transforms = QueryFile.Transforms[F]
  val unsafeq = QueryFile.Unsafe[FileSystem]

  type ExecM[A] = transforms.ExecM[A]
  type MountedFs[A] = State[MountedState, A]
  type MountedFsE[E, A] = EitherT[MountedFs, E, A]
  type HFSM[A] = HFSErrT[MountedFs, A]

  type HEff0[A] = Coproduct[HFSFailureF, MountedFs, A]
  type HEff1[A] = Coproduct[MountedResultHF, HEff0, A]
  type HEff[A]  = Coproduct[MonotonicSeqF, HEff1, A]
  type HEffM[A] = Free[HEff, A]

  type RHandles = Map[ResultHandle, (ADir, ResultHandle)]

  case class MountedState(
    n: Long,
    h: RHandles,
    a: InMemState,
    b: InMemState,
    c: InMemState)

  def emptyMS: MountedState =
    MountedState(0, Map.empty, InMemState.empty, InMemState.empty, InMemState.empty)

  val mntA: ADir = rootDir </> dir("bar") </> dir("mntA")
  val mntB: ADir = rootDir </> dir("bar") </> dir("mntB")
  val mntC: ADir = rootDir </> dir("foo") </> dir("mntC")

  val seq: Lens[MountedState, Long]         = Lens((_: MountedState).n)(x => ms => ms.copy(n = x))
  val handles: Lens[MountedState, RHandles] = Lens((_: MountedState).h)(m => ms => ms.copy(h = m))
  val aMem: Lens[MountedState, InMemState]  = Lens((_: MountedState).a)(s => ms => ms.copy(a = s))
  val bMem: Lens[MountedState, InMemState]  = Lens((_: MountedState).b)(s => ms => ms.copy(b = s))
  val cMem: Lens[MountedState, InMemState]  = Lens((_: MountedState).c)(s => ms => ms.copy(c = s))

  val interpHEff: HEff ~> HFSM = {
    val seqNT: MonotonicSeqF ~> HFSM =
      liftMT[MountedFs, HFSErrT].compose[MonotonicSeqF](
        Coyoneda.liftTF[MonotonicSeq, MountedFs](MonotonicSeq.toState[State](seq)))

    val failNT: HFSFailureF ~> HFSM =
      Coyoneda.liftTF[HFSFailure, HFSM](
        Failure.toError[MountedFsE, HierarchicalFileSystemError])

    val handlesNT: MountedResultHF ~> HFSM =
      liftMT[MountedFs, HFSErrT].compose[MountedResultHF](
        Coyoneda.liftTF[MountedResultH, MountedFs](KeyValueStore.toState[State](handles)))

    free.interpret4(seqNT, handlesNT, failNT, liftMT[MountedFs, HFSErrT]: (MountedFs ~> HFSM))
  }

  val interpretMnted: FileSystem ~> HEffM =
    hierarchical.fileSystem[MountedFs, HEff](Mounts.fromFoldable(List(
      (mntA, zoomNT[Id](aMem) compose Mem.interpretTerm),
      (mntB, zoomNT[Id](bMem) compose Mem.interpretTerm),
      (mntC, zoomNT[Id](cMem) compose Mem.interpretTerm)
    )).toOption.get)

  val runMntd: F ~> HFSM =
    hoistFree(hoistFree(interpHEff).compose[FileSystem](interpretMnted))

  val runEmpty: F ~> HFSM = {
    val interpEmpty: FileSystem ~> HEffM =
      hierarchical.fileSystem[MountedFs, HEff](Mounts.empty)
    hoistFree(hoistFree(interpHEff).compose[FileSystem](interpEmpty))
  }

  // NB: Defining these here for a reuse, but also because using `beLike`
  //     inline triggers a scalac bug that results in malformed class files
  def succeedH[A] =
    beLike[HierarchicalFileSystemError \/ (FileSystemError \/ A)] {
      case \/-(\/-(_)) => ok
    }

  def failDueToInvalidPath[A](p: APath) =
    beLike[HierarchicalFileSystemError \/ (FileSystemError \/ A)] {
      case \/-(-\/(PathError(InvalidPath(p0, _)))) => p0 must_== p
    }

  def failDueToMultipleMnts[A] =
    beLike[HierarchicalFileSystemError \/ (FileSystemError \/ A)] {
      case -\/(HierarchicalFileSystemError.MultipleMountsApply(_, _)) => ok
    }

  def failsForDifferentFs[A](f: (Fix[LogicalPlan], AFile) => ExecM[A]) =
    "should fail if any plan paths refer to different filesystems" >> {
      import quasar.{queryPlan, Variables}
      import quasar.sql._

      val joinQry =
        "select f.x, q.y from `/bar/mntA/foo` as f inner join `/foo/mntC/quux` as q on f.id = q.id"

      val lp = new SQLParser().parse(Query(joinQry)).toOption
        .flatMap(expr => queryPlan(expr, Variables(Map())).run.value.toOption)
        .get

      runMntd(f(lp, mntA </> file("out0")).run.value)
        .run.eval(emptyMS) must failDueToInvalidPath(mntC)
    }

  def failsWhenNoPaths[A](f: Fix[LogicalPlan] => ExecM[A]) =
    "containing no paths fails with HFS error" >> {
      runMntd(f(Constant(Data.Obj(Map("0" -> Data.Int(5))))).run.value)
        .run.eval(emptyMS) must failDueToMultipleMnts
    }

  def succeedsForMountedPath[A](f: Fix[LogicalPlan] => ExecM[A]) =
    "containing a mounted path succeeds" >> {
      val local = dir("d1") </> file("f1")
      val mnted = mntB </> local

      val lp = Invoke(Take, List(
        Invoke(Squash, List(Read(Path(posixCodec.printPath(mnted))))),
        Constant(Data.Int(5))))

      val fss = bMem.set(
        InMemState.fromFiles(Map((rootDir </> local) -> Vector(Data.Int(1)))))(
        emptyMS)

      runMntd(f(lp).run.value).run.eval(fss) must succeedH
    }

  "Mounted filesystems" should {
    "QueryFile" >> {
      "executing a plan" >> {
        failsForDifferentFs((lp, out) => query.execute(lp, out))

        "should fail when output path and plan paths refer to different filesystems" >> {
          val rd = mntB </> file("f1")
          val out = mntC </> file("outf")

          val lp = Invoke(Take, List(
            Invoke(Squash, List(Read(Path(posixCodec.printPath(rd))))),
            Constant(Data.Int(5))))

          val fss = bMem.set(InMemState.fromFiles(Map(rd -> Vector(Data.Int(1)))))(emptyMS)

          runMntd(query.execute(lp, out).run.value)
            .run.eval(fss) must failDueToInvalidPath(mntB)
        }

        "containing no paths succeeds" >> {
          val out = mntC </> file("outfile")
          runMntd(query.execute(Constant(Data.Obj(Map("0" -> Data.Int(3)))), out).run.value)
            .run.eval(emptyMS) must_== out.right.right
        }
      }

      "evaluating a plan" >> {
        failsForDifferentFs((lp, _) => unsafeq.eval(lp))

        failsWhenNoPaths(unsafeq.eval)

        succeedsForMountedPath(unsafeq.eval)
      }

      "explaining a plan" >> {
        failsForDifferentFs((lp, _) => query.explain(lp))

        failsWhenNoPaths(query.explain)

        succeedsForMountedPath(query.explain)
      }

      "listing children" >> {
        "of root dir when no mounts should return empty set" >> {
          runEmpty(query.ls(rootDir).run).run.eval(emptyMS) must_== Set().right.right
        }

        "of mount ancestor dir should return dir names" >> {
          val dirs = Set(DirName("bar").left, DirName("foo").left)
          runMntd(query.ls(rootDir).run).run.eval(emptyMS) must_== dirs.right.right
        }

        "of mount parent dir should return mount names" >> {
          val mnts = Set(DirName("mntA").left, DirName("mntB").left)
          runMntd(query.ls(rootDir </> dir("bar")).run)
            .run.eval(emptyMS) must_== mnts.right.right
        }
      }
    }

    "ManageFile" >> {
      "move dir should fail when dst not in same filesystem" >> {
        val src = mntA </> dir("srcdir")
        val dst = mntC </> dir("dstdir")
        val fss = emptyMS.copy(a = InMemState.fromFiles(Map(
          (src </> file("f1")) -> Vector(Data.Str("contents")))))

        runMntd(manage.moveDir(src, dst, MoveSemantics.Overwrite).run)
          .run.eval(fss) must failDueToInvalidPath(dst)
      }

      "move file should fail when dst not in same filesystem" >> {
        val src = mntA </> dir("srcdir") </> file("f1")
        val dst = mntC </> dir("dstdir") </> file("f2")
        val fss = emptyMS.copy(a = InMemState.fromFiles(Map(
          src -> Vector(Data.Str("contents")))))

        runMntd(manage.moveFile(src, dst, MoveSemantics.Overwrite).run)
          .run.eval(fss) must failDueToInvalidPath(dst)
      }

      "deleting a mount point should delete all data in mounted filesystem" >> {
        val f1 = mntB </> dir("d1") </> file("f1")
        val f2 = mntB </> file("f2")

        val fss = emptyMS.copy(b = InMemState.fromFiles(Map(
          f1 -> Vector(Data.Str("conts1")),
          f2 -> Vector(Data.Int(42)))))

        runMntd(manage.delete(mntB).run)
          .run.exec(fss) must_== emptyMS
      }

      "deleting the ancestor of one or more mount points should delete all data in their filesystems" >> {
        val f1 = mntB </> dir("d1") </> file("f1")
        val f2 = mntB </> dir("d1") </> file("f2")
        val f3 = mntA </> dir("d2") </> file("f3")
        val f4 = mntA </> dir("d2") </> file("f4")

        val fss = emptyMS.copy(
          b = InMemState.fromFiles(Map(
            f1 -> Vector(Data.Str("file1")),
            f2 -> Vector(Data.Str("file2")))),
          a = InMemState.fromFiles(Map(
            f3 -> Vector(Data.Str("file3")),
            f4 -> Vector(Data.Str("file4")))))

        runMntd(manage.delete(rootDir </> dir("bar")).run)
          .run.exec(fss) must_== emptyMS
      }
    }
  }

}
