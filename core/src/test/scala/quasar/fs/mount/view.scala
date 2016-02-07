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

import quasar._, LogicalPlan.{Free => LPFree, _}
import quasar.effect._
import quasar.fp._
import quasar.fs._
import quasar.recursionschemes._
import quasar.std.StdLib._, set._

import eu.timepit.refined.auto._
import monocle.macros.GenLens
import org.specs2.mutable._
import org.specs2.ScalaCheck
import pathy.{Path => PPath}, PPath._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import org.scalacheck.{Arbitrary, Gen}

class ViewFSSpec extends Specification with ScalaCheck with TreeMatchers {
  import TraceFS._
  import FileSystemError._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  case class VS(seq: Long, handles: ViewHandles)
  val _seq = GenLens[VS](_.seq)
  val _handles = GenLens[VS](_.handles)

  implicit val arbLogicalPlan: Arbitrary[Fix[LogicalPlan]] = Arbitrary(Gen.const(Read(Path("/zips"))))

  type VSF[F[_], A] = StateT[F, VS, A]
  type TraceS[S, A] = StateT[Trace, S, A]
  type VST[A]       = TraceS[VS, A]

  def traceViewFs(paths: Map[ADir, Set[PathName]]): ViewFileSystem ~> VST =
    interpretViewFileSystem[VST](
      KeyValueStore.toState[TraceS](_handles),
      MonotonicSeq.toState[TraceS](_seq),
      liftMT[Trace, VSF] compose
        interpretFileSystem[Trace](qfTrace(paths), rfTrace, wfTrace, mfTrace))

  def viewInterp[A](views: Views, paths: Map[ADir, Set[PathName]], t: Free[FileSystem, A]): (Vector[RenderedTree], A) =
    (t flatMapSuspension view.fileSystem[ViewFileSystem](views))
      .foldMap(traceViewFs(paths))
      .eval(VS(0, Map.empty)).run

  implicit val RenderedTreeRenderTree = new RenderTree[RenderedTree] {
    def render(t: RenderedTree) = t
  }

  "ReadFile.open" should {
    "translate simple read to query" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val exp = (for {
        h   <- query.unsafe.eval(q)
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterp(views, Map(), f)._1 must beTree(traceInterp(exp, Map())._1)
    }

    "translate limited read to query" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = (for {
        h <- read.unsafe.open(p, 5L, Some(10L))
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val expQ =
        Fix(Take(
          Fix(Drop(
            Read(Path("/zips")),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10))))
      val exp = (for {
        h   <- query.unsafe.eval(expQ)
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterp(views, Map(), f)._1 must beTree(traceInterp(exp, Map())._1)
    }

    "read from closed handle (error)" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- EitherT.right(read.unsafe.close(h))
        _ <- read.unsafe.read(h)
      } yield ()).run

      viewInterp(views, Map(), f)._2 must_== -\/(UnknownReadHandle(ReadFile.ReadHandle(p, 0)))
    }

    "double close (no-op)" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- EitherT.right(read.unsafe.close(h))
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      viewInterp(views, Map(), f)._2 must_== \/-(())
    }
  }

  "WriteFile.open" should {
    "fail with view path" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = write.unsafe.open(p).run

      viewInterp(views, Map(), f) must_==(
        (Vector.empty,
          -\/(FileSystemError.PathError(PathError2.InvalidPath(p, "cannot write to view")))))
    }
  }

  "ManageFile.move" should {
    import ManageFile._, MoveScenario._, MoveSemantics._

    "fail with view source path" in {
      val viewPath = rootDir </> dir("view") </> file("simpleZips")
      val otherPath = rootDir </> dir("foo") </> file("bar")
      val q = Read(Path("/zips"))

      val views = Views(Map(viewPath -> q))

      val f = manage.move(FileToFile(viewPath, otherPath), Overwrite).run

      viewInterp(views, Map(), f) must_==(
        (Vector.empty,
          -\/(FileSystemError.PathError(PathError2.InvalidPath(viewPath, "cannot move view")))))
    }

    "fail with view destination path" in {
      val viewPath = rootDir </> dir("view") </> file("simpleZips")
      val otherPath = rootDir </> dir("foo") </> file("bar")
      val q = Read(Path("/zips"))

      val views = Views(Map(viewPath -> q))

      val f = manage.move(FileToFile(otherPath, viewPath), Overwrite).run

      viewInterp(views, Map(), f) must_==(
        (Vector.empty,
          -\/(FileSystemError.PathError(PathError2.InvalidPath(viewPath, "cannot move file to view location")))))
    }
  }

  "ManageFile.delete" should {
    "fail with view path" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = manage.delete(p).run

      viewInterp(views, Map(), f) must_==(
        (Vector.empty,
          -\/(FileSystemError.PathError(PathError2.InvalidPath(p, "cannot delete view")))))
    }
  }

  "QueryFile.exec" should {
    "handle simple query" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = query.execute(Read(Path("/view/simpleZips")), rootDir </> file("tmp")).run.run

      val exp = query.execute(Read(Path("/zips")), rootDir </> file("tmp")).run.run

      viewInterp(views, Map(), f)._1 must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.eval" should {
    "handle simple query" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = (for {
        h <- query.unsafe.eval(Read(Path("/view/simpleZips")))
        _ <- query.transforms.fsErrToExec(
              query.unsafe.more(h))
        _ <- query.transforms.toExec(
              query.unsafe.close(h))
      } yield ()).run.run

      val exp = (for {
        h <- query.unsafe.eval(Read(Path("/zips")))
        _ <- query.transforms.fsErrToExec(
              query.unsafe.more(h))
        _ <- query.transforms.toExec(
              query.unsafe.close(h))
      } yield ()).run.run

      viewInterp(views, Map(), f)._1 must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.explain" should {
    "handle simple query" in {
      val p = rootDir </> dir("view") </> file("simpleZips")
      val q = Read(Path("/zips"))

      val views = Views(Map(p -> q))

      val f = query.explain(Read(Path("/view/simpleZips"))).run.run

      val exp = query.explain(Read(Path("/zips"))).run.run

      viewInterp(views, Map(), f)._1 must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.ls" should {
    def twoNodes(aDir: ADir) =
      Map(aDir -> Set[PathName](FileName("afile").right, DirName("adir").left))

    "preserve files and dirs in the presence of non-conflicting views" ! prop { (aDir: ADir) =>
      val views = Views(Map(
        (aDir </> file("view1")) -> Read(Path("/zips")),
        (aDir </> dir("views") </> file("view2")) -> Read(Path("/zips"))))

      val f = query.ls(aDir).run

      viewInterp(views, twoNodes(aDir), f) must_==(
        (traceInterp(f, twoNodes(aDir))._1,
          \/-(Set(
            FileName("afile").right,
            DirName("adir").left,
            FileName("view1").right,
            DirName("views").left))))
    }

    "overlay files and dirs with conflicting paths" ! prop { (aDir: ADir) =>
      val views = Views(Map(
        (aDir </> file("afile")) -> Read(Path("/zips")),
        (aDir </> dir("adir") </> file("view1")) -> Read(Path("/zips"))))

      val f = query.ls(aDir).run

      viewInterp(views, twoNodes(aDir), f) must_==(
        (traceInterp(f, twoNodes(aDir))._1,
          \/-(Set(
            FileName("afile").right,  // hides the regular file
            DirName("adir").left))))  // no conflict with same dir
    }

    "preserve empty dir result" ! prop { (aDir: ADir) =>
      val views = Views(Map())

      val f = query.ls(aDir).run

      viewInterp(views, Map(aDir -> Set()), f) must_==(
        (traceInterp(f, Map(aDir -> Set()))._1,
          \/-(Set())))
    }

    "preserve error for non-existent dir" ! prop { (aDir: ADir) =>
      (aDir =/= rootDir) ==> {
        val views = Views(Map())

        val f = query.ls(aDir).run

        viewInterp(views, Map(), f) must_==(
          (traceInterp(f, Map())._1,
            -\/(FileSystemError.pathError(PathError2.PathNotFound(aDir)))))
      }
    }

    "preserve empty dir result at root" in {
      val views = Views(Map())

      val f = query.ls(rootDir).run

      viewInterp(views, Map(), f) must_==(
        (traceInterp(f, Map())._1,
          \/-(Set())))
    }
  }

  "QueryFile.fileExists" should {
    "behave as underlying interpreter" ! prop { file: AFile =>
      val program = query.fileExists(file).run

      val ops = traceInterp(program, Map())._1

      val hasFile = {
        val paths = Map(fileParent(file) -> Set(fileName(file).right[DirName]))
        val expected = (ops, true.right)
        viewInterp(Views.empty, paths, program) must_== expected
      }
      val noFile = {
        val expected = (ops, false.right)
        viewInterp(Views.empty, Map(), program) must_== expected
      }
      hasFile and noFile
    }

    "return true if there is a view at that path" ! prop { (file: AFile, lp: Fix[LogicalPlan]) =>
      val program = query.fileExists(file).run

      val ops = traceInterp(program, Map())._1

      val expected = (ops, true.right)

      viewInterp(Views(Map(file -> lp)), Map(), program) must_== expected
    }
  }
}
