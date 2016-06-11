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

import quasar._, LogicalPlan._
import quasar.effect._
import quasar.fp._
import quasar.fs._
import quasar.fs.InMemory.{InMemoryFs, InMemState}
import quasar.sql._
import quasar.sql.ExprArbitrary._
import quasar.std._, IdentityLib.Squash, StdLib._, set._

import eu.timepit.refined.auto._
import matryoshka.{free => _, _}
import monocle.macros.GenLens
import org.specs2.mutable._
import org.specs2.ScalaCheck
import pathy.{Path => PPath}, PPath._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class ViewFSSpec extends Specification with ScalaCheck with TreeMatchers {
  import TraceFS._
  import FileSystemError._

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  case class VS(seq: Long, handles: ViewHandles, mountConfigs: Map[APath, MountConfig], fs: InMemState)
  val _seq = GenLens[VS](_.seq)
  val _handles = GenLens[VS](_.handles)
  val _mountConfigs = GenLens[VS](_.mountConfigs)
  val _fs = GenLens[VS](_.fs)

  object VS {
    def empty = VS(0, Map.empty, Map.empty, InMemState.empty)

    def emptyWithViews(views: Map[AFile, Fix[Sql]]) =
      _mountConfigs.set(
        views.map { case (p, expr) => p -> MountConfig.viewConfig(expr, Variables.empty) }
      )(empty)
  }

  type VSF[F[_], A] = StateT[F, VS, A]
  type TraceS[S, A] = StateT[Trace, S, A]
  type VST[A]       = TraceS[VS, A]

  def traceViewFs(paths: Map[ADir, Set[PathSegment]]): ViewFileSystem ~> VST =
    interpretViewFileSystem[VST](
      KeyValueStore.toState[VST](_mountConfigs),
      KeyValueStore.toState[VST](_handles),
      MonotonicSeq.toState[VST](_seq),
      liftMT[Trace, VSF] compose
        interpretFileSystem[Trace](qfTrace(paths), rfTrace, wfTrace, mfTrace))

  case class ViewInterpResultTrace[A](renderedTrees: Vector[RenderedTree], vs: VS, result: A)

  def viewInterpTrace[A](views: Map[AFile, Fix[Sql]], paths: Map[ADir, Set[PathSegment]], t: Free[FileSystem, A])
    : ViewInterpResultTrace[A] =
    viewInterpTrace(views, List.empty[AFile], paths, t)

  def viewInterpTrace[A](
    views: Map[AFile, Fix[Sql]], files: List[AFile], paths: Map[ADir, Set[PathSegment]], t: Free[FileSystem, A])
    : ViewInterpResultTrace[A] = {
    val tFS: Free[ViewFileSystem, A] = t flatMapSuspension view.fileSystem[ViewFileSystem]

    val mountViews: Free[ViewFileSystem, List[MountingError] \/ Unit] = views.map {
      case (loc, expr) => ViewMounter.mount[ViewFileSystem](loc, expr, Variables.empty).map(_.leftMap(List(_)))
    }.toList.suml

    val mountViewsTFS: Free[ViewFileSystem, A] = mountViews.flatMap(κ(tFS))

    val mountViewsTFSTrace: VST[A] =
      mountViewsTFS.foldMap(traceViewFs(paths))

    val (renderedTrees, (vs, a)) = mountViewsTFSTrace(VS(0, Map.empty, Map.empty, InMemState.empty)).run

    ViewInterpResultTrace(renderedTrees, vs, a)
  }

  case class ViewInterpResult[A](vs: VS, result: A)

  def viewInterp[A]
    (views: Map[AFile, Fix[Sql]], files: List[AFile], f: Free[FileSystem, A])
    : ViewInterpResult[A] = {

    val fv: Free[ViewFileSystem, A] = f flatMapSuspension view.fileSystem[ViewFileSystem]

    val l = Lens.lensu[VS, InMemState]((vs, ims) => vs.copy(fs = ims), _.fs)

    val fvs: InMemoryFs ~> State[VS, ?] = new (InMemoryFs ~> State[VS, ?]) {
      def apply[A](fa: InMemoryFs[A]): State[VS, A] = fa.zoom(l)
    }

    val viewfs: ViewFileSystem ~> State[VS, ?] = interpretViewFileSystem[State[VS, ?]](
      KeyValueStore.toState[State[VS, ?]](_mountConfigs),
      KeyValueStore.toState[State[VS, ?]](_handles),
      MonotonicSeq.toState[State[VS, ?]](_seq),
      fvs compose InMemory.fileSystem)

    val r: (VS, A) =
      free.foldMapNT[ViewFileSystem, State[VS, ?]](viewfs).apply(fv).run(
        VS.emptyWithViews(views).copy(fs = InMemState.fromFiles(files.map(_ -> Vector[Data]()).toMap)))

    (ViewInterpResult[A] _).tupled(r)
  }

  def parseExpr(query: String) =
    fixParser.parseInContext(Query(query), rootDir[Sandboxed]).toOption.get

  implicit val RenderedTreeRenderTree = new RenderTree[RenderedTree] {
    def render(t: RenderedTree) = t
  }

  "ReadFile.open" should {
    "translate simple read to query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")
      val lp = queryPlan(expr, Variables.empty, 0L, None).run.run._2.toOption.get

      val views = Map(p -> expr)

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val exp = (for {
        h   <- query.unsafe.eval(lp.valueOr(_ => scala.sys.error("impossible constant plan")))
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }

    "translate limited read to query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = (for {
        h <- read.unsafe.open(p, 5L, Some(10L))
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val expQ =
        Fix(Take(
          Fix(Drop(
            Fix(Squash(Read(rootDir </> file("zips")))),
            Constant(Data.Int(5)))),
          Constant(Data.Int(10))))
      val exp = (for {
        h   <- query.unsafe.eval(expQ)
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }

    "read from closed handle (error)" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- EitherT.right(read.unsafe.close(h))
        _ <- read.unsafe.read(h)
      } yield ()).run

      viewInterpTrace(views, Map(), f).result must_== -\/(unknownReadHandle(ReadFile.ReadHandle(p, 0)))
    }

    "double close (no-op)" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- EitherT.right(read.unsafe.close(h))
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      viewInterpTrace(views, Map(), f).result must_== \/-(())
    }
  }

  "WriteFile.open" should {
    "fail with view path" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = write.unsafe.open(p).run

      viewInterpTrace(views, Map(), f) must_== ViewInterpResultTrace(
        Vector.empty,
        VS.emptyWithViews(views),
        -\/(FileSystemError.pathErr(PathError.invalidPath(p, "cannot write to view"))))
    }
  }

  "ManageFile.move" should {
    import ManageFile._, MoveScenario._, MoveSemantics._

    val srcPath = rootDir </> dir("view") </> file("simpleZips")
    val dstPath = rootDir </> dir("foo") </> file("bar")
    val expr = parseExpr("select * from zips")

    def moveShouldSucceed(views: Map[AFile, Fix[Sql]], files: List[AFile], moveSemantic: MoveSemantics) = {
      val f = manage.move(fileToFile(srcPath, dstPath), moveSemantic).run

      viewInterp(views, files, f) must_== ViewInterpResult(
        VS.emptyWithViews(Map(dstPath -> expr)),
        \/-(()))
    }

    def moveShouldFail
      (views: Map[AFile, Fix[Sql]], files: List[AFile], moveSemantic: MoveSemantics, pathError: PathError) = {
      val f = manage.move(fileToFile(srcPath, dstPath), moveSemantic).run

      viewInterp(views, files, f) must_== ViewInterpResult(
        VS.emptyWithViews(views).copy(fs = InMemState.fromFiles(files.map(_ -> Vector[Data]()).toMap)),
        -\/(FileSystemError.pathErr(pathError)))
    }

    "succeed when destination view exists and semantic is Overwrite" in
      moveShouldSucceed(Map(srcPath -> expr, dstPath -> expr), Nil, Overwrite)

    "succeed when destination file exists and semantic is Overwrite" in
      moveShouldSucceed(Map(srcPath -> expr), List(dstPath), Overwrite)

    "succeed when destination doesn't exist and semantic is Overwrite" in
      moveShouldSucceed(Map(srcPath -> expr), Nil, Overwrite)

    "succeed when destination doesn't exist and semantic is FailIfExists" in
      moveShouldSucceed(Map(srcPath -> expr), Nil, FailIfExists)

    "fail when destination view exists and semantic is FailIfExists" in
      moveShouldFail(Map(srcPath -> expr, dstPath -> expr), Nil, FailIfExists, PathError.pathExists(dstPath))

    "fail when destination file exists and semantic is FailIfExists" in
      moveShouldFail(Map(srcPath -> expr), List(dstPath), FailIfExists, PathError.pathExists(dstPath))

    "succeed when destination view exists and semantic is FailIfMissing" in
      moveShouldSucceed(Map(srcPath -> expr, dstPath -> expr), Nil, FailIfMissing)

    "succeed when destination file exists and semantic is FailIfMissing" in
      moveShouldSucceed(Map(srcPath -> expr), List(dstPath), FailIfMissing)

    "fail when destination doesn't exist and semantic is FailIfMissing" in
      moveShouldFail(Map(srcPath -> expr), Nil, FailIfMissing, PathError.pathNotFound(dstPath))

    "move view and file subpaths" in {
      val srcDir = rootDir[Sandboxed] </> dir("view")
      val destDir = rootDir[Sandboxed] </> dir("zoo")
      val viewFile = file("simpleZips")
      val dataFile = file("complexFile")
      val expr = parseExpr("select * from zips")

      val f = manage.move(dirToDir(srcDir, destDir), MoveSemantics.FailIfExists).run

      viewInterp(Map((srcDir </> viewFile) -> expr), List(srcDir </> dataFile), f) must_== ViewInterpResult(
        VS.emptyWithViews(Map((destDir </> viewFile) -> expr))
          .copy(fs = InMemState.fromFiles(List(destDir </> dataFile).map(_ -> Vector[Data]()).toMap)),
        \/-(()))
    }

  }

  "ManageFile.delete" should {
    "delete with view path" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = manage.delete(p).run

      viewInterpTrace(views, Map(), f) must_== ViewInterpResultTrace(Vector.empty, VS.empty, \/-(()))
    }

    "delete with view subpath" in {
      val vp = rootDir[Sandboxed] </> dir("view")
      val p = vp </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = manage.delete(vp).run

      viewInterpTrace(views, Map(), f) must_== ViewInterpResultTrace(
        traceInterp(f, Map())._1,
        VS.empty,
        \/-(()))
    }

  }

  "QueryFile.exec" should {
    "handle simple query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = query.execute(Read(rootDir </> dir("view") </> file("simpleZips")), rootDir </> file("tmp")).run.run

      val exp = query.execute(Fix(Squash(Read(rootDir </> file("zips")))), rootDir </> file("tmp")).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.eval" should {
    "handle simple query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = (for {
        h <- query.unsafe.eval(Read(rootDir </> dir("view") </> file("simpleZips")))
        _ <- query.transforms.fsErrToExec(
              query.unsafe.more(h))
        _ <- query.transforms.toExec(
              query.unsafe.close(h))
      } yield ()).run.run

      val exp = (for {
        h <- query.unsafe.eval(Fix(Squash(Read(rootDir </> file("zips")))))
        _ <- query.transforms.fsErrToExec(
              query.unsafe.more(h))
        _ <- query.transforms.toExec(
              query.unsafe.close(h))
      } yield ()).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.explain" should {
    "handle simple query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = query.explain(Read(rootDir </> dir("view") </> file("simpleZips"))).run.run

      val exp = query.explain(Fix(Squash(Read(rootDir </> file("zips"))))).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.ls" should {
    def twoNodes(aDir: ADir) =
      Map(aDir -> Set[PathSegment](FileName("afile").right, DirName("adir").left))

    "preserve files and dirs in the presence of non-conflicting views" ! prop { (aDir: ADir) =>
      val expr = parseExpr("select * from zips")

      val views = Map(
        (aDir </> file("view1")) -> expr,
        (aDir </> dir("views") </> file("view2")) -> expr)

      val f = query.ls(aDir).run

      viewInterpTrace(views, twoNodes(aDir), f) must_== ViewInterpResultTrace(
        traceInterp(f, twoNodes(aDir))._1,
        VS.emptyWithViews(views),
        \/-(Set(
          FileName("afile").right,
          DirName("adir").left,
          FileName("view1").right,
          DirName("views").left)))
    }

    "overlay files and dirs with conflicting paths" ! prop { (aDir: ADir) =>
      val expr = parseExpr("select * from zips")

      val views = Map(
        (aDir </> file("afile")) -> expr,
        (aDir </> dir("adir") </> file("view1")) -> expr)

      val f = query.ls(aDir).run

      viewInterpTrace(views, twoNodes(aDir), f) must_== ViewInterpResultTrace(
        traceInterp(f, twoNodes(aDir))._1,
        VS.emptyWithViews(views),
        \/-(Set(
          FileName("afile").right,  // hides the regular file
          DirName("adir").left)))  // no conflict with same dir
    }

    "preserve empty dir result" ! prop { (aDir: ADir) =>
      val views = Map[AFile, Fix[Sql]]()

      val f = query.ls(aDir).run

      viewInterpTrace(views, Map(aDir -> Set()), f) must_== ViewInterpResultTrace(
        traceInterp(f, Map(aDir -> Set()))._1,
        VS.emptyWithViews(views),
        \/-(Set()))
    }

    "preserve error for non-existent dir" ! prop { (aDir: ADir) =>
      (aDir =/= rootDir) ==> {
        val views = Map[AFile, Fix[Sql]]()

        val f = query.ls(aDir).run

        viewInterpTrace(views, Map(), f) must_== ViewInterpResultTrace(
          traceInterp(f, Map())._1,
          VS.emptyWithViews(views),
          -\/(FileSystemError.pathErr(PathError.pathNotFound(aDir))))
      }
    }

    "preserve empty dir result at root" in {
      val views = Map[AFile, Fix[Sql]]()

      val f = query.ls(rootDir).run

      viewInterpTrace(views, Map(), f) must_== ViewInterpResultTrace(
        traceInterp(f, Map())._1,
        VS.emptyWithViews(views),
        \/-(Set()))
    }
  }

  "QueryFile.fileExists" should {
    "behave as underlying interpreter" ! prop { file: AFile =>
      val program = query.fileExists(file)

      val ops = traceInterp(program, Map())._1

      val hasFile = {
        val paths = Map(fileParent(file) -> Set(fileName(file).right[DirName]))
        viewInterpTrace(Map(), paths, program) must_== ViewInterpResultTrace(ops, VS.empty, true)
      }
      val noFile = {
        viewInterpTrace(Map(), Map(), program) must_== ViewInterpResultTrace(ops, VS.empty, false)
      }
      hasFile and noFile
    }

    "return true if there is a view at that path" ! prop { (file: AFile, expr: Fix[Sql]) =>
      val views = Map(file -> expr)

      val program = query.fileExists(file)

      viewInterp(views, Nil, program) must_== ViewInterpResult(
        VS.emptyWithViews(views),
        true)
    }
  }
}
