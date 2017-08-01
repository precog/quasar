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

package quasar.fs.mount

import slamdata.Predef._

import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.{Failure, KeyValueStore, MonotonicSeq}
import quasar.fp._
import quasar.fs._, InMemory.InMemState
import quasar.frontend.logicalplan.{Free => _, free => _, _}
import quasar.sql._, ExprArbitrary._
import quasar.std._, IdentityLib.Squash, StdLib._, set._

import eu.timepit.refined.auto._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._

import monocle.macros.Lenses

import pathy.{Path => PPath}, PPath._
import pathy.scalacheck.PathyArbitrary._

import scalaz.{Failure => _, _}, Scalaz._

class ViewFileSystemSpec extends quasar.Qspec with TreeMatchers {
  import TraceFS._
  import FileSystemError._
  import Mounting.PathTypeMismatch

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  val query  = QueryFile.Ops[FileSystem]
  val read   = ReadFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]
  val mounting = Mounting.Ops[ViewFileSystem]

  @Lenses
  case class VS(seq: Long, handles: ViewState.ViewHandles, mountConfigs: Map[APath, MountConfig], fs: InMemState)

  object VS {
    def empty = VS(0, Map.empty, Map.empty, InMemState.empty)

    def emptyWithViews(views: Map[AFile, Fix[Sql]]) =
      mountConfigs.set(views.map { case (p, expr) =>
        p -> MountConfig.viewConfig(ScopedExpr(expr, Nil), Variables.empty)
      })(empty)
  }

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import StateT.stateTMonadState

  type Errs = MountingError \/ PathTypeMismatch

  type VSF[F[_], A]   = StateT[F, VS, A]
  type VST[A]         = VSF[Trace, A]
  type ErrsT[F[_], A] = EitherT[F, Errs, A]
  type Traced[A]      = ErrsT[VST, A]

  type VSS[A]         = State[VS, A]
  type VFS[A]         = ErrsT[VSS, A]

  def runMounting[F[_]](implicit F: MonadState[F, VS]): Mounting ~> F =
    free.foldMapNT(KeyValueStore.impl.toState[F](VS.mountConfigs)) compose Mounter.trivial[MountConfigs]

  def runViewFileSystem[F[_]](
    runFileSystem: FileSystem ~> F
  )(implicit
    F0: MonadState[F, VS],
    F1: MonadError[F, Errs]
  ): ViewFileSystem ~> F =
    ViewFileSystem.interpret[F](
      runMounting[F],
      Failure.toError[F, Errs] compose Failure.mapError[PathTypeMismatch, Errs](_.right),
      Failure.toError[F, Errs] compose Failure.mapError[MountingError, Errs](_.left),
      KeyValueStore.impl.toState[F](VS.handles),
      MonotonicSeq.toState[F](VS.seq),
      runFileSystem)

  def traceViewFs(paths: Map[ADir, Set[PathSegment]]): ViewFileSystem ~> Traced =
    runViewFileSystem[Traced](
      liftMT[VST, ErrsT] compose
      liftMT[Trace, VSF] compose
      interpretFileSystem[Trace](qfTrace(paths), rfTrace, wfTrace, mfTrace))

  case class ViewInterpResultTrace[A](renderedTrees: Vector[RenderedTree], vs: VS, result: Errs \/ A)


  def viewInterpTrace[A](views: Map[AFile, Fix[Sql]], paths: Map[ADir, Set[PathSegment]], t: Free[FileSystem, A])
    : ViewInterpResultTrace[A] =
    viewInterpTrace(views, List.empty[AFile], paths, t)

  def viewInterpTrace[A](
    views: Map[AFile, Fix[Sql]], files: List[AFile], paths: Map[ADir, Set[PathSegment]], t: Free[FileSystem, A])
    : ViewInterpResultTrace[A] = {

    val mountViews: Free[ViewFileSystem, Unit] =
      views.toList.traverse_ { case (loc, expr) => mounting.mountView(loc, ScopedExpr(expr, Nil), Variables.empty) }

    val toBeTraced: Free[ViewFileSystem, A] =
      mountViews *> t.flatMapSuspension(view.fileSystem[ViewFileSystem])

    val (renderedTrees, (vs, r)) =
      toBeTraced.foldMap(traceViewFs(paths))
        .run.run(VS(0, Map.empty, Map.empty, InMemState.empty)).run

    ViewInterpResultTrace(renderedTrees, vs, r)
  }

  case class ViewInterpResult[A](vs: VS, result: Errs \/ A)

  def viewInterp[A](
    views: Map[AFile, Fix[Sql]],
    files: List[AFile],
    f: Free[FileSystem, A]
  ) : ViewInterpResult[A] = {

    val viewfs: ViewFileSystem ~> VFS =
      runViewFileSystem[VFS](liftMT[VSS, ErrsT] compose zoomNT[Id](VS.fs) compose InMemory.fileSystem)

    val memState = InMemState.fromFiles(files.strengthR(Vector[Data]()).toMap)

    val (vs, r) =
      f.foldMap(free.foldMapNT(viewfs) compose view.fileSystem[ViewFileSystem])
        .run.run(VS.fs.set(memState)(VS.emptyWithViews(views)))

    ViewInterpResult(vs, r)
  }

  def parseExpr(query: String) =
    fixParser.parseExpr(Query(query)).toOption.get

  implicit val RenderedTreeRenderTree = new RenderTree[RenderedTree] {
    def render(t: RenderedTree) = t
  }

  "ReadFile.open" should {
    "translate simple read to query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from `/zips`")
      val lp = queryPlan(expr, Variables.empty, rootDir, 0L, None).run.run._2.toOption.get

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
      val expr = parseExpr("select * from `/zips`")

      val views = Map(p -> expr)

      val f = (for {
        h <- read.unsafe.open(p, 5L, Some(10L))
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val expQ =
        Fix(Take(
          Fix(Drop(
            Fix(Squash(lpf.read(rootDir </> file("zips")))),
            lpf.constant(Data.Int(5)))),
          lpf.constant(Data.Int(10))))
      val exp = (for {
        h   <- query.unsafe.eval(expQ)
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }

    "translate read with view-view reference" in {
      val p0 = rootDir[Sandboxed] </> dir("view") </> file("view0")
      val p1 = rootDir[Sandboxed] </> dir("view") </> file("view1")

      val views = Map(
        p0 -> parseExpr("select * from `/zips`"),
        p1 -> parseExpr("select * from view0"))

      val f = (for {
        h <- read.unsafe.open(p1, 0L, None)
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val expQ = Fix(Squash(lpf.read(rootDir </> file("zips"))))
      val exp = (for {
        h   <- query.unsafe.eval(expQ)
        _   <- query.transforms.fsErrToExec(
                query.unsafe.more(h))
        _   <- query.transforms.fsErrToExec(
                EitherT.right(query.unsafe.close(h)))
      } yield ()).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }

    "translate read with constant" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("view0")

      val views = Map(
        p -> parseExpr("1 + 2"))

      val f = (for {
        h <- read.unsafe.open(p, 0L, None)
        _ <- read.unsafe.read(h)
        _ <- EitherT.right(read.unsafe.close(h))
      } yield ()).run

      val exp = ().point[Free[FileSystem, ?]]

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

      viewInterpTrace(views, Map(), f).result must_=== \/.right(\/.left((unknownReadHandle(ReadFile.ReadHandle(p, 0)))))
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

      viewInterpTrace(views, Map(), f).result must_=== \/.right(\/.right(()))
    }
  }

  "WriteFile.open" should {
    "fail with view path" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)
      val f = write.unsafe.open(p).run

      viewInterpTrace(views, Map(), f) must_=== ViewInterpResultTrace(
        Vector.empty,
        VS.emptyWithViews(views),
        \/.right(\/.left(FileSystemError.pathErr(PathError.invalidPath(p, "Cannot write to a view.")))))
    }
  }

  "ManageFile.move" should {
    import ManageFile._, MoveScenario._, MoveSemantics._

    val srcPath = rootDir </> dir("view") </> file("simpleZips")
    val dstPath = rootDir </> dir("foo") </> file("bar")
    val expr = parseExpr("select * from zips")

    def moveShouldSucceed(views: Map[AFile, Fix[Sql]], files: List[AFile], moveSemantic: MoveSemantics) = {
      val f = manage.move(fileToFile(srcPath, dstPath), moveSemantic).run

      viewInterp(views, files, f) must_=== ViewInterpResult(
        VS.emptyWithViews(Map(dstPath -> expr)),
        \/.right(\/.right(())))
    }

    def moveShouldFail
      (views: Map[AFile, Fix[Sql]], files: List[AFile], moveSemantic: MoveSemantics, pathError: PathError) = {
      val f = manage.move(fileToFile(srcPath, dstPath), moveSemantic).run

      viewInterp(views, files, f) must_=== ViewInterpResult(
        VS.fs.set(InMemState.fromFiles(files.strengthR(Vector[Data]()).toMap))(VS.emptyWithViews(views)),
        \/.right(\/.left(FileSystemError.pathErr(pathError))))
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

    "succeed when src and dst directory is outside the underlying filesystem" >> {
      val v1 = rootDir[Sandboxed] </> dir("view") </> file("viewA")
      val v2 = rootDir[Sandboxed] </> dir("view") </> file("viewB")
      val destDir = rootDir[Sandboxed] </> dir("zoo")
      val expr = parseExpr("select * from zips")

      val f = manage.move(dirToDir(rootDir </> dir("view"), destDir), MoveSemantics.FailIfExists).run

      viewInterp(Map(v1 -> expr, v2 -> expr), Nil, f) must_=== ViewInterpResult(
        VS.emptyWithViews(Map((destDir </> file("viewA")) -> expr, (destDir </> file("viewB")) -> expr)),
        \/.right(\/.right(())))
    }

    "move view and file subpaths" in {
      val srcDir = rootDir[Sandboxed] </> dir("view")
      val destDir = rootDir[Sandboxed] </> dir("zoo")
      val viewFile = file("simpleZips")
      val dataFile = file("complexFile")
      val expr = parseExpr("select * from zips")

      val f = manage.move(dirToDir(srcDir, destDir), MoveSemantics.FailIfExists).run

      viewInterp(Map((srcDir </> viewFile) -> expr), List(srcDir </> dataFile), f) must_=== ViewInterpResult(
        VS.emptyWithViews(Map((destDir </> viewFile) -> expr))
          .copy(fs = InMemState.fromFiles(List(destDir </> dataFile).map(_ -> Vector[Data]()).toMap)),
        \/.right(\/.right(())))
    }
  }

  "ManageFile.delete" should {
    "delete with view path" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = manage.delete(p).run

      viewInterpTrace(views, Map(), f) must_=== ViewInterpResultTrace(Vector.empty, VS.empty, \/.right(\/.right(())))
    }

    "delete with view subpath" in {
      val vp = rootDir[Sandboxed] </> dir("view")
      val p = vp </> file("simpleZips")
      val expr = parseExpr("select * from zips")

      val views = Map(p -> expr)

      val f = manage.delete(vp).run

      viewInterpTrace(views, Map(), f) must_=== ViewInterpResultTrace(
        traceInterp(f, Map())._1,
        VS.empty,
        \/.right(\/.right(())))
    }

  }

  "QueryFile.exec" should {
    "handle simple query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from `/zips`")

      val views = Map(p -> expr)

      val f = query.execute(lpf.read(rootDir </> dir("view") </> file("simpleZips")), rootDir </> file("tmp")).run.run

      val exp = query.execute(Fix(Squash(lpf.read(rootDir </> file("zips")))), rootDir </> file("tmp")).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.eval" should {
    "handle simple query" in {
      val p = rootDir[Sandboxed] </> dir("view") </> file("simpleZips")
      val expr = parseExpr("select * from `/zips`")

      val views = Map(p -> expr)

      val f = (for {
        h <- query.unsafe.eval(lpf.read(rootDir </> dir("view") </> file("simpleZips")))
        _ <- query.transforms.fsErrToExec(
              query.unsafe.more(h))
        _ <- query.transforms.toExec(
              query.unsafe.close(h))
      } yield ()).run.run

      val exp = (for {
        h <- query.unsafe.eval(Fix(Squash(lpf.read(rootDir </> file("zips")))))
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
      val expr = parseExpr("select * from `/zips`")

      val views = Map(p -> expr)

      val f = query.explain(lpf.read(rootDir </> dir("view") </> file("simpleZips"))).run.run

      val exp = query.explain(Fix(Squash(lpf.read(rootDir </> file("zips"))))).run.run

      viewInterpTrace(views, Map(), f).renderedTrees must beTree(traceInterp(exp, Map())._1)
    }
  }

  "QueryFile.ls" should {
    def twoNodes(aDir: ADir) =
      Map(aDir -> Set[PathSegment](FileName("afile").right, DirName("adir").left))

    "preserve files and dirs in the presence of non-conflicting views" >> prop { (aDir: ADir) =>
      val expr = parseExpr("select * from zips")

      val views = Map(
        (aDir </> file("view1")) -> expr,
        (aDir </> dir("views") </> file("view2")) -> expr)

      val f = query.ls(aDir).run

      viewInterpTrace(views, twoNodes(aDir), f) must_=== ViewInterpResultTrace(
        traceInterp(f, twoNodes(aDir))._1,
        VS.emptyWithViews(views),
        \/.right(\/.right(Set(
          FileName("afile").right,
          DirName("adir").left,
          FileName("view1").right,
          DirName("views").left))))
    }

    "overlay files and dirs with conflicting paths" >> prop { (aDir: ADir) =>
      val expr = parseExpr("select * from zips")

      val views = Map(
        (aDir </> file("afile")) -> expr,
        (aDir </> dir("adir") </> file("view1")) -> expr)

      val f = query.ls(aDir).run

      viewInterpTrace(views, twoNodes(aDir), f) must_=== ViewInterpResultTrace(
        traceInterp(f, twoNodes(aDir))._1,
        VS.emptyWithViews(views),
        \/.right(\/.right(Set(
          FileName("afile").right, // hides the regular file
          DirName("adir").left)))) // no conflict with same dir
    }

    "preserve empty dir result" >> prop { (aDir: ADir) =>
      val views = Map[AFile, Fix[Sql]]()

      val f = query.ls(aDir).run

      viewInterpTrace(views, Map(aDir -> Set()), f) must_=== ViewInterpResultTrace(
        traceInterp(f, Map(aDir -> Set()))._1,
        VS.emptyWithViews(views),
        \/.right(\/.right((Set()))))
    }

    "preserve error for non-existent dir" >> prop { (aDir: ADir) =>
      (aDir =/= rootDir) ==> {
        val views = Map[AFile, Fix[Sql]]()

        val f = query.ls(aDir).run

        viewInterpTrace(views, Map(), f) must_=== ViewInterpResultTrace(
          traceInterp(f, Map())._1,
          VS.emptyWithViews(views),
          \/.right(\/.left(FileSystemError.pathErr(PathError.pathNotFound(aDir)))))
      }
    }

    "preserve empty dir result at root" in {
      val views = Map[AFile, Fix[Sql]]()

      val f = query.ls(rootDir).run

      viewInterpTrace(views, Map(), f) must_=== ViewInterpResultTrace(
        traceInterp(f, Map())._1,
        VS.emptyWithViews(views),
        \/.right(\/.right((Set()))))
    }
  }

  "QueryFile.fileExists" should {
    "behave as underlying interpreter" >> prop { file: AFile =>
      val program = query.fileExists(file)

      val ops = traceInterp(program, Map())._1

      val hasFile = {
        val paths = Map(fileParent(file) -> Set(fileName(file).right[DirName]))
        viewInterpTrace(Map(), paths, program) must_=== ViewInterpResultTrace(ops, VS.empty, \/.right(true))
      }
      val noFile = {
        viewInterpTrace(Map(), Map(), program) must_=== ViewInterpResultTrace(ops, VS.empty, \/.right(false))
      }
      hasFile and noFile
    }

    "return true if there is a view at that path" >> prop { (file: AFile, expr: Fix[Sql]) =>
      val views = Map(file -> expr)

      val program = query.fileExists(file)

      viewInterp(views, Nil, program) must_=== ViewInterpResult(
        VS.emptyWithViews(views),
        \/.right(true))
    }
  }

  "resolveViewRefs" >> {
    def unsafeParse(sqlQry: String): Fix[Sql] =
      sql.fixParser.parseExpr(sql.Query(sqlQry)).toOption.get

    def resolvedRefs[A](views: Map[AFile, Fix[Sql]], lp: Fix[LogicalPlan]): FileSystemError \/ Fix[LogicalPlan] =
      view.resolveViewRefs[Mounting](lp).run
        .foldMap(runMounting[State[VS, ?]])
        .eval(VS.emptyWithViews(views))

    "no match" >> {
      resolvedRefs(Map(), lpf.read(rootDir </> file("zips"))) must
        beRightDisjunction.like { case r => r must beTreeEqual(lpf.read(rootDir </> file("zips"))) }
    }

    "trivial read" >> {
      val p = rootDir </> dir("view") </> file("justZips")
      val vs = Map[AFile, Fix[Sql]](p -> unsafeParse("select * from `/zips`"))

      resolvedRefs(vs, lpf.read(p)) must beRightDisjunction.like {
        case r => r must beTreeEqual(
          Fix(Squash(lpf.read(rootDir </> file("zips"))))
        )
      }
    }

    "trivial read with relative path" >> {
      val p = rootDir </> dir("foo") </> file("justZips")
      val vs = Map[AFile, Fix[Sql]](p -> unsafeParse("select * from zips"))

      resolvedRefs(vs, lpf.read(p)) must beRightDisjunction.like {
        case r => r must beTreeEqual(
          Fix(Squash(lpf.read(rootDir </> dir("foo") </> file("zips"))))
        )
      }
    }

    "non-trivial" >> {
      val inner = unsafeParse("select city, state from `/zips` order by state")

      val p = rootDir </> dir("view") </> file("simpleZips")

      val outer =
        Take(
          Drop(
            lpf.read(p),
            lpf.constant(Data.Int(5))).embed,
          lpf.constant(Data.Int(10))).embed

      val innerLP =
        quasar.precompile[Fix[LogicalPlan]](inner, Variables.empty, fileParent(p)).run.value.toOption.get

      val vs = Map[AFile, Fix[Sql]](p -> inner)

      val exp = quasar.preparePlan(Take(
          Drop(
            innerLP,
            lpf.constant(Data.Int(5))).embed,
          lpf.constant(Data.Int(10))).embed).run.value.toOption.get

      resolvedRefs(vs, outer) must beRightDisjunction.like {
        case r => r must beTreeEqual(exp)
      }
    }

    "multi-level" >> {
      val vs = Map[AFile, Fix[Sql]](
        (rootDir </> dir("view") </> file("view1")) ->
          unsafeParse("select * from `/zips`"),
        (rootDir </> dir("view") </> file("view2")) ->
          unsafeParse("select * from view1"))

      resolvedRefs(vs, lpf.read(rootDir </> dir("view") </> file("view2"))) must
        beRightDisjunction.like { case r => r must beTreeEqual(
          Squash(lpf.read(rootDir </> file("zips"))).embed)
        }
    }


    // Several tests for edge cases with view references:

    "multiple references" >> {
      // NB: joining a view to itself means two expanded reads. The main point is
      // that these references should not be mistaken for a circular reference.

      val vp = rootDir </> dir("view") </> file("view1")
      val zp = rootDir </> file("zips")

      val vs = Map[AFile, Fix[Sql]](
        vp -> unsafeParse("select * from `/zips`"))

      val q = lpf.join(
        lpf.read(vp),
        lpf.read(vp),
        JoinType.Inner,
        JoinCondition('__leftJoin0, '__rightJoin1, lpf.constant(Data.Bool(true))))

      val exp = lpf.join(
        Squash(lpf.read(zp)).embed,
        Squash(lpf.read(zp)).embed,
        JoinType.Inner,
        JoinCondition('__leftJoin2, '__rightJoin3, lpf.constant(Data.Bool(true))))

      resolvedRefs(vs, q) must beRightDisjunction.like { case r => r must beTreeEqual(exp) }
    }

    "self reference" >> {
      // NB: resolves to a read on the underlying collection, allowing a view
      // to act like a filter or decorator for an existing collection.

      val p = rootDir </> dir("foo") </> file("bar")

      val q = unsafeParse(s"select * from `${posixCodec.printPath(p)}` limit 10")

      val qlp =
        quasar.queryPlan(q, Variables.empty, rootDir, 0L, None)
          .run.value.toOption.get
          .valueOr(_ => scala.sys.error("impossible constant plan"))

      val vs = Map[AFile, Fix[Sql]](p -> q)

      resolvedRefs(vs, lpf.read(p)) must beRightDisjunction.like { case r => r must beTreeEqual(qlp) }
    }

    "circular reference" >> {
      // NB: this situation probably results from user error, but since this is
      // now the _only_ way the view definitions can be ill-formed, it seems
      // like a shame to introduce `\/` just to handle this case. Instead,
      // the inner reference is treated the same way as self-references, and
      // left un-expanded. That means the user will see an error when the query
      // is evaluated and there turns out to be no actual file called "view2".

      val v1p = rootDir </> dir("view") </> file("view1")
      val v2p = rootDir </> dir("view") </> file("view2")

      val vs = Map[AFile, Fix[Sql]](
        v1p -> unsafeParse(s"select * from `${posixCodec.printPath(v2p)}` offset 5"),
        v2p -> unsafeParse(s"select * from `${posixCodec.printPath(v1p)}` limit 10"))

      resolvedRefs(vs, lpf.read(v2p)) must beRightDisjunction.like {
        case r => r must beTreeEqual(
          Take(
            Squash(Drop(
              Squash(lpf.read(v2p)).embed,
              lpf.constant(Data.Int(5))).embed).embed,
            lpf.constant(Data.Int(10))).embed
        )
      }
    }
  }
}
