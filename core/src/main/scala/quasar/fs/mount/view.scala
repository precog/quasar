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

import quasar.Predef._
import quasar._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.frontend.{SemanticErrors, SemanticErrsT}
import quasar.fs._, FileSystemError._, PathError._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, Optimizer}
import quasar.sql.{Sql, Blob}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

object view {
  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  /** Translate reads on view paths to the equivalent queries. */
  def readFile[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: QueryFile :<: S,
    S2: MonotonicSeq :<: S,
    S3: ViewState :<: S,
    S4: Mounting :<: S
  ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val seq = MonotonicSeq.Ops[S]
    val viewState = ViewState.Ops[S]
    val mount = Mounting.Ops[S]

    def openFile(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] =
      for {
        rh <- readUnsafe.open(f, off, lim)
        h  <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
        _  <- viewState.put(h, ResultSet.Read(rh)).liftM[FileSystemErrT]
      } yield h

    def openView(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] = {
      val readLP = addOffsetLimit(lpr.read(f), off, lim)

      def dataHandle(data: List[Data]): Free[S, ReadHandle] =
        for {
          h <- seq.next.map(ReadHandle(f, _))
          _ <- viewState.put(h, ResultSet.Data(data.toVector))
        } yield h

      def queryHandle(lp: Fix[LP]): FileSystemErrT[Free[S, ?], ReadHandle] =
        for {
          qh <- EitherT(queryUnsafe.eval(lp).run.value)
          h  <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
          _  <- viewState.put(h, ResultSet.Results(qh)).liftM[FileSystemErrT]
        } yield h

      for {
        lp <- resolveViewRefs[S](readLP).leftMap(se =>
                planningFailed(readLP, Planner.InternalError fromMsg se.shows))
        h  <- refineConstantPlan(lp).fold(dataHandle(_).liftM[FileSystemErrT], queryHandle)
      } yield h
    }

    λ[ReadFile ~> Free[S, ?]] {
      case Open(file, off, lim) =>
        mount.exists(file).ifM(
          openView(file, off, lim).run,
          openFile(file, off, lim).run)

      case Read(handle) =>
        viewState.get(handle).toRight(unknownReadHandle(handle)).flatMap {
          case ResultSet.Data(values) =>
            viewState.put(handle, ResultSet.Data(Vector.empty))
              .as(values)
              .liftM[FileSystemErrT]

          case ResultSet.Read(handle) =>
            readUnsafe.read(handle)

          case ResultSet.Results(handle) =>
            queryUnsafe.more(handle)
        }.run

      case Close(handle) =>
        (viewState.get(handle) <* viewState.delete(handle).liftM[OptionT]).flatMapF {
          case ResultSet.Data(_)         => ().point[Free[S, ?]]
          case ResultSet.Read(handle)    => readUnsafe.close(handle)
          case ResultSet.Results(handle) => queryUnsafe.close(handle)
        }.getOrElse(())
    }
  }

  /** Intercept and fail any write to a view path; all others are passed untouched. */
  def writeFile[S[_]](
    implicit
    S0: WriteFile :<: S,
    S1: Mounting :<: S
  ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    val writeUnsafe = WriteFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    λ[WriteFile ~> Free[S, ?]] {
      case Open(p) =>
        mount.exists(p).ifM(
          pathErr(invalidPath(p, "Cannot write to a view."))
            .left[WriteHandle].point[Free[S, ?]],
          writeUnsafe.open(p).run)

      case Write(h, chunk) =>
        writeUnsafe.write(h, chunk)

      case Close(h) =>
        writeUnsafe.close(h)
    }
  }

  /** Intercept and handle moves and deletes involving view path(s); all others are passed untouched. */
  def manageFile[S[_]](
    implicit
    S0: ManageFile :<: S,
    S1: QueryFile :<: S,
    S2: Mounting :<: S,
    S3: MountingFailure :<: S,
    S4: PathMismatchFailure :<: S
  ): ManageFile ~> Free[S, ?] = {
    import ManageFile._
    import MoveSemantics._

    val manage = ManageFile.Ops[S]
    val query = QueryFile.Ops[S]
    val mount = Mounting.Ops[S]
    val mntErr = Failure.Ops[MountingError, S]

    val fsErrorPath = pathErr composeLens errorPath
    val fsPathNotFound = pathErr composePrism pathNotFound

    def deleteView(loc: AFile): Free[S, Unit] =
      mntErr.attempt(mount.unmount(loc)).void

    def overwriteView(src: AFile, dst: AFile): Free[S, Unit] =
      deleteView(dst) *> mount.remount(src, dst)

    def dirToDirMove(src: ADir, dst: ADir, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] = {
      def moveAll(srcViews: Set[AFile]): manage.M[Unit] =
        if (srcViews.isEmpty)
          pathErr(pathNotFound(src)).raiseError[manage.M, Unit]
        else
          srcViews
            .flatMap(v => v.relativeTo(src).map(dst </> _).strengthL(v).toSet)
            .traverse_ { case (s, d) => fileToFileMove(s, d, semantics) }

      /** Abort if there is a fileSystem error related to the destination
        * directory to avoid surprising behavior where all the views in
        * a directory move while all the rest of the files stay put.
        *
        * We know that the 'src' directory exists in the fileSystem as
        * otherwise, the error would be src not found. We are happy to
        * ignore the latter as views can exist outside any filesystem so
        * there may be "directories" comprised of nothing but views.
        *
        * Otherwise, attempt to move the views, emitting any errors that may
        * occur in the process.
        */
      def onFileSystemError(views: Set[AFile], err: FileSystemError): Free[S, FileSystemError \/ Unit] =
        fsErrorPath.getOption(err).exists(_ === dst)
          .fold(err.left[Unit].point[Free[S, ?]], moveAll(views).run)

      /** Attempt to move views, but silence any 'src not found' errors since
        * this means there aren't any views to move, which isn't an error in this
        * case.
        */
      def onFileSystemSuccess(views: Set[AFile]): Free[S, FileSystemError \/ Unit] =
        moveAll(views).handleError(err =>
          fsPathNotFound.getOption(err).exists(_ === src)
            .fold(().point[manage.M], err.raiseError[manage.M, Unit]))
          .run

      mount.viewsHavingPrefix(src).flatMap { views =>
        manage.moveDir(src, dst, semantics).fold(
          onFileSystemError(views, _),
          κ(onFileSystemSuccess(views))
        ).join
      }
    }

    def fileToFileMove(src: AFile, dst: AFile, semantics: MoveSemantics): manage.M[Unit] =
      EitherT((mount.exists(src) |@| mount.exists(dst) |@| query.fileExists(dst)).tupled.flatMap {
        case (srcViewExists, dstViewExists, dstFileExists) => (semantics match {
          case FailIfExists if dstViewExists || dstFileExists =>
            pathErr(pathExists(dst)).raiseError[manage.M, Unit]

          case FailIfMissing if !(dstViewExists || dstFileExists) =>
            pathErr(pathNotFound(dst)).raiseError[manage.M, Unit]

          case _ if srcViewExists && dstFileExists =>
            // NB: We ignore the result of the filesystem delete as we're willing to
            //     shadow existing files if it fails for any reason.
            (manage.delete(dst).run *> overwriteView(src, dst)).liftM[FileSystemErrT]

          case _ if srcViewExists && !dstFileExists =>
            overwriteView(src, dst).liftM[FileSystemErrT]

          case _ =>
            manage.moveFile(src, dst, semantics)
        }).run
      })

    λ[ManageFile ~> Free[S, ?]] {
      case Move(scenario, semantics) =>
        scenario.fold(
          (src, dst) => dirToDirMove(src, dst, semantics),
          (src, dst) => fileToFileMove(src, dst, semantics).run)

      case Delete(path) =>
        refineType(path).fold(
          d => mount.viewsHavingPrefix(d)
                 .flatMap(_.traverse_(deleteView))
                 .liftM[FileSystemErrT] *> manage.delete(d),

          f => mount.exists(f).liftM[FileSystemErrT].ifM(
                 deleteView(f).liftM[FileSystemErrT],
                 manage.delete(f))
        ).run

      case TempFile(nearTo) =>
        manage.tempFile(nearTo).run
    }
  }

  /** Intercept and resolve queries involving views, and overlay views when
    * enumerating files and directories. */
  def queryFile[S[_]](
    implicit
    S0: QueryFile :<: S,
    S1: Mounting :<: S
  ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val mount = Mounting.Ops[S]
    import query.transforms.ExecM

    def resolve[A](lp: Fix[LP], op: Fix[LP] => ExecM[A]) =
      resolveViewRefs[S](lp).run.flatMap(_.fold(
        e => planningFailed(lp, Planner.InternalError fromMsg e.shows).raiseError[ExecM, A],
        p => op(p)).run.run)

    def listViews(dir: ADir): Free[S, Set[PathSegment]] =
      mount.viewsHavingPrefix(dir).map(_ foldMap { f =>
        f.relativeTo(dir).flatMap(firstSegmentName).toSet
      })

    λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        resolve(lp, query.execute(_, out))

      case EvaluatePlan(lp) =>
        resolve(lp, queryUnsafe.eval)

      case More(handle) =>
        queryUnsafe.more(handle).run

      case Close(handle) =>
        queryUnsafe.close(handle)

      case Explain(lp) =>
        resolve(lp, query.explain)

      case ListContents(dir) =>
        (listViews(dir) |@| query.ls(dir).run)((vls, qls) => qls match {
          case \/-(ps) =>
            (ps ++ vls).right
          case -\/(err @ PathErr(PathNotFound(_))) =>
            if (vls.nonEmpty) vls.right else err.left
          case -\/(v) =>
            v.left
        })

      case FileExists(file) =>
        mount.exists(file).ifM(
          true.point[Free[S, ?]],
          query.fileExists(file))
    }
  }

  /** Translates requests which refer to any view path into operations
    * on an underlying filesystem, where references to views have been
    * rewritten as queries against actual files.
    */
  def fileSystem[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S4: MonotonicSeq :<: S,
    S5: ViewState :<: S,
    S6: Mounting :<: S,
    S7: MountingFailure :<: S,
    S8: PathMismatchFailure :<: S
  ): FileSystem ~> Free[S, ?] =
    interpretFileSystem[Free[S, ?]](queryFile, readFile, writeFile, manageFile)

  /** Resolve view references in the given `LP`. */
  def resolveViewRefs[S[_]](plan: Fix[LP])(implicit M: Mounting.Ops[S])
    : SemanticErrsT[Free[S, ?], Fix[LP]] = {
    import MountConfig._

    def lift(e: Set[FPath], plan: Fix[LP]) =
      plan.project.map((e, _)).point[SemanticErrsT[Free[S, ?], ?]]

    def lookup(loc: AFile): OptionT[Free[S, ?], (Fix[Sql], Variables)] =
      OptionT(M.lookupConfig(loc).run.map(_.flatMap(viewConfig.getOption)))

    def compiledView(loc: AFile): OptionT[Free[S, ?], SemanticErrors \/ Fix[LP]] =
      lookup(loc).map { case (expr, vars) =>
         precompile[Fix[LP]](Blob(expr, Nil), vars, fileParent(loc)).run.value
      }

    // NB: simplify incoming queries to the raw, idealized LP which is simpler
    //     to manage.
    val cleaned = plan.cata(optimizer.elideTypeCheckƒ)

    (Set[FPath](), cleaned).anaM[Fix[LP]] {
      case (e, i @ Embed(lp.Read(p))) if !(e contains p) =>
        refineTypeAbs(p).swap.map(f =>
          EitherT(compiledView(f) getOrElse i.right).map(_.project.map((e + f, _)))
        ).getOrElse(lift(e, i))

      case (e, i) => lift(e, i)
    } flatMap (resolved => EitherT(preparePlan(resolved).run.value.point[Free[S, ?]]))
  }
}
