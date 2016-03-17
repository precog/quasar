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
import quasar._
import quasar.effect._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._, FileSystemError._, PathError._
import quasar.std.StdLib._, set._
import quasar.sql.Expr

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

object view {
  /** Translate reads on view paths to the equivalent queries. */
  def readFile[S[_]: Functor]
      (implicit
        S0: ReadFileF :<: S,
        S1: QueryFileF :<: S,
        S2: MonotonicSeqF :<: S,
        S3: ViewStateF :<: S,
        S4: MountConfigsF :<: S
      ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val seq = MonotonicSeq.Ops[S]
    val viewState = ViewState.Ops[S]

    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]): Free[S, A] = rf match {
        case Open(path, off, lim) =>
          val fOpen: Free[S, FileSystemError \/ ReadHandle] = {
            for {
              rh <- readUnsafe.open(path, off, lim)
              h  <- seq.next.map(ReadHandle(path, _)).liftM[FileSystemErrT]
              _  <- viewState.put(h, -\/(rh)).liftM[FileSystemErrT]
            } yield h
          }.run

          def vOpen(e: Expr, v: Variables): Free[S, FileSystemError \/ ReadHandle] = {
            queryPlan(e, v).run.value.fold[EitherT[queryUnsafe.F, FileSystemError, ReadHandle]](
              e => EitherT[Free[S, ?], FileSystemError, ReadHandle](
                // TODO: more sensible error?
                Free.point(pathErr(invalidPath(path, e.shows)).left[ReadHandle])),
              lp =>
                for {
                  qh <- EitherT(queryUnsafe.eval(limit(lp, off, lim)).run.value)
                  h  <- seq.next.map(ReadHandle(path, _)).liftM[FileSystemErrT]
                  _  <- viewState.put(h, \/-(qh)).liftM[FileSystemErrT]
                } yield h)
          }.run

          ViewMounter.lookup[S](path).run.flatMap(_.cata((vOpen _).tupled, fOpen))

        case Read(handle) =>
          (for {
            v <- viewState.get(handle).toRight(unknownReadHandle(handle))
            d <- v.fold(readUnsafe.read, queryUnsafe.more)
          } yield d).run

        case Close(handle) =>
          (for {
            v <- viewState.get(handle)
            _ <- viewState.delete(handle).liftM[OptionT]
            _ <- v.fold(readUnsafe.close, queryUnsafe.close).liftM[OptionT]
          } yield ()).getOrElse(())
      }
    }
  }

  def limit(lp: Fix[LogicalPlan], off: Natural, lim: Option[Positive]): Fix[LogicalPlan] = {
    val skipped = if (off.get != 0L) Fix(Drop(lp, LogicalPlan.Constant(Data.Int(off.get)))) else lp
    val limited = lim.fold(skipped)(l => Fix(Take(skipped, LogicalPlan.Constant(Data.Int(l.get)))))
    limited
  }


  /** Intercept and fail any write to a view path; all others are passed untouched. */
  def writeFile[S[_]: Functor]
      (implicit
        S0: WriteFileF :<: S,
        S1: MountConfigsF :<: S
      ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    val writeUnsafe = WriteFile.Unsafe[S]

    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]): Free[S, A] = wf match {
        case Open(p) =>
          ViewMounter.lookup[S](p).run.flatMap(_.cata(
            κ(emit[S, A](-\/(pathErr(invalidPath(p, "cannot write to view"))))),
            writeUnsafe.open(p).run))

        case Write(h, chunk) =>
          writeUnsafe.write(h, chunk)

        case Close(h) =>
          writeUnsafe.close(h)
      }
    }
  }


  /** Intercept and handle moves and deletes involving view path(s); all others are passed untouched. */
  def manageFile[S[_]: Functor]
      (implicit
        S0: ManageFileF :<: S,
        S1: QueryFileF :<: S,
        S2: MountConfigsF :<: S
      ): ManageFile ~> Free[S, ?] = {
    import ManageFile._
    import MoveSemantics._

    val manage = ManageFile.Ops[S]
    val query = QueryFile.Ops[S]

    def dirToDirMove(src: ADir, dst: ADir, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] = {
      implicit val m = EitherT.eitherTMonad[Free[S, ?], FileSystemError]

      def move(pathSegments: Set[PathSegment]): Free[S, FileSystemError \/ Unit] =
        pathSegments
          .toList
          .traverse_[EitherT[Free[S, ?], FileSystemError, ?]] { s =>
            EitherT[Free[S, ?], FileSystemError, Unit](s.fold(
              d => dirToDirMove(src </> dir1(d), dst </> dir1(d), semantics),
              f => fileToFileMove(src </> file1(f), dst </> file1(f), semantics)))
          }.run

      ViewMounter.ls[S](src).flatMap { ps =>
        if (ps.isEmpty)
          manage.moveDir(src, dst, semantics).run
        else
          manage.moveDir(src, dst, semantics).run *> move(ps)
      }
    }

    def fileToFileMove(src: AFile, dst: AFile, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] = {
      val move = manage.moveFile(src, dst, semantics).run

      (
        ViewMounter.lookup[S](src).run.map(_.isDefined) |@|
        ViewMounter.lookup[S](dst).run.map(_.isDefined) |@|
        query.fileExists(dst)
      ) .tupled
        .flatMap { case (srcViewExists, dstViewExists, dstFileExists) => semantics match {
          case FailIfExists if dstViewExists || dstFileExists =>
            emit[S, FileSystemError \/ Unit](-\/(pathErr(pathExists(dst))))
          case FailIfMissing if !(dstViewExists || dstFileExists) =>
            emit[S, FileSystemError \/ Unit](-\/(pathErr(pathNotFound(dst))))
          case _ if srcViewExists && dstFileExists =>
            manage.delete(dst).run *> ViewMounter.move[S](src, dst).map(_.right[FileSystemError])
          case _ if srcViewExists && !dstFileExists =>
            ViewMounter.move[S](src, dst).map(_.right[FileSystemError])
          case _ =>
            move
        }}
    }

    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scenario, semantics) =>
          scenario.fold(
            (src, dst) =>  dirToDirMove(src, dst, semantics),
            (src, dst) => fileToFileMove(src, dst, semantics))

        case Delete(path) =>
          val delete = manage.delete(path).run
          ViewMounter.viewPaths[S].flatMap { viewPaths =>
            refineType(path).fold(
              d => viewPaths
                .foldMap(f => f.relativeTo(d).map(κ(f)).toList)
                .foldMap(ViewMounter.unmount[S])
                *> delete,
              f => viewPaths.contains(f) ? ViewMounter.unmount[S](f).map(_.right[FileSystemError]) | delete)
          }

        case TempFile(nearTo) =>
          manage.tempFile(nearTo).run
      }
    }
  }


  /** Intercept and rewrite queries involving views, and overlay views when
    * enumerating files and directories. */
  def queryFile[S[_]: Functor]
      (implicit
        S0: QueryFileF :<: S,
        S1: MountConfigsF :<: S
      ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]

    def rewrite[A](lp: Fix[LogicalPlan], op: Fix[LogicalPlan] => query.transforms.ExecM[A]) =
      ViewMounter.rewrite[S](lp).run.flatMap(_.fold[Free[S, (PhaseResults, FileSystemError \/ A)]](
        e => (
            Vector.empty[PhaseResult],
            planningFailed(lp, Planner.InternalError(e.shows)).left[A]
          ).point[Free[S, ?]],
        p => op(p).run.run))

    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          rewrite(lp, query.execute(_, out))

        case EvaluatePlan(lp) =>
          rewrite(lp, queryUnsafe.eval)

        case More(handle) =>
          queryUnsafe.more(handle).run

        case Close(handle) =>
          queryUnsafe.close(handle)

        case Explain(lp) =>
          rewrite(lp, query.explain)

        case ListContents(dir) =>
          (ViewMounter.ls[S](dir) |@| query.ls(dir).run) { (vls, qls) => qls match {
            case  \/-(ps) =>
              (ps ++ vls).right
            case -\/(err @ PathErr(PathNotFound(_))) =>
              if (vls.nonEmpty) vls.right
              else err.left
            case -\/(v) => v.left
          }}

        case FileExists(file) =>
          ViewMounter.lookup[S](file).run.flatMap(mc =>
            if(mc.isDefined) true.point[Free[S, ?]] else query.fileExists(file))
      }
    }
  }

  /** Translates requests which refer to any view path into operations
    * on an underlying filesystem, where references to views have been
    * rewritten as queries against actual files.
    */
  def fileSystem[S[_]: Functor]
      (implicit
        S0: ReadFileF :<: S,
        S1: WriteFileF :<: S,
        S2: ManageFileF :<: S,
        S3: QueryFileF :<: S,
        S4: MonotonicSeqF :<: S,
        S5: ViewStateF :<: S,
        S6: MountConfigsF :<: S
      ): FileSystem ~> Free[S, ?] = {
    interpretFileSystem[Free[S, ?]](
      queryFile,
      readFile,
      writeFile,
      manageFile)
  }


  // NB: wrapping this in a function seems to help the type checker
  // with the narrowed `A` type.
  private def emit[S[_]: Functor, A](a: A): Free[S, A] = a.point[Free[S, ?]]
}
