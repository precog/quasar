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
import quasar.fs._, FileSystemError._, PathError2._
import quasar.std.StdLib._, set._

import matryoshka._
import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

object view {
  /** Translate reads on view paths to the equivalent queries. */
  def readFile[S[_]: Functor]
      (views: Views)
      (implicit
        S0: ReadFileF :<: S,
        S1: QueryFileF :<: S,
        S2: MonotonicSeqF :<: S,
        S3: ViewStateF :<: S
      ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val seq = MonotonicSeq.Ops[S]
    val viewState = ViewState.Ops[S]

    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]): Free[S, A] = rf match {
        case Open(path, off, lim) =>
          views.lookup(path).cata(
            { lp =>
              for {
                qh <- EitherT(queryUnsafe.eval(limit(lp, off, lim)).run.value)
                h  <- seq.next.map(ReadHandle(path, _)).liftM[FileSystemErrT]
                _  <- viewState.put(h, \/-(qh)).liftM[FileSystemErrT]
              } yield h
            },
            for {
              rh <- readUnsafe.open(path, off, lim)
              h  <- seq.next.map(ReadHandle(path, _)).liftM[FileSystemErrT]
              _  <- viewState.put(h, -\/(rh)).liftM[FileSystemErrT]
            } yield h).run

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
      (views: Views)
      (implicit
        S0: WriteFileF :<: S
      ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    val writeUnsafe = WriteFile.Unsafe[S]

    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]): Free[S, A] = wf match {
        case Open(p) =>
          if (views.contains(p))
            emit[S,A](-\/(pathError(invalidPath(p, "cannot write to view"))))
          else
            writeUnsafe.open(p).run

        case Write(h, chunk) =>
          writeUnsafe.write(h, chunk)

        case Close(h) =>
          writeUnsafe.close(h)
      }
    }
  }


  /** Intercept and fail any write to a view path; all others are passed untouched. */
  def manageFile[S[_]: Functor]
      (views: Views)
      (implicit
        S0: ManageFileF :<: S
      ): ManageFile ~> Free[S, ?] = {
    import ManageFile._

    val manage = ManageFile.Ops[S]

    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scenario, semantics) =>
          MoveScenario.fileToFile.getOption(scenario).flatMap { case (src, dst) =>
            (for {
              _ <- if (views.contains(src)) -\/(pathError(invalidPath(src, "cannot move view"))) else \/-(())
              _ <- if (views.contains(dst)) -\/(pathError(invalidPath(dst, "cannot move file to view location"))) else \/-(())
            } yield ()).swap.toOption
          }.fold(manage.move(scenario, semantics).run)(err => emit[S,A](-\/(err)))

        case Delete(path) =>
          if (maybeFile(path).map(views.contains(_)).getOrElse(false))
            emit[S,A](-\/(pathError(invalidPath(path, "cannot delete view"))))
          else
            manage.delete(path).run

        case TempFile(nearTo) =>
          manage.tempFile(nearTo).run
      }
    }
  }


  /** Intercept and rewrite queries involving views, and overlay views when
    * enumerating files and directories. */
  def queryFile[S[_]: Functor]
      (views: Views)
      (implicit
        S0: QueryFileF :<: S
      ): QueryFile ~> Free[S, ?] = {
    import QueryFile._

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]

    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          query.execute(views.rewrite(lp), out).run.run

        case EvaluatePlan(lp) =>
          queryUnsafe.eval(views.rewrite(lp)).run.run

        case More(handle) =>
          queryUnsafe.more(handle).run

        case Close(handle) =>
          queryUnsafe.close(handle)

        case Explain(lp) =>
          query.explain(views.rewrite(lp)).run.run

        case ListContents(dir) =>
          query.ls(dir).run.map(_ match {
            case  \/-(ps) =>
              (ps ++ views.ls(dir)).right
            case -\/(err @ PathError(PathNotFound(_))) =>
              val vs = views.ls(dir)
              if (vs.nonEmpty) vs.right
              else err.left
            case -\/(v) => v.left
          })

        case FileExists(file) =>
          if (views.contains(file))
            true.point[Free[S, ?]]
          else
            query.fileExists(file)
      }
    }
  }

  /** Translates requests which refer to any view path into operations
    * on an underlying filesystem, where references to views have been
    * rewritten as queries against actual files.
    */
  def fileSystem[S[_]: Functor]
      (views: Views)
      (implicit
        S0: ReadFileF :<: S,
        S1: WriteFileF :<: S,
        S2: ManageFileF :<: S,
        S3: QueryFileF :<: S,
        S4: MonotonicSeqF :<: S,
        S5: ViewStateF :<: S
      ): FileSystem ~> Free[S, ?] = {
    interpretFileSystem[Free[S, ?]](
      queryFile(views),
      readFile(views),
      writeFile(views),
      manageFile(views))
  }


  // NB: wrapping this in a function seems to help the type checker
  // with the narrowed `A` type.
  private def emit[S[_]: Functor, A](a: A): Free[S, A] = a.point[Free[S, ?]]
}
