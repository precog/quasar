/*
 * Copyright 2014–2018 SlamData Inc.
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
import quasar._, RenderTree.ops._, RenderTreeT.ops._
import quasar.common.{PhaseResults, PhaseResultT, PhaseResultW}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.frontend.SemanticErrsT
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.{Transform => _, _}
import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz.{ToIdOps => _, _}
import scalaz.stream.{Process0, Process}

sealed abstract class QueryFile[A]

object QueryFile {
  final case class ResultHandle(run: Long) extends scala.AnyVal

  object ResultHandle {
    implicit val show: Show[ResultHandle] = Show.showFromToString

    implicit val order: Order[ResultHandle] = Order.orderBy(_.run)
  }

  /** The result of the query is stored in an output file, overwriting any existing
    * contents, instead of being returned to the user immediately.
    *
    * The `LogicalPlan` is expected to only contain absolute paths even though
    * that is unfortunately not expressed in the types currently.
    */
  final case class ExecutePlan(lp: Fix[LogicalPlan], out: AFile)
    extends QueryFile[(PhaseResults, FileSystemError \/ Unit)]

  /** The result of the query is immediately
    * streamed back to the client. This operation begins the streaming, in order
    * to continue the streaming, the client must make use of the `More` operation and
    * finally the `Close` operation in order to halt the streaming.
    * The `LogicalPlan` is expected to only contain absolute paths even though
    * that is unfortunately not expressed in the types currently.
    */
  final case class EvaluatePlan(lp: Fix[LogicalPlan])
    extends QueryFile[(PhaseResults, FileSystemError \/ ResultHandle)]

  /** Used to continue streaming after initiating a streaming
    * result with the `EvaluatePlan` operation.
    */
  final case class More(h: ResultHandle)
    extends QueryFile[FileSystemError \/ Vector[Data]]

  /** Used to halt streaming of a result set initiated using
    * the `EvaluatePlan` operation.
    */
  final case class Close(h: ResultHandle)
    extends QueryFile[Unit]

  /** Represents an "explain plan" operation. This operation should not actually
    * have any side effect on the filesystem, it should simply return useful
    * information to the user about how a given query would be evaluated on
    * this filesystem implementation.
    * The [[quasar.LogicalPlan]] is expected to only contain absolute paths even
    * though that is unfortunately not expressed in the types currently.
    */
  final case class Explain(lp: Fix[LogicalPlan])
    extends QueryFile[(PhaseResults, FileSystemError \/ ExecutionPlan)]

  /** This operation lists the names of all the immediate children of the supplied directory
    * in the filesystem.
    */
    /* TODO: While this is a bit better in one dimension here in `QueryFile`,
    *       `@mossprescott` points out it is still a bit of a stretch to include
    *       in this algebra. We need to revisit this and probably add algebras
    *       over multiple dimensions to better organize these (and other)
    *       operations.
    *
    *       For more discussion, see
    *       https://github.com/quasar-analytics/quasar/pull/986#discussion-diff-45081757
    */
  final case class ListContents(dir: ADir)
    extends QueryFile[FileSystemError \/ Set[Node]]

  /** This operation should return whether a file exists in the filesystem.*/
  final case class FileExists(file: AFile)
    extends QueryFile[Boolean]

  final class Ops[S[_]](implicit S: QueryFile :<: S)
    extends LiftedOps[QueryFile, S] {

    type M[A] = FileSystemErrT[FreeS, A]

    val unsafe = Unsafe[S]
    val transforms = Transforms[FreeS]
    import transforms._

    /** Returns the path to the result of executing the given `LogicalPlan`,
      * using the provided path.
      *
      * If the given file path exists, it will be overwritten with the results
      * from the query.
      */
    def execute(plan: Fix[LogicalPlan], out: AFile): ExecM[Unit] =
      EitherT(WriterT(lift(ExecutePlan(plan, out))): G[FileSystemError \/ Unit])

    /** Returns the stream of data resulting from evaluating the given
      * `LogicalPlan`.
      */
    def evaluate(plan: Fix[LogicalPlan]): Process[ExecM, Data] = {
      // TODO: use DataCursor.process for the appropriate cursor type
      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def moreUntilEmpty(h: ResultHandle): Process[M, Data] =
        Process.await(unsafe.more(h): M[Vector[Data]]) { data =>
          if (data.isEmpty)
            Process.halt
          else
            Process.emitAll(data) ++ moreUntilEmpty(h)
        }

      def close(h: ResultHandle): ExecM[Unit] =
        toExec(unsafe.close(h))

      Process.bracket(unsafe.eval(plan))(h => Process.eval_(close(h))) { h =>
        moreUntilEmpty(h).translate(hoistToExec)
      }
    }

    def first(plan: Fix[LogicalPlan]): ExecM[Option[Data]] = for {
      h  <- unsafe.eval(plan)
      vs <- hoistToExec(unsafe.more(h))
      _  <- toExec(unsafe.close(h))
    } yield vs.headOption

    /** Returns a stream of data resulting from evaluating the given
      * `LogicalPlan`.
      *
      * This consumes the entire result, if you need control over how much
      * data is consumed, see `evaluate`.
      */
    def results(plan: Fix[LogicalPlan]): ExecM[Process0[Data]] = {
      def close(h: ResultHandle): ExecM[Unit] =
        toExec(unsafe.close(h))

      def next(h: ResultHandle): ExecM[Option[(Vector[Data], ResultHandle)]] =
        hoistToExec(unsafe.more(h))
          .ensuring(_.isDefined whenM close(h))
          .map(xs => xs.nonEmpty.option((xs, h)))

      unsafe.eval(plan)
        .flatMap(h => StreamT.unfoldM(h)(next).toStream <* close(h))
        .map(cs => Process.emitAll(cs) flatMap (Process.emitAll(_)))
    }

    /** Returns a description of how the the given logical plan will be
      * executed.
      */
    def explain(plan: Fix[LogicalPlan]): ExecM[ExecutionPlan] =
      EitherT(WriterT(lift(Explain(plan))): G[FileSystemError \/ ExecutionPlan])

    /** Returns the names of the immediate children of the given directory,
      * fails if the directory does not exist.
      */
    def ls(dir: ADir): M[Set[Node]] =
      listContents(dir)

    /** Returns a Map of all files in this directory and all of it's sub-directories
      * along with its `Node.Type`.
      * Fails if the directory does not exist.
      */
    def descendantFiles(dir: ADir): M[Map[RFile, Node.Type]] = {
      type S[A] = StreamT[M, A]

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def lsR(current1: RDir): StreamT[M, (RFile, Node.Type)] =
        StreamT.fromStream[M, Node](ls(dir </> current1) map (_.toStream)) flatMap {
          case f: FileNode => ((current1 </> file1(f.name)) -> f.`type`).point[S]
          case d: DirNode  => lsR(current1 </> dir1(d.name))
        }

      lsR(currentDir).foldLeft(Set.empty[(RFile, Node.Type)])(_ + _).map(_.toMap)
    }

    def listContents(dir: ADir): M[Set[Node]] =
      EitherT(lift(ListContents(dir)))

    /** Returns whether the given file exists. */
    def fileExists(file: AFile): FreeS[Boolean] =
      lift(FileExists(file))

    /** Returns whether the given file exists, lifted into the same monad as
      * the rest of the functions here, for convenience.
      */
    def fileExistsM(file: AFile): M[Boolean] =
      fileExists(file).liftM[FileSystemErrT]

    ////

    private val hoistToExec: M ~> ExecM =
      Hoist[FileSystemErrT].hoist[FreeS, G](liftMT[FreeS, PhaseResultT])
  }

  object Ops {
    implicit def apply[S[_]](implicit S: QueryFile :<: S): Ops[S] =
      new Ops[S]
  }

  /** Low-level, unsafe operations. Clients are responsible for resource-safety
    * when using these.
    */
  final class Unsafe[S[_]](implicit S: QueryFile :<: S)
    extends LiftedOps[QueryFile, S] {

    val transforms = Transforms[FreeS]
    import transforms._

    /** Returns a handle to the results of evaluating the given `LogicalPlan`
      * that can be used to read chunks of result data.
      *
      * Care must be taken to `close` the returned handle in order to avoid
      * potential resource leaks.
      */
    def eval(lp: Fix[LogicalPlan]): ExecM[ResultHandle] =
      EitherT(WriterT(lift(EvaluatePlan(lp))): G[FileSystemError \/ ResultHandle])

    /** Read the next chunk of data from the result set represented by the given
      * handle.
      *
      * An empty `Vector` signals that all data has been read.
      */
    def more(rh: ResultHandle): FileSystemErrT[FreeS, Vector[Data]] =
      EitherT(lift(More(rh)))

    /** Closes the given result handle, freeing any resources it was using. */
    def close(rh: ResultHandle): FreeS[Unit] =
      lift(Close(rh))
  }

  object Unsafe {
    implicit def apply[S[_]](implicit S: QueryFile :<: S): Unsafe[S] =
      new Unsafe[S]
  }

  class Transforms[F[_]: Monad] {
    type G[A] = PhaseResultT[F, A]
    type H[A] = SemanticErrsT[G, A]

    type ExecM[A]     = FileSystemErrT[G, A]
    type CompExecM[A] = FileSystemErrT[H, A]

    val execToCompExec: ExecM ~> CompExecM =
      Hoist[FileSystemErrT].hoist[G, H](liftMT[G, SemanticErrsT])

    val compToCompExec: CompileM ~> CompExecM = {
      val hoistW: PhaseResultW ~> G = Hoist[PhaseResultT].hoist(pointNT[F])
      val hoistC: CompileM ~> H     = Hoist[SemanticErrsT].hoist(hoistW)
      liftMT[H, FileSystemErrT] compose hoistC
    }

    val toExec: F ~> ExecM =
      liftMT[G, FileSystemErrT] compose liftMT[F, PhaseResultT]

    def fsErrToExec: FileSystemErrT[F, ?] ~> ExecM =
      Hoist[FileSystemErrT].hoist[F, PhaseResultT[F, ?]](liftMT[F, PhaseResultT])

    val toCompExec: F ~> CompExecM =
      execToCompExec compose toExec

    val dropPhases: ExecM ~> FileSystemErrT[F, ?] =
      Hoist[FileSystemErrT].hoist(λ[PhaseResultT[F, ?] ~> F](_.value))
  }

  object Transforms {
    def apply[F[_]: Monad]: Transforms[F] =
      new Transforms[F]
  }

  implicit def renderTree[A]: RenderTree[QueryFile[A]] =
    new RenderTree[QueryFile[A]] {
      def render(qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) => NonTerminal(List("ExecutePlan"), None, List(lp.render, out.render))
        case EvaluatePlan(lp)     => NonTerminal(List("EvaluatePlan"), None, List(lp.render))
        case More(handle)         => Terminal(List("More"), handle.shows.some)
        case Close(handle)        => Terminal(List("Close"), handle.shows.some)
        case Explain(lp)          => NonTerminal(List("Explain"), None, List(lp.render))
        case ListContents(dir)    => NonTerminal(List("ListContents"), None, List(dir.render))
        case FileExists(file)     => NonTerminal(List("FileExists"), None, List(file.render))
      }
    }
}
