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
import quasar._, Planner._, RenderTree.ops._, RenderTreeT.ops._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT, PhaseResultW}
import quasar.connector.CompileM
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fp.ski._
import quasar.frontend.SemanticErrsT
import quasar.frontend.logicalplan.{LogicalPlan, Optimizer}
import quasar.qscript._

import matryoshka.{Transform => _, _}
import matryoshka.data.Fix
import matryoshka.implicits._
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

  def convertAndNormalize
    [T[_[_]]: BirecursiveT: EqualT: ShowT, QS[_]: Traverse: Normalizable]
    (lp: T[LogicalPlan])
    (eval: QS[T[QS]] => QS[T[QS]])
    (implicit
      CQ: Coalesce.Aux[T, QS, QS],
      DE:    Const[DeadEnd, ?] :<: QS,
      QC:    QScriptCore[T, ?] :<: QS,
      TJ:      ThetaJoin[T, ?] :<: QS,
      PB:  ProjectBucket[T, ?] :<: QS,
      FI: Injectable.Aux[QS, QScriptTotal[T, ?]],
      mergeable: Mergeable.Aux[T, QS],
      render: Delay[RenderTree, QS],
      eq: Delay[Equal, QS],
      show: Delay[Show, QS])
      : PlannerError \/ T[QS] = {
    val transform = new Transform[T, QS]
    val optimizer = new Optimizer[T[LogicalPlan]]

    // TODO: Instead of eliding Lets, use a `Binder` fold, or ABTs or something
    //       so we don’t duplicate work.
    lp.transCata[T[LogicalPlan]](orOriginal(optimizer.elideLets))
      .cataM[PlannerError \/ ?, Target[T, QS]](newLP => transform.lpToQScript(newLP.map(Target.value.modify(_.transAna[T[QS]](eval)))))
      .map(target => QC.inj((transform.reifyResult(target.ann, target.value))).embed.transCata[T[QS]](eval))
  }

  def simplifyAndNormalize
    [T[_[_]]: BirecursiveT: EqualT: ShowT,
      IQS[_]: Functor,
      QS[_]: Traverse: Normalizable]
    (implicit
      CI: Coalesce.Aux[T, IQS, IQS],
      CQ: Coalesce.Aux[T, QS, QS],
      SP: SimplifyProjection.Aux[IQS, QS],
      PA: PruneArrays[QS],
      QC: QScriptCore[T, ?] :<: QS,
      TJ:   ThetaJoin[T, ?] :<: QS,
      FI: Injectable.Aux[QS, QScriptTotal[T, ?]])
      : T[IQS] => T[QS] = {
    val rewrite = new Rewrite[T]

    // TODO: This would be `transHylo` if there were such a thing.
    _.transAna[T[QS]](SP.simplifyProjection)
      // TODO: Rather than explicitly applying multiple times, we should apply
      //       repeatedly until unchanged.
      .transAna[T[QS]](rewrite.normalize)
      .transAna[T[QS]](rewrite.normalize)
      .pruneArraysF
  }

  /** The shape of QScript that’s used during conversion from LP. */
  private type QScriptInternal[T[_[_]], A] =
    (QScriptCore[T, ?] :\: ProjectBucket[T, ?] :\: ThetaJoin[T, ?] :/: Const[DeadEnd, ?])#M[A]

  implicit def qScriptInternalToQscriptTotal[T[_[_]]]
      : Injectable.Aux[QScriptInternal[T, ?], QScriptTotal[T, ?]] =
    Injectable.coproduct(Injectable.inject[QScriptCore[T, ?], QScriptTotal[T, ?]],
      Injectable.coproduct(Injectable.inject[ProjectBucket[T, ?], QScriptTotal[T, ?]],
        Injectable.coproduct(Injectable.inject[ThetaJoin[T, ?], QScriptTotal[T, ?]],
          Injectable.inject[Const[DeadEnd, ?], QScriptTotal[T, ?]])))

  /** This is a stop-gap function that QScript-based backends should use until
    * LogicalPlan no longer needs to be exposed.
    */
  def convertToQScript
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT, QS[_]: Traverse: Normalizable]
    (lp: T[LogicalPlan])
    (implicit
      CQ: Coalesce.Aux[T, QS, QS],
      PA: PruneArrays[QS],
      DE:  Const[DeadEnd, ?] :<: QS,
      QC:  QScriptCore[T, ?] :<: QS,
      TJ:    ThetaJoin[T, ?] :<: QS,
      FI: Injectable.Aux[QS, QScriptTotal[T, ?]],
      show: Delay[Show, QS],
      renderI: Delay[RenderTree, QScriptInternal[T, ?]],
      render: Delay[RenderTree, QS])
      : EitherT[Writer[PhaseResults, ?], FileSystemError, T[QS]] = {
    val transform = new Transform[T, QScriptInternal[T, ?]]
    val rewrite = new Rewrite[T]

    val qs =
      convertAndNormalize[T, QScriptInternal[T, ?]](lp)(rewrite.normalize)
        .leftMap(FileSystemError.planningFailed(lp.convertTo[Fix[LogicalPlan]], _)) ∘
        simplifyAndNormalize[T, QScriptInternal[T, ?], QS]

    EitherT(Writer(
      qs.fold(κ(Vector()), a => Vector(PhaseResult.tree("QScript", a))),
      qs))
  }

  def convertToQScriptRead
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT, M[_]: Monad, QS[_]: Traverse: Normalizable]
    (listContents: DiscoverPath.ListContents[M])
    (lp: T[LogicalPlan])
    (implicit
      merr: MonadError_[M, FileSystemError],
      mtell: MonadTell_[M, PhaseResults],
      RD: Const[Read[ADir], ?]  :<: QS,
      RF: Const[Read[AFile], ?] :<: QS,
      QC:    QScriptCore[T, ?]  :<: QS,
      TJ:      ThetaJoin[T, ?]  :<: QS,
      CQ: Coalesce.Aux[T, QS, QS],
      PA: PruneArrays[QS],
      FI: Injectable.Aux[QS, QScriptTotal[T, ?]],
      show: Delay[Show, QS],
      renderI: Delay[RenderTree, QScriptInternal[T, ?]],
      render: Delay[RenderTree, QS])
      : M[T[QS]] = {
    val transform = new Transform[T, QScriptInternal[T, ?]]
    val rewrite = new Rewrite[T]

    type InterimQS[A] =
      (QScriptCore[T, ?] :\: ProjectBucket[T, ?] :\: ThetaJoin[T, ?] :\: Const[Read[ADir], ?] :/: Const[Read[AFile], ?])#M[A]

    implicit val interimQsToQscriptTotal
        : Injectable.Aux[InterimQS, QScriptTotal[T, ?]] =
      Injectable.coproduct(Injectable.inject[QScriptCore[T, ?], QScriptTotal[T, ?]],
        Injectable.coproduct(Injectable.inject[ProjectBucket[T, ?], QScriptTotal[T, ?]],
          Injectable.coproduct(Injectable.inject[ThetaJoin[T, ?], QScriptTotal[T, ?]],
            Injectable.coproduct(Injectable.inject[Const[Read[ADir], ?], QScriptTotal[T, ?]],
              Injectable.inject[Const[Read[AFile], ?], QScriptTotal[T, ?]]))))

    convertAndNormalize[T, QScriptInternal[T, ?]](lp)(rewrite.normalize)
      .fold(
        perr => merr.raiseError(FileSystemError.planningFailed(lp.convertTo[Fix[LogicalPlan]], perr)),
        _.point[M])
      .flatMap(rewrite.pathify[M, QScriptInternal[T, ?], InterimQS](listContents))
      .map(simplifyAndNormalize[T, InterimQS, QS])
      .flatMap(qs => mtell.writer(Vector(PhaseResult.tree("QScript", qs)), qs))
  }

  /** The result of the query is stored in an output file, overwriting any existing
    * contents, instead of being returned to the user immediately.
    *
    * The `LogicalPlan` is expected to only contain absolute paths even though
    * that is unfortunately not expressed in the types currently.
    */
  final case class ExecutePlan(lp: Fix[LogicalPlan], out: AFile)
    extends QueryFile[(PhaseResults, FileSystemError \/ AFile)]

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
    extends QueryFile[FileSystemError \/ Set[PathSegment]]

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
      * using the provided path if possible.
      *
      * If the given file path exists, it will be overwritten with the results
      * from the query.
      *
      * Execution of certain plans may return a result file other than the
      * requested file if it is more efficient to do so (i.e. to avoid copying
      * lots of data for a plan consisting of a single `Read(...)`).
      */
    def execute(plan: Fix[LogicalPlan], out: AFile): ExecM[AFile] =
      EitherT(WriterT(lift(ExecutePlan(plan, out))): G[FileSystemError \/ AFile])

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
    def ls(dir: ADir): M[Set[PathSegment]] =
      EitherT(lift(ListContents(dir)))

    /** Returns all files in this directory and all of it's sub-directories
      * Fails if the directory does not exist.
      */
    def descendantFiles(dir: ADir): M[Set[RFile]] = {
      type S[A] = StreamT[M, A]

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def lsR(desc: RDir): StreamT[M, RFile] =
        StreamT.fromStream[M, PathSegment](ls(dir </> desc) map (_.toStream))
          .flatMap(_.fold(
            d => lsR(desc </> dir1(d)),
            f => (desc </> file1(f)).point[S]))

      lsR(currentDir).foldLeft(Set.empty[RFile])(_ + _)
    }

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
