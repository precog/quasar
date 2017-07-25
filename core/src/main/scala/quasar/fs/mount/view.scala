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
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.frontend.{SemanticErrors, SemanticErrsT}
import quasar.fs._, FileSystemError._, PathError._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, Optimizer}

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
        lp <- resolveViewRefs[S](readLP)
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
    val mount = Mounting.Ops[S]
    nonFsMounts.failSomeWrites(
      on = file => mount.lookupType(file).run.run.map(_.filter(_ ≟ MountType.ViewMount.right).isDefined),
      message = "Cannot write to a view.")
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
        e => e.raiseError[ExecM, A],
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

  def analyze[S[_]](implicit
    M: Mounting.Ops[S],
    A: Analyze.Ops[S]
  ): Analyze ~> Free[S, ?] = new (Analyze ~> Free[S, ?]) {
    def apply[A](from: Analyze[A]) = from match {
      case Analyze.QueryCost(lp) => resolveViewRefs[S](lp).run.flatMap(_.fold(
        e => planningFailed(lp, Planner.InternalError fromMsg e.shows).raiseError[FileSystemErrT[Free[S, ?], ?], Int],
        p => A.queryCost(p)).run)

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
  ): FileSystem ~> Free[S, ?] = {
    val mount = Mounting.Ops[S]
    val manageFile = nonFsMounts.manageFile(dir => mount.viewsHavingPrefix_(dir).map(paths => paths.map(p => (p:RPath))))
    interpretFileSystem[Free[S, ?]](queryFile, readFile, writeFile, manageFile)
  }

  def backendEffect[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: WriteFile :<: S,
    S2: ManageFile :<: S,
    S3: QueryFile :<: S,
    S4: MonotonicSeq :<: S,
    S5: ViewState :<: S,
    S6: Mounting :<: S,
    S7: MountingFailure :<: S,
    S8: PathMismatchFailure :<: S,
    S9: Analyze :<: S
  ): BackendEffect ~> Free[S, ?] = analyze :+: fileSystem[S]

  /** Resolve view references in the given `LP`. */
  def resolveViewRefs[S[_]](plan: Fix[LP])(implicit M: Mounting.Ops[S])
    : FileSystemErrT[Free[S, ?], Fix[LP]] = {

    def lift(e: Set[FPath], plan: Fix[LP]) =
      plan.project.map((e, _)).point[SemanticErrsT[FileSystemErrT[Free[S, ?], ?], ?]]

    def compiledView(loc: AFile): OptionT[Free[S, ?], FileSystemError \/ (SemanticErrors \/ Fix[LP])] =
      (for {
        viewConfig   <- EitherT(EitherT(OptionT(
                          M.lookupViewConfig(loc)
                            .leftMap(e => SemanticError.genericError(e.shows))
                            .run.run.map(_.map(_.right[FileSystemError]))
                        ))).leftMap(_.wrapNel)
        block        <- EitherT(EitherT(
                          resolveImports_(viewConfig.query, rootDir).run.run.liftM[OptionT]
                        )).leftMap(_.wrapNel)
        r            <- EitherT(EitherT(
                          precompile[Fix[LP]](block, viewConfig.vars, fileParent(loc))
                            .run.value.right[FileSystemError].η[Free[S, ?]].liftM[OptionT]))
      } yield r).run.run

    // NB: simplify incoming queries to the raw, idealized LP which is simpler
    //     to manage.
    val cleaned = plan.cata(optimizer.elideTypeCheckƒ)

    val newLP: SemanticErrsT[FileSystemErrT[Free[S, ?], ?], Fix[LP]] =
      (Set[FPath](), cleaned).anaM[Fix[LP]] {
        case (e, i @ Embed(lp.Read(p))) if !(e contains p) =>
          refineTypeAbs(p).swap.map(f =>
            EitherT(EitherT(compiledView(f) getOrElse i.right.right)).map(_.project.map((e + f, _)))
          ).getOrElse(lift(e, i))

        case (e, i) => lift(e, i)
      } flatMap (resolved => EitherT(preparePlan(resolved).run.value.point[FileSystemErrT[Free[S, ?], ?]]))

    newLP.leftMap(e => planningFailed(plan, Planner.CompilationFailed(e))).flattenLeft
  }
}
