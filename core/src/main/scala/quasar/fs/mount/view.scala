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
import quasar.fs.mount.cache.{VCache, ViewCache}, VCache.VCacheKVS
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, Optimizer}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.{Failure => _, Node => _, _}, Scalaz._
import scalaz.concurrent.Task

object view {

  sealed abstract class ResultHandle

  private final case class ReadH(handle: ReadFile.ReadHandle)     extends ResultHandle
  private final case class QueryH(handle: QueryFile.ResultHandle) extends ResultHandle

  type State[A] = KeyValueStore[ReadFile.ReadHandle, ResultHandle, A]

  object State {
    def Ops[S[_]](
      implicit S: State :<: S
    ): KeyValueStore.Ops[ReadFile.ReadHandle, ResultHandle, S] =
      KeyValueStore.Ops[ReadFile.ReadHandle, ResultHandle, S]

    type ViewHandles = Map[ReadFile.ReadHandle, ResultHandle]

    def toTask(initial: ViewHandles): Task[State ~> Task] =
      TaskRef(initial) map KeyValueStore.impl.fromTaskRef
  }

  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  /** Translate reads on view paths to the equivalent queries. */
  def readFile[S[_]](
    implicit
    S0: ReadFile :<: S,
    S1: QueryFile :<: S,
    S2: MonotonicSeq :<: S,
    S3: State :<: S,
    S4: VCacheKVS :<: S,
    S5: Mounting :<: S
  ): ReadFile ~> Free[S, ?] = {
    import ReadFile._

    val readUnsafe = ReadFile.Unsafe[S]
    val queryUnsafe = QueryFile.Unsafe[S]
    val seq = MonotonicSeq.Ops[S]
    val state = State.Ops[S]
    val mount = Mounting.Ops[S]

    def openFile(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] =
      for {
        readHandle <- readUnsafe.open(f, off, lim)
        handle     <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
        _          <- state.put(handle, ReadH(readHandle)).liftM[FileSystemErrT]
      } yield handle

    def openView(f: AFile, off: Natural, lim: Option[Positive]): FileSystemErrT[Free[S, ?], ReadHandle] = {
      val readLP = addOffsetLimit(lpr.read(f), off, lim)

      for {
        lp          <- resolveViewRefs[S](readLP)
        queryHandle <- EitherT(queryUnsafe.eval(lp).run.value)
        readHandle  <- seq.next.map(ReadHandle(f, _)).liftM[FileSystemErrT]
        _           <- state.put(readHandle, QueryH(queryHandle)).liftM[FileSystemErrT]
      } yield readHandle
    }

    λ[ReadFile ~> Free[S, ?]] {
      case Open(file, off, lim) =>
        mount.exists(file).ifM(
          openView(file, off, lim).run,
          openFile(file, off, lim).run)

      case Read(handle) =>
        state.get(handle).toRight(unknownReadHandle(handle)).flatMap {
          case QueryH(handle) => queryUnsafe.more(handle)
          case ReadH(handle)  => readUnsafe.read(handle)
        }.run

      case Close(handle) =>
        state.get(handle).flatMapF {
          case QueryH(queryHandle) => queryUnsafe.close(queryHandle) *> state.delete(handle)
          case ReadH(handle)  => readUnsafe.close(handle)
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
    S1: VCacheKVS :<: S,
    S2: Mounting :<: S
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

    def listViews(dir: ADir): Free[S, Set[Node]] =
      mount.viewsHavingPrefix_(dir).map(_ foldMap { f =>
        firstSegmentName(f).map(_.fold[Node](Node.ImplicitDir(_), Node.View(_))).toSet
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
    S0: VCacheKVS :<: S,
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
    S5: State :<: S,
    S6: VCacheKVS :<: S,
    S7: Mounting :<: S,
    S8: MountingFailure :<: S,
    S9: PathMismatchFailure :<: S
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
    S5: State :<: S,
    S6: VCacheKVS :<: S,
    S7: Mounting :<: S,
    S8: MountingFailure :<: S,
    S9: PathMismatchFailure :<: S,
    S10: Analyze :<: S
  ): BackendEffect ~> Free[S, ?] = analyze :+: fileSystem[S]

  /** Resolve view references in the given `LP`. */
  def resolveViewRefs[S[_]](
    plan: Fix[LP]
  )(implicit
    S0: VCacheKVS :<: S,
    M: Mounting.Ops[S]
  ): FileSystemErrT[Free[S, ?], Fix[LP]] = {
    val VC = VCacheKVS.Ops[S]

    def lift(e: Set[FPath], plan: Fix[LP]) =
      plan.project.strengthL(e).point[SemanticErrsT[FileSystemErrT[Free[S, ?], ?], ?]]

    def compiledView(loc: AFile): OptionT[Free[S, ?], FileSystemError \/ (SemanticErrors \/ Fix[LP])] =
      (for {
        viewConfig   <- EitherT(EitherT(OptionT(
                          M.lookupViewConfig(loc)
                            .leftMap(e => SemanticError.genericError(e.shows))
                            .run.run.map(_.map(_.right[FileSystemError]))
                        ))).leftMap(_.wrapNel)
        block        <- EitherT(EitherT(
                          resolveImports_(viewConfig.query, fileParent(loc)).run.run.liftM[OptionT]
                        )).leftMap(_.wrapNel)
        r            <- EitherT(EitherT(
                          precompile[Fix[LP]](block, viewConfig.vars, fileParent(loc))
                            .run.value.right[FileSystemError].η[Free[S, ?]].liftM[OptionT]))
      } yield r).run.run

    def vcacheRead(loc: AFile): OptionT[Free[S, ?], FileSystemError \/ (SemanticErrors \/ Fix[LP])] =
      for {
        vc <- VC.get(loc)
        _  <- VC.modify(loc, vc => vc.copy(cacheReads = vc.cacheReads + 1)).liftM[OptionT]
        r  <- OptionT(
                ((vc.status ≟ ViewCache.Status.Successful).option(lp.Read[Fix[LP]](vc.dataFile).embed) ∘ (
                  _.right[SemanticErrors].right[FileSystemError])).η[Free[S, ?]])
      } yield r

    // NB: simplify incoming queries to the raw, idealized LP which is simpler
    //     to manage.
    // JR: Why is this easier to manage exactly? Perhaps we could add a more detailed explanation here
    val cleaned = plan.cata(optimizer.elideTypeCheckƒ)

    // The `Set[FPath]` is to ensure we don't expand the same view within the SAME AST
    // branch as that would be nonsensical and lead to an infinitely large LP
    // Instead, we just ignore it letting the view refer to an underlying file
    // with the same name
    // Alternatively, we could error out saying a view cannot reference itself
    // but we chose the former approach.
    // Note: This does not prevent a view from being referenced twice in an expression, as
    // those references would appear in separate branches and thus not share the same `Set[FPath]`
    val newLP: SemanticErrsT[FileSystemErrT[Free[S, ?], ?], Fix[LP]] =
      (Set[FPath](), cleaned).anaM[Fix[LP]] {
        case (e, i @ Embed(lp.Read(p))) if !(e contains p) =>
          refineTypeAbs(p).swap.map { absFile =>
            val inlinedView = vcacheRead(absFile) orElse compiledView(absFile) getOrElse i.right.right
            EitherT(EitherT(inlinedView)).map(_.project.strengthL(e + absFile))
          }.getOrElse(lift(e, i))

        case (e, i) => lift(e, i)
      } flatMap (resolved => EitherT(preparePlan(resolved).run.value.point[FileSystemErrT[Free[S, ?], ?]]))

    newLP.leftMap(e => planningFailed(plan, Planner.CompilationFailed(e))).flattenLeft
  }
}
