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
import quasar.effect.KeyValueStore
import quasar.fp._
import quasar.fs._
import quasar.sql.Sql

import eu.timepit.refined.auto._
import matryoshka._, TraverseT.ops._
import pathy.Path._
import scalaz._, Scalaz._

object ViewMounter {
  import MountingError._, MountConfig._

  /** Retrieve view file paths */
  def viewPaths[S[_]]
    (implicit S: MountConfigs :<: S)
    : Free[S, Vector[AFile]] =
    mntCfgs[S].keys.flatMap(_.foldMap(refineType(_).fold(
      κ(Vector.empty[AFile].point[Free[S, ?]]),
      f => exists[S](f).map(_ ?? Vector(f)))))

  /** Check whether a view is located at the given path. */
  def exists[S[_]]
    (loc: AFile)
    (implicit S: MountConfigs :<: S)
    : Free[S, Boolean] =
    mntCfgs[S].contains(loc)

  /** Attempts to mount a view at the given location. */
  def mount[S[_]]
    (loc: AFile, query: Fix[Sql], vars: Variables)
    (implicit S: MountConfigs :<: S)
    : Free[S, MountingError \/ Unit] = {
      val vc = viewConfig(query, vars)
      queryPlan(query, vars, fileParent(loc), 0L, None).run.value.fold(
        e => invalidConfig(vc, e.map(_.shows)).left.point[Free[S, ?]],
        κ(mntCfgs[S].put(loc, vc).map(_.right)))
    }

  /** Attempts to move a view at the given location. */
  def move[S[_]]
    (srcLoc: AFile, dstLoc: AFile)
    (implicit S: MountConfigs :<: S)
    : Free[S, Unit] =
    mntCfgs[S].move(srcLoc, dstLoc)

  /** Unmounts the view at the given location. */
  def unmount[S[_]]
    (loc: AFile)
    (implicit S0: MountConfigs :<: S)
    : Free[S, Unit] =
    exists[S](loc).flatMap(_.whenM(mntCfgs[S].delete(loc)))

  /** Resolve view references within a query. */
  def rewrite[S[_]]
    (lp: Fix[LogicalPlan])
    (implicit S0: MountConfigs :<: S)
    : SemanticErrsT[Free[S, ?], Fix[LogicalPlan]] = {

    implicit val m: Monad[EitherT[Free[S, ?], SemanticErrors, ?]] =
      EitherT.eitherTMonad[Free[S, ?], SemanticErrors]

    def lift(e: Set[FPath], lp: Fix[LogicalPlan]) =
      EitherT.right[Free[S, ?], SemanticErrors, LogicalPlan[(Set[FPath], Fix[LogicalPlan])]](
        lp.unFix.map((e, _)).point[Free[S, ?]])

    def lookup(loc: AFile): OptionT[Free[S, ?], (Fix[Sql], Variables)] =
      mntCfgs[S].get(loc).flatMap(mc =>
        OptionT.optionT[Free[S, ?]](Free.point(viewConfig.getOption(mc))))

    /** This collapses literal data results into a LogicalPlan for cases where
      * currently _want_ a Constant plan. This will have to go away when
      * [[quasar.Data.Set]] does.
      */
    def collapseData[T[_[_]]: Corecursive](plan: List[Data] \/ T[LogicalPlan]):
        T[LogicalPlan] =
      plan.fold(d => LogicalPlan.ConstantF[T[LogicalPlan]](Data.Set(d)).embed, ι)

    (Set[FPath](), lp).anaM[Fix, SemanticErrsT[Free[S, ?], ?], LogicalPlan] {
      case (e, i @ Embed(r @ LogicalPlan.ReadF(p))) if !(e contains p) =>
        refineTypeAbs(p).fold(
          f => EitherT[Free[S, ?], SemanticErrors, LogicalPlan[(Set[FPath], Fix[LogicalPlan])]](
            lookup(f).run.flatMap[SemanticErrors \/ LogicalPlan[(Set[FPath], Fix[LogicalPlan])]] {
              _.cata(
                { case (expr, vars) => queryPlan(expr, vars, fileParent(f), 0L, None).run.run._2.map(p => collapseData(p.map(absolutize(_, fileParent(f))))) },
                  i.right)
                .map(_.unFix.map((e + f, _)))
                .point[Free[S, ?]]
            }),
          κ(lift(e, i)))
      case (e, i) => lift(e, i)
    }

  }

  /** Enumerate view files and view ancestor directories at a particular location. */
  def ls[S[_]]
    (dir: ADir)
    (implicit S0: MountConfigs :<: S)
    : Free[S, Set[PathSegment]] =
    viewPaths[S].map(_.foldMap(_.relativeTo(dir).flatMap(firstSegmentName).toSet))

  ////

  private def mntCfgs[S[_]](implicit S: MountConfigs :<: S) = KeyValueStore.Ops[APath, MountConfig, S]

  /** Rewrite relative paths to be based on the given dir. */
  private def absolutize(lp: Fix[LogicalPlan], dir: ADir): Fix[LogicalPlan] =
    lp.transCata {
      case read @ LogicalPlan.ReadF(p) =>
        refineTypeAbs(p).fold(κ(read), rel => LogicalPlan.ReadF(dir </> rel))
      case t => t
    }

}
