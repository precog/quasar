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
import quasar.fp.κ
import quasar.fp.prism._
import quasar.fs._
import quasar.sql.Expr

import matryoshka._, TraverseT.ops._
import pathy.Path._
import scalaz._, Scalaz._

object ViewMounter {
  import MountingError._, MountConfig._

  /** Retrieve view file paths */
  def viewPaths[S[_]: Functor]
    (implicit S: MountConfigsF :<: S)
    : Free[S, Vector[AFile]] =
    mntCfgs[S].keys.flatMap(_.foldMap(refineType(_).fold(
      κ(Vector.empty[AFile].point[Free[S, ?]]),
      f => lookup[S](f).fold(κ(Vector(f)), Vector.empty))))

  /** Lookup mounted view */
  def lookup[S[_]: Functor]
    (loc: AFile)
    (implicit S: MountConfigsF :<: S)
    : OptionT[Free[S, ?], (Expr, Variables)] =
    mntCfgs[S].get(loc).flatMap(mc => OptionT.optionT[Free[S, ?]](Free.point(viewConfig.getOption(mc))))

  /** Attempts to mount a view at the given location. */
  def mount[S[_]: Functor]
    (loc: AFile, query: Expr, vars: Variables)
    (implicit S: MountConfigsF :<: S)
    : Free[S, MountingError \/ Unit] = {
    val vc = viewConfig(query, vars)
    queryPlan(query, vars).run.value.fold(
      e => invalidConfig(vc, e.map(_.shows)).left.point[Free[S, ?]],
      κ(mntCfgs[S].put(loc, vc).map(_.right)))
  }

  /** Attempts to move a view at the given location. */
  def move[S[_]: Functor]
    (srcLoc: AFile, dstLoc: AFile)
    (implicit S: MountConfigsF :<: S)
    : Free[S, Unit] =
    mntCfgs[S].move(srcLoc, dstLoc)

  /** Unmounts the view at the given location. */
  def unmount[S[_]: Functor]
    (loc: AFile)
    (implicit S0: MountConfigsF :<: S)
    : Free[S, Unit] =
    lookup[S](loc).flatMapF(κ(mntCfgs[S].delete(loc))).run.void

  /** Resolve view references within a query. */
  def rewrite[S[_]: Functor]
    (lp: Fix[LogicalPlan])
    (implicit S0: MountConfigsF :<: S)
    : SemanticErrsT[Free[S, ?], Fix[LogicalPlan]] = {

    implicit val m = EitherT.eitherTMonad[Free[S, ?], SemanticErrors]

    def lift(e: Set[FPath], lp: Fix[LogicalPlan]) =
      EitherT.right[Free[S, ?], SemanticErrors, LogicalPlan[(Set[FPath], Fix[LogicalPlan])]](
        lp.unFix.map((e, _)).point[Free[S, ?]])

    (Set[FPath](), lp).anaM[Fix, SemanticErrsT[Free[S, ?], ?], LogicalPlan] {
      case (e, i @ Embed(r @ LogicalPlan.ReadF(p))) if !(e contains p) =>
        refineTypeAbs(p).fold(
          f => EitherT[Free[S, ?], SemanticErrors, LogicalPlan[(Set[FPath], Fix[LogicalPlan])]](
            lookup[S](f).run.flatMap[SemanticErrors \/ LogicalPlan[(Set[FPath], Fix[LogicalPlan])]] {
              _ .cata(
                  { case (expr, vars) => queryPlan(expr, vars).run.run._2.map(absolutize(_, fileParent(f))) },
                  i.right)
                .map(_.unFix.map((e + f, _)))
                .point[Free[S, ?]]
            }),
          κ(lift(e, i)))
      case (e, i) => lift(e, i)
    }

  }

  /** Enumerate view files and view ancestor directories at a particular location. */
  def ls[S[_]: Functor]
    (dir: ADir)
    (implicit S0: MountConfigsF :<: S)
    : Free[S, Set[PathSegment]] =
    viewPaths[S].map(_.foldMap(_.relativeTo(dir).flatMap(firstSegmentName).toSet))

  ////

  private def mntCfgs[S[_]: Functor](implicit S: MountConfigsF :<: S) = KeyValueStore.Ops[APath, MountConfig, S]

  /** Rewrite relative paths to be based on the given dir. */
  private def absolutize(lp: Fix[LogicalPlan], dir: ADir): Fix[LogicalPlan] =
    lp.transCata {
      case read @ LogicalPlan.ReadF(p) =>
        refineTypeAbs(p).fold(κ(read), rel => LogicalPlan.ReadF(dir </> rel))
      case t => t
    }

}
