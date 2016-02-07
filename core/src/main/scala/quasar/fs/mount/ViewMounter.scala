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

import quasar.Predef.Unit
import quasar.{queryPlan, Variables}
import quasar.effect.AtomicRef
import quasar.fs.AFile
import quasar.fp.prism._
import quasar.sql.Expr

import scalaz._
import scalaz.syntax.show._
import scalaz.syntax.applicative._
import scalaz.syntax.either._

object ViewMounter {
  import MountingError._, MountConfig2._

  /** Attempts to mount a view at the given location. */
  def mount[S[_]: Functor]
      (loc: AFile, query: Expr, vars: Variables)
      (implicit S: MountedViewsF :<: S)
      : Free[S, MountingError \/ Unit] = {

    queryPlan(query, vars).run.value.fold(
      errs => invalidConfig(viewConfig(query, vars), errs.map(_.shows))
                .left.point[Free[S, ?]],
      lp   => views[S].modify(_.add(loc, lp)).as(().right))
  }

  /** Unmounts the view at the given location. */
  def unmount[S[_]: Functor](loc: AFile)(implicit S: MountedViewsF :<: S): Free[S, Unit] =
    views[S].modify(_.remove(loc)).void

  ////

  private def views[S[_]: Functor](implicit S: MountedViewsF :<: S) =
    AtomicRef.Ops[Views, S]
}
