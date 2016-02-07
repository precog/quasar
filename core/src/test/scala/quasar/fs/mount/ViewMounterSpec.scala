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
import quasar.{LogicalPlan, Variables}
import quasar.effect.AtomicRef
import quasar.fp.TaskRef
import quasar.fs.{Path => QPath}
import quasar.recursionschemes.Fix
import quasar.sql

import org.specs2.mutable
import pathy.Path._
import scalaz._
import scalaz.concurrent.Task

class ViewMounterSpec extends mutable.Specification {
  import MountingError._

  type F[A]      = Free[MountedViewsF, A]
  type ViewsS[A] = State[Views, A]
  type Res[A]    = (Views, A)

  def eval(vs: Views): F ~> Res =
    new (F ~> Res) {
      def apply[A](fa: F[A]) = {
        val f = AtomicRef.toState[State, Views]
        fa.foldMap(Coyoneda.liftTF[MountedViews, ViewsS](f)).run(vs)
      }
    }

  "mounting views" >> {
    "fails with InvalidConfig when compilation fails" >> {
      val fnDNE = Fix(sql.InvokeFunctionF("DNE", List[sql.Expr]()))
      val f     = rootDir </> dir("mnt") </> file("dne")

      eval(Views.empty)(ViewMounter.mount[MountedViewsF](f, fnDNE, Variables.empty))
        ._2 must beLike {
          case -\/(InvalidConfig(_, _)) => ok
        }
    }

    "updates mounted views with compiled plan when compilation succeeds" >> {
      val selStar = Fix(sql.SelectF(
        sql.SelectAll,
        Nil,
        Some(sql.TableRelationAST[sql.Expr]("/foo/bar", None)),
        None, None, None))

      val f = rootDir </> dir("mnt") </> file("selectStar")

      eval(Views.empty)(ViewMounter.mount[MountedViewsF](f, selStar, Variables.empty))
        ._1.map.get(f) must beSome
    }
  }

  "unmounting views" >> {
    "removes plan from mounted views" >> {
      val rd = LogicalPlan.Read(QPath("/foo/bar"))
      val f  = rootDir </> dir("mnt") </> file("foo")

      eval(Views(Map(f -> rd)))(ViewMounter.unmount[MountedViewsF](f))
        ._1.map.toList must beEmpty
    }
  }
}
