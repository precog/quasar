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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.contrib.pathy.ADir
import quasar.fp._
import quasar.fp.free._
import quasar.qscript.MapFuncsCore._
import quasar.qscript._

import matryoshka.data._
import matryoshka.{Hole => _, _}
import pathy._, Path._

import scalaz._, Scalaz._

final class ProjectPathSpec extends quasar.Qspec {
  def projectField[S[_]: Functor](src: Free[MapFunc[Fix, ?], Hole], str: String)(
    implicit I: MapFunc[Fix, ?] :<: S
  ): Free[S, Hole] =
    Free.roll[MapFunc[Fix, ?], Hole](MFC(ProjectField(src, StrLit(str)))).mapSuspension(injectNT[MapFunc[Fix, ?], S])

  def makeMap[S[_]: Functor](key: String, values: Free[MapFunc[Fix, ?], Hole])(
    implicit I: MapFunc[Fix, ?] :<: S
  ) : Free[S, Hole] =
    Free.roll[MapFunc[Fix, ?], Hole](MFC(MakeMap(StrLit(key), values))).mapSuspension(injectNT[MapFunc[Fix, ?], S])

  def hole[S[_]: Functor](
    implicit I: MapFunc[Fix, ?] :<: S
  ): Free[S, Hole] =
    Free.point[MapFunc[Fix, ?], Hole](SrcHole).mapSuspension(injectNT[MapFunc[Fix, ?], S])

  def projectPath(src: FreePathMap[Fix], path: ADir): FreePathMap[Fix] =
    Free.roll(Inject[ProjectPath, PathMapFunc[Fix, ?]].inj(ProjectPath(src, path)))

  val root = rootDir[Sandboxed]

  "foldProjectField" should {
    "squash nested ProjectField of strings into a single ProjectPath" in {
      val nestedProjects = projectField[MapFunc[Fix, ?]](projectField(hole, "info"), "location")

      ProjectPath.foldProjectField(nestedProjects) must
        equal(projectPath(hole, root </> dir("info") </> dir("location")))
    }

    "fold a single ProjectField into a single ProjectPath" in {
      ProjectPath.foldProjectField(projectField[MapFunc[Fix, ?]](hole, "location")) must
        equal(projectPath(hole, root </> dir("location")))
    }

    "preserve an unrelated node inside a nesting of ProjectField" in {
      val inclUnrelatedNode = projectField[MapFunc[Fix, ?]](makeMap("k", projectField(hole, "info")), "location")

      ProjectPath.foldProjectField(inclUnrelatedNode) must
        equal(projectPath(makeMap("k", projectField(hole, "info")), root </> dir("location")))
    }
  }
}
