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

package quasar.physical.marklogic.qscript

import slamdata.Predef._
import quasar.contrib.pathy.ADir
import quasar.contrib.iota.SubInject
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.MapFuncsCore._
import quasar.qscript._

import matryoshka.data._
import matryoshka.{Hole => _, _}
import pathy._, Path._

import scalaz._, Scalaz._
import iotaz.CopK

final class ProjectPathSpec extends quasar.Qspec {
  val json = Fixed[Fix[EJson]]

  implicit val I = SubInject[MapFunc[Fix, ?], PathMapFunc[Fix, ?]]

  def projectKey[S[_]: Functor](src: Free[MapFunc[Fix, ?], Hole], str: String)(
    implicit I: Injectable[MapFunc[Fix, ?], S]
  ): Free[S, Hole] =
    Free.roll[MapFunc[Fix, ?], Hole](MFC(ProjectKey(src, StrLit(str)))).mapSuspension(I.inject)

  def makeMap[S[_]: Functor](key: String, values: Free[MapFunc[Fix, ?], Hole])(
    implicit I: Injectable[MapFunc[Fix, ?], S]
  ) : Free[S, Hole] =
    Free.roll[MapFunc[Fix, ?], Hole](MFC(MakeMap(StrLit(key), values))).mapSuspension(I.inject)

  def hole[S[_]: Functor](
    implicit I: Injectable[MapFunc[Fix, ?], S]
  ): Free[S, Hole] =
    Free.point[MapFunc[Fix, ?], Hole](SrcHole).mapSuspension(I.inject)

  def projectPath(src: FreePathMap[Fix], path: ADir): FreePathMap[Fix] =
    Free.roll(CopK.Inject[ProjectPath, PathMapFunc[Fix, ?]].inj(ProjectPath(src, path)))

  val root = rootDir[Sandboxed]

  "foldProjectKey" should {
    "squash nested ProjectKey of strings into a single ProjectPath" in {
      val nestedProjects = projectKey[MapFunc[Fix, ?]](projectKey(hole, "info"), "location")

      ProjectPath.foldProjectKey(nestedProjects) must
        equal(projectPath(hole, root </> dir("info") </> dir("location")))
    }

    "fold a single ProjectKey into a single ProjectPath" in {
      ProjectPath.foldProjectKey(projectKey[MapFunc[Fix, ?]](hole, "location")) must
        equal(projectPath(hole, root </> dir("location")))
    }

    "preserve an unrelated node inside a nesting of ProjectKey" in {
      val inclUnrelatedNode = projectKey[MapFunc[Fix, ?]](makeMap("k", projectKey(hole, "info")), "location")

      ProjectPath.foldProjectKey(inclUnrelatedNode) must
        equal(projectPath(makeMap("k", projectKey(hole, "info")), root </> dir("location")))
    }
  }
}
