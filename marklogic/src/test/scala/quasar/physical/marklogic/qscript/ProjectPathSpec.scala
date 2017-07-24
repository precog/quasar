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
  def projectField0[T[_[_]]: BirecursiveT, S[_]: Functor](src: Free[MapFunc[T, ?], Hole], str: String)(
    implicit I: MapFunc[T, ?] :<: S
  ): Free[S, Hole] =
    Free.roll[MapFunc[T, ?], Hole](MFC(ProjectField(src, StrLit(str)))).mapSuspension(injectNT[MapFunc[T, ?], S])

  def makeMap[T[_[_]]: BirecursiveT, S[_]: Functor](key: String, values: Free[MapFunc[T, ?], Hole])(
    implicit I: MapFunc[T, ?] :<: S
  ) : Free[S, Hole] =
    Free.roll[MapFunc[T, ?], Hole](MFC(MakeMap(StrLit(key), values))).mapSuspension(injectNT[MapFunc[T, ?], S])

  def holeS[T[_[_]]: BirecursiveT, S[_]: Functor](implicit I: MapFunc[T, ?] :<: S): Free[S, Hole] =
    Free.point[MapFunc[T, ?], Hole](SrcHole).mapSuspension(injectNT[MapFunc[T, ?], S])

  def projectPath[T[_[_]]: BirecursiveT](src: FreePathMap[T], path: ADir): FreePathMap[T] =
    Free.roll(Inject[ProjectPath, PathMapFunc[T, ?]].inj(ProjectPath(src, path)))

  def projectField(src: FreeMap[Fix], str: String): FreeMap[Fix] =
    projectField0[Fix, MapFunc[Fix, ?]](src, str)

  def makeMapPath(key: String, values: Free[MapFunc[Fix, ?], Hole]): FreePathMap[Fix] =
    makeMap[Fix, PathMapFunc[Fix, ?]](key, values)

  def holeM = holeS[Fix, MapFunc[Fix, ?]]
  def holeP = holeS[Fix, PathMapFunc[Fix, ?]]

  "foldProjectField" should {
    "squash nested ProjectField of strings into a single ProjectPath" in {
      val nestedProjects = projectField(projectField(holeM, "info"), "location")

      ProjectPath.foldProjectField(nestedProjects) must
        equal(projectPath[Fix](holeP, rootDir[Sandboxed] </> dir("info") </> dir("location")))
    }

    "preserve an unrelated node inside a nesting of ProjectField" in {
      val inclUnrelatedNode = projectField(makeMap("k", projectField(holeM, "info")), "location")

      ProjectPath.foldProjectField(inclUnrelatedNode) must
        equal(projectPath[Fix](makeMapPath("k", projectField0(holeM, "info")), rootDir[Sandboxed] </> dir("location")))
    }
  }
}
