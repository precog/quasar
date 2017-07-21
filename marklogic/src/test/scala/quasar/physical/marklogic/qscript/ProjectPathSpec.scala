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
import quasar.qscript.MapFuncsCore._
import quasar.qscript._

import matryoshka.data._
import matryoshka.{Hole => _, _}
import pathy._, Path._

import scalaz._

final class ProjectPathSpec extends quasar.Qspec {
  def projectField[T[_[_]]: BirecursiveT](src: FreeMap[T], str: String): FreeMap[T] =
    Free.roll(MFC(ProjectField(src, StrLit(str))))

  def projectPath[T[_[_]]: BirecursiveT](src: FreePathMap[T], path: ADir): FreePathMap[T] =
    Free.roll(Inject[ProjectPath, PathMapFunc[T, ?]].inj(ProjectPath(src, path)))

  def hole[T[_[_]]: BirecursiveT]: FreePathMap[T] = Free.point[PathMapFunc[T, ?], Hole](SrcHole)

  "foldProjectField" should {
    "squash nested ProjectField of constants into a single ProjectPath" in {
      val nestedProjects: FreeMap[Fix] = projectField(projectField(HoleF, "info"), "location")

      ProjectPath.foldProjectField(nestedProjects) must_===
        projectPath(hole, rootDir[Sandboxed] </> dir("location") </> dir("info"))
    }
  }
}
