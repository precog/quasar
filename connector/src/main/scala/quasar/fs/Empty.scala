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

package quasar.fs

import slamdata.Predef._
import quasar.Planner.UnsupportedPlan
import quasar.common.PhaseResults
import quasar.contrib.pathy._
import quasar.frontend.logicalplan.{LogicalPlan, LogicalPlanR}
import quasar.fp.free._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.{~>, \/, Applicative, Bind}
import scalaz.syntax.equal._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.std.option._

/** `FileSystem` interpreters for a filesystem that has no, and doesn't support
  * creating any, files.
  */
object Empty {
  import FileSystemError._, PathError._

  def readFile[F[_]: Applicative] = λ[ReadFile ~> F] {
    case ReadFile.Open(f, _, _) => fsPathNotFound(f)
    case ReadFile.Read(h)       => unknownReadHandle(h).left.point[F]
    case ReadFile.Close(_)      => ().point[F]
  }

  def writeFile[F[_]: Applicative] = λ[WriteFile ~> F] {
    case WriteFile.Open(f)        => WriteFile.WriteHandle(f, 0).right.point[F]
    case WriteFile.Write(_, data) => data.map(writeFailed(_, "empty filesystem")).point[F]
    case WriteFile.Close(_)       => ().point[F]
  }

  def manageFile[F[_]: Applicative] = λ[ManageFile ~> F] {
    case ManageFile.Move(scn, _) => fsPathNotFound(scn.src)
    case ManageFile.Copy(pair)   => fsPathNotFound(pair.src)
    case ManageFile.Delete(p)    => fsPathNotFound(p)
    case ManageFile.TempFile(p)  => (refineType(p).swap.valueOr(fileParent) </> file("tmp")).right.point[F]
  }

  def queryFile[F[_]: Applicative]: QueryFile ~> F =
    new (QueryFile ~> F) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case QueryFile.ExecutePlan(lp, _) =>
          lpResult(lp)

        case QueryFile.EvaluatePlan(lp) =>
          lpResult(lp)

        case QueryFile.More(h) =>
          unknownResultHandle(h).left.point[F]

        case QueryFile.Close(_) =>
          ().point[F]

        case QueryFile.Explain(lp) =>
          lpResult(lp)

        case QueryFile.ListContents(d) =>
          if (d === rootDir)
            \/.right[FileSystemError, Set[PathSegment]](Set()).point[F]
          else
            fsPathNotFound(d)

        case QueryFile.FileExists(_) =>
          false.point[F]
      }
    }

  def analyze[F[_] : Applicative]: Analyze ~> F = new (Analyze ~> F) {
    def apply[A](from: Analyze[A]) = from match {
      case Analyze.QueryCost(lp) => lpResult[F, FileSystemError \/ Int](lp).map(_._2).map(Bind[FileSystemError \/ ?].join(_))
    }
  }


  def fileSystem[F[_]: Applicative]: FileSystem ~> F =
    interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  def backendEffect[F[_]: Applicative]: BackendEffect ~> F =
    analyze :+: fileSystem

  ////

  private val lp = new LogicalPlanR[Fix[LogicalPlan]]

  private def lpResult[F[_]: Applicative, A](plan: Fix[LogicalPlan]): F[(PhaseResults, FileSystemError \/ A)] =
    lp.paths(plan)
      .findMin
      // Documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
      .cata(p => fsPathNotFound[F, A](mkAbsolute(rootDir, p)), unsupportedPlan[F, A](plan))
      .strengthL(Vector())

  private def fsPathNotFound[F[_]: Applicative, A](p: APath): F[FileSystemError \/ A] =
    pathErr(pathNotFound(p)).left.point[F]

  private def unsupportedPlan[F[_]: Applicative, A](lp: Fix[LogicalPlan]): F[FileSystemError \/ A] =
    planningFailed(lp, UnsupportedPlan(lp.project, None)).left.point[F]
}
