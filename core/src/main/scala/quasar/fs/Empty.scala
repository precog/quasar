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

package quasar.fs

import quasar.Predef.{Vector, None, Set}
import quasar.Planner.UnsupportedPlan
import quasar.{LogicalPlan, PhaseResults}
import quasar.fp.prism._
import quasar.recursionschemes.Fix

import pathy.Path._
import scalaz.{~>, \/, Applicative}
import scalaz.syntax.equal._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.std.option._

/** `FileSystem` interpreters for a filesystem that has no, and doesn't support
  * creating any, files.
  */
object Empty {
  import FileSystemError._, PathError2._

  def readFile[F[_]: Applicative]: ReadFile ~> F =
    new (ReadFile ~> F) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case ReadFile.Open(f, _, _) =>
          fsPathNotFound(f)

        case ReadFile.Read(h) =>
          unknownReadHandle(h).left.point[F]

        case ReadFile.Close(_) =>
          ().point[F]
      }
    }

  def writeFile[F[_]: Applicative]: WriteFile ~> F =
    new (WriteFile ~> F) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case WriteFile.Open(f) =>
          WriteFile.WriteHandle(f, 0).right.point[F]

        case WriteFile.Write(_, data) =>
          data.map(writeFailed(_, "empty filesystem")).point[F]

        case WriteFile.Close(_) =>
          ().point[F]
      }
    }

  def manageFile[F[_]: Applicative]: ManageFile ~> F =
    new (ManageFile ~> F) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case ManageFile.Move(scn, _) =>
          fsPathNotFound(scn.src)

        case ManageFile.Delete(p) =>
          fsPathNotFound(p)

        case ManageFile.TempFile(p) =>
          (refineType(p).swap.valueOr(fileParent) </> file("tmp")).right.point[F]
      }
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
            \/.right[FileSystemError, Set[PathName]](Set()).point[F]
          else
            fsPathNotFound(d)

        case QueryFile.FileExists(_) =>
          false.right.point[F]
      }
    }

  def fileSystem[F[_]: Applicative]: FileSystem ~> F =
    interpretFileSystem(queryFile, readFile, writeFile, manageFile)

  ////

  private def lpResult[F[_]: Applicative, A](lp: Fix[LogicalPlan]): F[(PhaseResults, FileSystemError \/ A)] =
    LogicalPlan.paths(lp)
      .headOption
      .cata(p => fsPathNotFound[F, A](p.asAPath), unsupportedPlan[F, A](lp))
      .strengthL(Vector())

  private def fsPathNotFound[F[_]: Applicative, A](p: APath): F[FileSystemError \/ A] =
    pathError(PathNotFound(p)).left.point[F]

  private def unsupportedPlan[F[_]: Applicative, A](lp: Fix[LogicalPlan]): F[FileSystemError \/ A] =
    plannerError(lp, UnsupportedPlan(lp.unFix, None)).left.point[F]
}
