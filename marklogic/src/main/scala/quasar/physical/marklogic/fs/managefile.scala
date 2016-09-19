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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.fp.free.lift
import quasar.fs._
import quasar.physical.marklogic.xcc.SessionIO

import scala.util.Random

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object managefile {
  import ManageFile._
  import PathError._, FileSystemError._

  def interpret[S[_]](implicit S: SessionIO :<: S): ManageFile ~> Free[S, ?] =
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](fs: ManageFile[A]) = fs match {
        case Move(scenario, semantics) => move(scenario, semantics)
        case Delete(path)              => delete(path)
        case TempFile(path)            => tempFile(path)
      }

      def move(scenario: MoveScenario, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] =
        lift(scenario match {
          case MoveScenario.FileToFile(src, dst) => moveFile(src, dst, semantics)
          case MoveScenario.DirToDir(src, dst)   => moveDir(src, dst, semantics)
        }).into[S]

      def checkMoveSemantics(dst: APath, sem: MoveSemantics): FileSystemErrT[SessionIO, Unit] =
        EitherT(sem match {
          case MoveSemantics.Overwrite =>
            ().right[FileSystemError].point[SessionIO]

          case MoveSemantics.FailIfExists =>
            ops.exists(dst).map(_.fold(
              pathErr(pathExists(dst)).left[Unit],
              ().right))

          case MoveSemantics.FailIfMissing =>
            ops.exists(dst).map(_.fold(
              ().right,
              pathErr(pathNotFound(dst)).left[Unit]))
        })

      def moveFile(src: AFile, dst: AFile, sem: MoveSemantics): SessionIO[FileSystemError \/ Unit] =
        ifExists(src)((
          checkMoveSemantics(dst, sem) *>
          ops.moveFile(src, dst).liftM[FileSystemErrT]
        ).run)

      def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): SessionIO[FileSystemError \/ Unit] = {
        def moveContents(src0: ADir, dst0: ADir): SessionIO[Unit] =
          ops.ls(src0).flatMap(_.traverse_(_.fold(
            d => moveContents(src0 </> dir1(d), dst0 </> dir1(d)),
            f => ops.moveFile(src0 </> file1(f), dst0 </> file1(f)))))

        def doMove =
          checkMoveSemantics(dst, sem)                 *>
          moveContents(src, dst).liftM[FileSystemErrT] *>
          ops.deleteDir(src).liftM[FileSystemErrT]

        ifExists(src)(doMove.run)
      }

      def delete(path: APath): Free[S, FileSystemError \/ Unit] =
        lift(ifExists(path)(
          refineType(path)
            .fold(ops.deleteDir, ops.deleteFile)
            .map(_.right[FileSystemError])
        )).into[S]

      def tempFile(path: APath): Free[S, FileSystemError \/ AFile] =
        tempName map { fname =>
          refineType(path).fold(
            d => d </> file(fname),
            f => fileParent(f) </> file(fname)
          ).right
        }

      val tempName: Free[S, String] =
        lift(SessionIO.liftT(
          Task.delay("temp-" + Random.alphanumeric.take(10).mkString)
        )).into[S]

      def ifExists[A](
        path: APath)(
        thenDo: => SessionIO[FileSystemError \/ A]
      ): SessionIO[FileSystemError \/ A] =
        ops.exists(path).ifM(thenDo, pathErr(pathNotFound(path)).left[A].point[SessionIO])
    }
}
