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
import quasar.effect.Capture
import quasar.fs._
import quasar.physical.marklogic.xcc._

import scala.util.Random

import pathy.Path._
import scalaz._, Scalaz._

object managefile {
  import ManageFile._
  import PathError._, FileSystemError._

  def interpret[F[_]: Monad: Capture: SessionReader: XccErr]: ManageFile ~> F = {
    val tempName: F[String] =
      Capture[F].delay("temp-" + Random.alphanumeric.take(10).mkString)

    def ifExists[A](
      path: APath)(
      thenDo: => F[FileSystemError \/ A]
    ): F[FileSystemError \/ A] =
      ops.exists[F](path).ifM(thenDo, pathErr(pathNotFound(path)).left[A].point[F])

    def checkMoveSemantics(dst: APath, sem: MoveSemantics): FileSystemErrT[F, Unit] =
      EitherT(sem match {
        case MoveSemantics.Overwrite =>
          ().right[FileSystemError].point[F]

        case MoveSemantics.FailIfExists =>
          ops.exists[F](dst).map(_.fold(
            pathErr(pathExists(dst)).left[Unit],
            ().right))

        case MoveSemantics.FailIfMissing =>
          ops.exists[F](dst).map(_.fold(
            ().right,
            pathErr(pathNotFound(dst)).left[Unit]))
      })

    def moveFile(src: AFile, dst: AFile, sem: MoveSemantics): F[FileSystemError \/ Unit] =
      ifExists(src)((
        checkMoveSemantics(dst, sem) *>
        ops.moveFile[F](src, dst).liftM[FileSystemErrT]
      ).run)

    def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): F[FileSystemError \/ Unit] = {
      def moveContents(src0: ADir, dst0: ADir): F[Unit] =
        ops.ls[F](src0).flatMap(_.traverse_(_.fold(
          d => moveContents(src0 </> dir1(d), dst0 </> dir1(d)),
          f => ops.moveFile[F](src0 </> file1(f), dst0 </> file1(f)))))

      def doMove =
        checkMoveSemantics(dst, sem)                 *>
        moveContents(src, dst).liftM[FileSystemErrT] *>
        ops.deleteDir[F](src).liftM[FileSystemErrT]

      ifExists(src)(doMove.run)
    }

    def move(scenario: MoveScenario, semantics: MoveSemantics): F[FileSystemError \/ Unit] =
      scenario match {
        case MoveScenario.FileToFile(src, dst) => moveFile(src, dst, semantics)
        case MoveScenario.DirToDir(src, dst)   => moveDir(src, dst, semantics)
      }

    def delete(path: APath): F[FileSystemError \/ Unit] =
      ifExists(path)(
        refineType(path)
          .fold(ops.deleteDir[F], ops.deleteFile[F])
          .map(_.right[FileSystemError]))

    def tempFile(path: APath): F[FileSystemError \/ AFile] =
      tempName map { fname =>
        refineType(path).fold(
          d => d </> file(fname),
          f => fileParent(f) </> file(fname)
        ).right
      }

    λ[ManageFile ~> F] {
      case Move(scenario, semantics) => move(scenario, semantics)
      case Delete(path)              => delete(path)
      case TempFile(path)            => tempFile(path)
    }
  }
}
