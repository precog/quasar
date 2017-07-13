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

package quasar.fs.mount

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.numeric._
import quasar.fs._, FileSystemError._, PathError._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP, Optimizer}

import matryoshka._
import matryoshka.data.Fix
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

object nonFsMounts {
  private val optimizer = new Optimizer[Fix[LP]]
  private val lpr = optimizer.lpr

  /** Intercept and handle moves and deletes involving view path(s); all others are passed untouched. */
  def manageFile[S[_]](mountsIn: ADir => Free[S, Set[RPath]])(
                        implicit
                        S0: ManageFile :<: S,
                        S1: QueryFile :<: S,
                        S2: Mounting :<: S,
                        S3: MountingFailure :<: S,
                        S4: PathMismatchFailure :<: S
                      ): ManageFile ~> Free[S, ?] = {
    import ManageFile._
    import MoveSemantics._

    val manage = ManageFile.Ops[S]
    val query = QueryFile.Ops[S]
    val mount = Mounting.Ops[S]
    val mntErr = Failure.Ops[MountingError, S]

    val fsErrorPath = pathErr composeLens errorPath
    val fsPathNotFound = pathErr composePrism pathNotFound

    def deleteMount(loc: APath): Free[S, Unit] =
      mntErr.attempt(mount.unmount(loc)).void

    def overwriteMount(src: APath, dst: APath): Free[S, Unit] =
      deleteMount(dst) *> mount.remount(src, dst)

    def dirToDirMove(src: ADir, dst: ADir, semantics: MoveSemantics): Free[S, FileSystemError \/ Unit] = {
      def moveAll(srcMounts: Set[RPath]): manage.M[Unit] =
        if (srcMounts.isEmpty)
          pathErr(pathNotFound(src)).raiseError[manage.M, Unit]
        else
          srcMounts
            .traverse_ { mountPath =>
              val scenario = refineType(mountPath).fold[MoveScenario](
                m => MoveScenario.DirToDir(src </> m, dst </> m),
                m => MoveScenario.FileToFile(src </> m, dst </> m))
              mountMove(scenario, semantics)
            }

      /** Abort if there is a fileSystem error related to the destination
        * directory to avoid surprising behavior where all the views in
        * a directory move while all the rest of the files stay put.
        *
        * We know that the 'src' directory exists in the fileSystem as
        * otherwise, the error would be src not found. We are happy to
        * ignore the latter as views can exist outside any filesystem so
        * there may be "directories" comprised of nothing but views.
        *
        * Otherwise, attempt to move the views, emitting any errors that may
        * occur in the process.
        */
      def onFileSystemError(mounts: Set[RPath], err: FileSystemError): Free[S, FileSystemError \/ Unit] =
        fsErrorPath.getOption(err).exists(_ === dst)
          .fold(err.left[Unit].point[Free[S, ?]], moveAll(mounts).run)

      /** Attempt to move views, but silence any 'src not found' errors since
        * this means there aren't any views to move, which isn't an error in this
        * case.
        */
      def onFileSystemSuccess(mounts: Set[RPath]): Free[S, FileSystemError \/ Unit] =
        moveAll(mounts).handleError(err =>
          fsPathNotFound.getOption(err).exists(_ === src)
            .fold(().point[manage.M], err.raiseError[manage.M, Unit]))
          .run

      mountsIn(src).flatMap { mounts =>
        manage.moveDir(src, dst, semantics).fold(
          onFileSystemError(mounts, _),
          κ(onFileSystemSuccess(mounts))
        ).join
      }
    }

    def mountMove(scenario: MoveScenario, semantics: MoveSemantics): manage.M[Unit] = {
      val destinationNonEmpty = scenario match {
        case MoveScenario.FileToFile(_, dst) => query.fileExists(dst)
        case MoveScenario.DirToDir(_, dst)   => query.ls(dst).run.map(_.toOption.map(_.nonEmpty).getOrElse(false))
      }
      EitherT((mount.exists(scenario.src) |@| mount.exists(scenario.dst) |@| destinationNonEmpty).tupled.flatMap {
        case (srcMountExists, dstMountExists, dstNonEmpty) => (semantics match {
          case FailIfExists if dstMountExists || dstNonEmpty =>
            pathErr(pathExists(scenario.dst)).raiseError[manage.M, Unit]

          case FailIfMissing if !(dstMountExists || dstNonEmpty) =>
            pathErr(pathNotFound(scenario.dst)).raiseError[manage.M, Unit]

          case _ if srcMountExists && dstNonEmpty =>
            // NB: We ignore the result of the filesystem delete as we're willing to
            //     shadow existing files if it fails for any reason.
            (manage.delete(scenario.dst).run *> overwriteMount(scenario.src, scenario.dst)).liftM[FileSystemErrT]

          case _ if srcMountExists && !dstNonEmpty =>
            overwriteMount(scenario.src, scenario.dst).liftM[FileSystemErrT]

          case _ =>
            manage.move(scenario, semantics)
        }).run
      })
    }

    λ[ManageFile ~> Free[S, ?]] {
      case Move(scenario, semantics) =>
        scenario.fold(
          (src, dst) => dirToDirMove(src, dst, semantics),
          (src, dst) => mountMove(MoveScenario.FileToFile(src, dst), semantics).run)

      case Delete(path) =>
        refineType(path).fold(
          d => mountsIn(d).map(paths => paths.map(d </> _))
            .flatMap(_.traverse_(deleteMount))
            .liftM[FileSystemErrT] *> manage.delete(d),

          f => mount.exists(f).liftM[FileSystemErrT].ifM(
            deleteMount(f).liftM[FileSystemErrT],
            manage.delete(f))
        ).run

      case TempFile(nearTo) =>
        manage.tempFile(nearTo).run
    }
  }

  /** Intercept and fail any write to a module path; all others are passed untouched. */
  def failSomeWrites[S[_]](on: AFile => Free[S, Boolean], message: String)(
                       implicit
                       S0: WriteFile :<: S,
                       S1: Mounting :<: S
                     ): WriteFile ~> Free[S, ?] = {
    import WriteFile._

    val writeUnsafe = WriteFile.Unsafe[S]
    val mount = Mounting.Ops[S]

    λ[WriteFile ~> Free[S, ?]] {
      case Open(file) =>
        on(file).ifM(
          pathErr(invalidPath(file, message)).left.point[Free[S, ?]],
          writeUnsafe.open(file).run)
      case Write(h, chunk) => writeUnsafe.write(h, chunk)
      case Close(h) => writeUnsafe.close(h)
    }
  }
}
