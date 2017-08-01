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

import slamdata.Predef.{Unit, String}
import quasar.contrib.pathy.ADir
import quasar.fp.numeric._
import quasar.effect.AtomicRef
import quasar.fs.{FileSystemType, PathError}
import quasar.fp.free

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.either._

final class FileSystemMountHandler[F[_]](fsDef: BackendDef[F]) {
  import MountingError._, PathError._, MountConfig._, BackendDef._

  type MountedFsRef[A] = AtomicRef[Mounts[DefinitionResult[F]], A]

  object MountedFsRef {
    def Ops[S[_]](implicit S: MountedFsRef :<: S) =
      AtomicRef.Ops[Mounts[DefinitionResult[F]], S]
  }

  /** Attempts to mount a filesystem at the given location, using the provided
    * definition.
    */
  def mount[S[_]]
      (loc: ADir, typ: FileSystemType, uri: ConnectionUri)
      (implicit S0: F :<: S, S1: MountedFsRef :<: S, F: Monad[F])
      : Free[S, MountingError \/ Unit] = {

    type M[A] = Free[S, A]

    val failUnlessCandidate: MntErrT[M, Unit] =
      EitherT[M, MountingError, Unit](MountedFsRef.Ops[S].get map { mnts =>
        mnts.candidacy(loc).leftMap(r => pathError(invalidPath(loc, r)))
      })

    val createFs: MntErrT[M, DefinitionResult[F]] =
      EitherT[M, DefinitionError, DefinitionResult[F]](
        free.lift(fsDef(typ, uri).run).into[S]
      ).leftMap(_.fold(
        invalidConfig(fileSystemConfig(typ, uri), _),
        environmentError(_)))

    def addMount(fsr: DefinitionResult[F]): MntErrT[M, Unit] = {
      def cleanupOnError(err: String) =
        free.lift(fsr.close).into[S] as pathError(invalidPath(loc, err))

      EitherT[M, MountingError, Unit](MountedFsRef.Ops[S].modifyS(mnts =>
        mnts.add(loc, fsr).fold(
          err => (mnts, cleanupOnError(err) map (_.left[Unit])),
          nxt => (nxt, cleanup[S](mnts, loc) map (_.right[MountingError])))
      ).join)
    }

    (failUnlessCandidate *> createFs).flatMap(addMount).run
  }

  def unmount[S[_]]
      (loc: ADir)
      (implicit S0: F :<: S, S1: MountedFsRef :<: S)
      : Free[S, Unit] = {

    MountedFsRef.Ops[S].modifyS(mnts => (mnts - loc, cleanup[S](mnts, loc))).join
  }

  ////

  private def cleanup[S[_]]
              (mnts: Mounts[DefinitionResult[F]], loc: ADir)
              (implicit S: F :<: S)
              : Free[S, Unit] = {

    mnts.lookup(loc).map(_.close)
      .fold(().point[Free[S, ?]])(free.lift(_).into[S])
  }
}

object FileSystemMountHandler {
  def apply[F[_]](fsDef: BackendDef[F]): FileSystemMountHandler[F] =
    new FileSystemMountHandler[F](fsDef)
}
