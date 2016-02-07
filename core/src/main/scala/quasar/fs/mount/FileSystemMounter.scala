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

package quasar.fs.mount

import quasar.Predef.{Unit, String}
import quasar.effect.AtomicRef
import quasar.fs.{ADir, FileSystemType, PathError2}
import quasar.fp.free
import quasar.fp.prism._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.either._

final class FileSystemMounter[F[_]](fsDef: FileSystemDef[F]) {
  import MountingError._, PathError2._, MountConfig2._, FileSystemDef._

  type MountedFs[A]  = AtomicRef[Mounts[DefinitionResult[F]], A]
  type MountedFsF[A] = Coyoneda[MountedFs, A]

  /** Attempts to mount a filesystem at the given location, using the provided
    * definition.
    */
  def mount[S[_]: Functor]
      (loc: ADir, typ: FileSystemType, uri: ConnectionUri)
      (implicit S0: F :<: S, S1: MountedFsF :<: S)
      : Free[S, MountingError \/ Unit] = {

    type M[A] = Free[S, A]

    val failUnlessCandidate: MntErrT[M, Unit] =
      EitherT[M, MountingError, Unit](mounts[S].get map { mnts =>
        mnts.candidacy(loc).leftMap(r => pathError(invalidPath(loc, r)))
      })

    val createFs: MntErrT[M, DefinitionResult[F]] =
      EitherT[M, DefinitionError, DefinitionResult[F]](
        free.lift(fsDef(typ, uri).run).into[S]).leftMap(_.fold(
          invalidConfig(fileSystemConfig(typ, uri), _),
          environmentError(_)))

    def addMount(fsr: DefinitionResult[F]): MntErrT[M, Unit] = {
      def cleanupOnError(err: String) =
        free.lift(fsr._2).into[S] as pathError(invalidPath(loc, err))

      EitherT[M, MountingError, Unit](mounts[S].modifyS(mnts =>
        mnts.add(loc, fsr).fold(
          err => (mnts, cleanupOnError(err) map (_.left[Unit])),
          nxt => (nxt, cleanup[S](mnts, loc) map (_.right[MountingError])))
      ).join)
    }

    (failUnlessCandidate *> createFs).flatMap(addMount).run
  }

  def unmount[S[_]: Functor]
      (loc: ADir)
      (implicit S0: F :<: S, S1: MountedFsF :<: S)
      : Free[S, Unit] = {

    mounts[S].modifyS(mnts => (mnts - loc, cleanup[S](mnts, loc))).join
  }

  ////

  private def cleanup[S[_]: Functor]
              (mnts: Mounts[DefinitionResult[F]], loc: ADir)
              (implicit S: F :<: S)
              : Free[S, Unit] = {

    mnts.lookup(loc).map(_._2)
      .fold(().point[Free[S, ?]])(free.lift(_).into[S])
  }

  private def mounts[S[_]: Functor](implicit S: MountedFsF :<: S) =
    AtomicRef.Ops[Mounts[DefinitionResult[F]], S]
}

object FileSystemMounter {
  def apply[F[_]](fsDef: FileSystemDef[F]): FileSystemMounter[F] =
    new FileSystemMounter[F](fsDef)
}
