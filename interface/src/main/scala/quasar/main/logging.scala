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

package quasar.main

import slamdata.Predef.{PartialFunction, Some, Unit}

import quasar.Planner, Planner.PlannerError
import quasar.connector.EnvironmentError
import quasar.effect.{Capture, Failure}
import quasar.fp.free
import quasar.fs.{FileSystemError, PhysicalError, UnhandledFSError}
import quasar.fs.mount.MountingError
import quasar.fs.mount.module.Module

import org.slf4s.Logger
import scalaz.{~>, :<:, Applicative, Free, Show}
import scalaz.syntax.apply._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.std.option._

object logging {
  def logFailure[E, F[_]: Applicative: Capture, S[_]](
      f: PartialFunction[E, Logger => Unit])(
      implicit
      E: Failure[E, ?] :<: S,
      F: F :<: S)
      : Logger => (Failure[E, ?] ~> Free[S, ?]) =
    log => λ[Failure[E, ?] ~> Free[S, ?]] {
      case Failure.Fail(e) =>
        free.lift(f.lift(e).traverse_(g => Capture[F].capture(g(log))))
          .into[S] *> Failure.Ops[E, S].fail(e)
    }

  def logPhysicalError[F[_]: Applicative: Capture, S[_]](
      logger: Logger)(
      implicit
      E: Failure[PhysicalError, ?] :<: S,
      F: F :<: S)
      : Failure[PhysicalError, ?] ~> Free[S, ?] =
    logFailure[PhysicalError, F, S] {
      case UnhandledFSError(e) =>
        _.error("Physical Error.", e)
    } apply logger

  def logFatalFileSystemError[F[_]: Applicative: Capture, S[_]](
      logger: Logger)(
      implicit
      E: Failure[FileSystemError, ?] :<: S,
      F: F :<: S)
      : Failure[FileSystemError, ?] ~> Free[S, ?] =
    logFailure[FileSystemError, F, S](logFileSystemError) apply logger

  def logFatalMountingError[F[_]: Applicative: Capture, S[_]](
      logger: Logger)(
      implicit
      E: Failure[MountingError, ?] :<: S,
      F: F :<: S)
      : Failure[MountingError, ?] ~> Free[S, ?] =
    logFailure[MountingError, F, S] {
      case MountingError.EError(EnvironmentError.ConnectionFailed(cause)) =>
        _.error("Connection failed.", cause)

      case MountingError.EError(ee) =>
        _.error(Show[EnvironmentError].shows(ee))

      case err @ MountingError.InvalidMount(_, _) =>
        _.error(Show[MountingError].shows(err))
    } apply logger

  def logFatalModuleError[F[_]: Applicative: Capture, S[_]](
      logger: Logger)(
      implicit
      E: Failure[Module.Error, ?] :<: S,
      F: F :<: S)
      : Failure[Module.Error, ?] ~> Free[S, ?] =
    logFailure[Module.Error, F, S] {
      case Module.Error.FSError(e) =>
        logFileSystemError.lift(e) getOrElse noop
    } apply logger

  ////

  private val noop: Logger => Unit = _ => ()

  private def logFileSystemError: PartialFunction[FileSystemError, Logger => Unit] = {
    case FileSystemError.PathErr(_) | FileSystemError.UnsupportedOperation(_) =>
      noop

    case FileSystemError.ExecutionFailed(_, rsn, _, Some(UnhandledFSError(e))) =>
      _.error(rsn, e)

    case FileSystemError.PlanningFailed(_, e) =>
      logPlannerError.lift(e) getOrElse noop

    case FileSystemError.QScriptPlanningFailed(e) =>
      logPlannerError.lift(e) getOrElse noop

    case other =>
      _.error(other.shows)
  }

  private def logPlannerError: PartialFunction[PlannerError, Logger => Unit] = {
    case Planner.InternalError(msg, cause) =>
      l => cause.fold(l.error(msg))(l.error(msg, _))
  }
}
