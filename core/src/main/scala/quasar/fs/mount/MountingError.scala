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

import quasar.Predef._
import quasar.EnvironmentError2
import quasar.fs._

import monocle.Prism
import scalaz.NonEmptyList
import scalaz._, Scalaz._

sealed trait MountingError

object MountingError {
  final case class PathError private[mount] (err: PathError2)
    extends MountingError

  final case class EnvironmentError private[mount] (err: EnvironmentError2)
    extends MountingError

  final case class InvalidConfig private[mount] (config: MountConfig2, reasons: NonEmptyList[String])
    extends MountingError

  val pathError: Prism[MountingError, PathError2] =
    Prism[MountingError, PathError2] {
      case PathError(err) => Some(err)
      case _ => None
    } (PathError(_))

  val environmentError: Prism[MountingError, EnvironmentError2] =
    Prism[MountingError, EnvironmentError2] {
      case EnvironmentError(err) => Some(err)
      case _ => None
    } (EnvironmentError(_))

  val invalidConfig: Prism[MountingError, (MountConfig2, NonEmptyList[String])] =
    Prism[MountingError, (MountConfig2, NonEmptyList[String])] {
      case InvalidConfig(cfg, reasons) => Some((cfg, reasons))
      case _ => None
    } ((InvalidConfig(_, _)).tupled)

  implicit def mountingErrorShow: Show[MountingError] =
    Show.shows {
      case PathError(e) =>
        e.shows
      case EnvironmentError(e) =>
        e.shows
      case InvalidConfig(cfg, rsns) =>
        s"Invalid mount config, '${cfg.shows}', because: ${rsns.list.mkString("; ")}"
    }
}
