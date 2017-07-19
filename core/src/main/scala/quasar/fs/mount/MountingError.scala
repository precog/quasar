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
import quasar.connector.EnvironmentError
import quasar.fs._

import monocle.Prism
import scalaz.NonEmptyList
import scalaz._, Scalaz._

sealed abstract class MountingError

object MountingError {
  final case class PError private (err: PathError)
    extends MountingError

  final case class EError private (err: EnvironmentError)
    extends MountingError

  final case class InvalidConfig private (config: MountConfig, reasons: NonEmptyList[String])
    extends MountingError

  final case class InvalidMount private (`type`: MountType, error: String)
      extends MountingError

  val pathError: Prism[MountingError, PathError] =
    Prism.partial[MountingError, PathError] {
      case PError(err) => err
    } (PError)

  val environmentError: Prism[MountingError, EnvironmentError] =
    Prism.partial[MountingError, EnvironmentError] {
      case EError(err) => err
    } (EError)

  val invalidConfig: Prism[MountingError, (MountConfig, NonEmptyList[String])] =
    Prism.partial[MountingError, (MountConfig, NonEmptyList[String])] {
      case InvalidConfig(cfg, reasons) => (cfg, reasons)
    } (InvalidConfig.tupled)

  val invalidMount: Prism[MountingError, (MountType, String)] =
    Prism.partial[MountingError, (MountType, String)] {
      case InvalidMount(t, e) => (t, e)
    } (InvalidMount.tupled)

  implicit val mountingErrorShow: Show[MountingError] =
    Show.shows {
      case PError(e) =>
        e.shows
      case EError(e) =>
        e.shows
      case InvalidConfig(cfg, rsns) =>
        s"Invalid mount config, '${cfg.shows}', because: ${rsns.list.toList.mkString("; ")}"
      case InvalidMount(t, e) =>
        s"Invalid ${t.shows}, because: $e"
    }

  implicit val equal: Equal[MountingError] = Equal.equal {
    case (PError(a), PError(b))                     => a ≟ b
    case (EError(a), EError(b))                     => a ≟ b
    case (InvalidConfig(a, b), InvalidConfig(c, d)) => a ≟ c && b ≟ d
    case (InvalidMount(a, b), InvalidMount(c, d))   => a ≟ c && b ≟ d
    case _                                          => false
  }

}
