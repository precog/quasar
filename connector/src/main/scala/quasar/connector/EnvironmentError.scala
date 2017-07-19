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

package quasar.connector

import slamdata.Predef._

import argonaut._, Argonaut._
import monocle._
import scalaz._, Scalaz._

sealed abstract class EnvironmentError

object EnvironmentError {
  final case class ConnectionFailed(error: Throwable)
    extends EnvironmentError
  final case class InvalidCredentials(message: String)
    extends EnvironmentError
  final case class UnsupportedVersion(backendName: String, version: String)
    extends EnvironmentError

  val connectionFailed: Prism[EnvironmentError, Throwable] =
    Prism[EnvironmentError, Throwable] {
      case ConnectionFailed(err) => Some(err)
      case _ => None
    } (ConnectionFailed(_))

  val invalidCredentials: Prism[EnvironmentError, String] =
    Prism[EnvironmentError, String] {
      case InvalidCredentials(msg) => Some(msg)
      case _ => None
    } (InvalidCredentials(_))

  val unsupportedVersion: Prism[EnvironmentError, (String, String)] =
    Prism[EnvironmentError, (String, String)] {
      case UnsupportedVersion(name, version) => Some((name, version))
      case _ => None
    } ((UnsupportedVersion(_, _)).tupled)

  implicit val environmentErrorShow: Show[EnvironmentError] =
    Show.shows {
      case ConnectionFailed(err) =>
        s"Connection failed: ${summarize(err)}"
      case InvalidCredentials(msg) =>
        s"Invalid credentials: $msg"
      case UnsupportedVersion(name, version) =>
        s"Unsupported $name version: $version"
    }

  // Roll with universal equality for Throwable
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit val equal: Equal[EnvironmentError] = Equal.equal {
    case (ConnectionFailed(a), ConnectionFailed(b))           => a == b
    case (InvalidCredentials(a), InvalidCredentials(b))       => a ≟ b
    case (UnsupportedVersion(a, b), UnsupportedVersion(c, d)) => a ≟ c && b ≟ d
    case _                                                    => false
  }

  implicit val environmentErrorEncodeJson: EncodeJson[EnvironmentError] = {
    def format(message: String, detail: Option[String]) =
      Json(("error" := message) :: detail.toList.map("errorDetail" := _): _*)

    EncodeJson[EnvironmentError] {
      case ConnectionFailed(err) =>
        format("Connection failed.", Some(summarize(err)))
      case InvalidCredentials(msg) =>
        format("Invalid username and/or password specified.", Some(msg))
      case err =>
        format(err.shows, None)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def summarize(err: Throwable): String =
    err.toString +
      Option(err.getCause)
        .foldMap("; caused by: " + summarize(_))
}
