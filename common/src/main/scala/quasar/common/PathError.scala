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

package quasar.common

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fp.ski._

import monocle.{Lens, Prism}
import pathy.Path._
import scalaz._
import scalaz.std.string._
import scalaz.syntax.equal._

sealed abstract class PathError

object PathError {
  final case class PathExists private (path: APath)
      extends PathError
  final case class PathNotFound private (path: APath)
      extends PathError
  final case class InvalidPath private (path: APath, reason: String)
      extends PathError

  val pathExists =
    Prism.partial[PathError, APath] { case PathExists(p) => p } (PathExists)

  val pathNotFound =
    Prism.partial[PathError, APath] { case PathNotFound(p) => p } (PathNotFound)

  val invalidPath =
    Prism.partial[PathError, (APath, String)] {
      case InvalidPath(p, r) => (p, r)
    } (InvalidPath.tupled)

  val errorPath: Lens[PathError, APath] =
    Lens[PathError, APath] {
      case PathExists(p)     => p
      case PathNotFound(p)   => p
      case InvalidPath(p, _) => p
    } { p => {
      case PathExists(_)     => pathExists(p)
      case PathNotFound(_)   => pathNotFound(p)
      case InvalidPath(_, r) => invalidPath(p, r)
    }}

  implicit val pathErrorShow: Show[PathError] = {
    val typeStr: APath => String =
      p => refineType(p).fold(κ("Dir"), κ("File"))

    Show.shows {
      case PathExists(p) =>
        s"${typeStr(p)} already exists: ${posixCodec.printPath(p)}"

      case PathNotFound(p) =>
        s"${typeStr(p)} not found: ${posixCodec.printPath(p)}"

      case InvalidPath(p, r) =>
        s"${typeStr(p)} ${posixCodec.printPath(p)} is invalid: $r"
    }
  }

  implicit val equal: Equal[PathError] = Equal.equal {
    case (PathExists(a), PathExists(b))         => a ≟ b
    case (PathNotFound(a), PathNotFound(b))     => a ≟ b
    case (InvalidPath(a, b), InvalidPath(c, d)) => a ≟ c && b ≟ d
    case _                                      => false
  }
}
