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

package quasar
package fs

import quasar.Predef._
import quasar.fp._

import monocle.Lens
import pathy.Path._
import scalaz._

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2

object PathError2 {
  final case class PathExists private (path: APath)
      extends PathError2
  final case class PathNotFound private (path: APath)
      extends PathError2
  final case class InvalidPath private (path: APath, reason: String)
      extends PathError2

  val pathExists =
    pPrism[PathError2, APath] { case PathExists(p) => p } (PathExists)

  val pathNotFound =
    pPrism[PathError2, APath] { case PathNotFound(p) => p } (PathNotFound)

  val invalidPath =
    pPrism[PathError2, (APath, String)] {
      case InvalidPath(p, r) => (p, r)
    } (InvalidPath.tupled)

  val errorPath: Lens[PathError2, APath] =
    Lens[PathError2, APath] {
      case PathExists(p)     => p
      case PathNotFound(p)   => p
      case InvalidPath(p, _) => p
    } { p => {
      case PathExists(_)     => pathExists(p)
      case PathNotFound(_)   => pathNotFound(p)
      case InvalidPath(_, r) => invalidPath(p, r)
    }}

  implicit val pathErrorShow: Show[PathError2] = {
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
}
