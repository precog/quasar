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

package quasar.yggdrasil.vfs

import quasar.precog.common.Path
import quasar.yggdrasil._
import quasar.yggdrasil.vfs.ResourceError._

import java.io.{File, FileFilter}

import org.slf4s.Logging

import org.apache.commons.io.filefilter.FileFilterUtils

import scalaz._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.syntax.traverse._

object VFSPathUtils extends Logging {
  // Methods for dealing with path escapes, lookup, enumeration
  private final val disallowedPathComponents = Set(".", "..")

  // For a given path directory, this subdir holds the full set of version dirs
  private final val versionsSubdir = "pathVersions"

  // This is the previous "hidden" projection dir. Needed to filter in case of symlinking
  private final val perAuthProjectionsDir = "perAuthProjections"

  private[yggdrasil] final val escapeSuffix = "_byUser"

  private final val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    and(not(nameFileFilter(versionsSubdir)), not(nameFileFilter(perAuthProjectionsDir)))
  }

  def escapePath(path: Path, toEscape: Set[String]) =
    Path(path.elements.map {
      case needsEscape if toEscape.contains(needsEscape) || needsEscape.endsWith(escapeSuffix) =>
        needsEscape + escapeSuffix
      case fine => fine
    }.toList)

  def unescapePath(path: Path) =
    Path(path.elements.map {
      case escaped if escaped.endsWith(escapeSuffix) =>
        escaped.substring(0, escaped.length - escapeSuffix.length)
      case fine => fine
    }.toList)

  /**
    * Computes the stable path for a given vfs path relative to the given base dir. Version subdirs
    * for the given path will reside under this directory
    */
  def pathDir(baseDir: File, path: Path): File = {
    // The path component maps directly to the FS
    val prefix = escapePath(path, Set(versionsSubdir)).elements.filterNot(disallowedPathComponents)
    new File(baseDir, prefix.mkString(File.separator))
  }

  def versionsSubdir(pathDir: File): File = new File(pathDir, versionsSubdir)

  def findChildren(baseDir: File, path: Path): IO[Set[PathMetadata]] = {
    val pathRoot = pathDir(baseDir, path)

    log.debug(s"Checking for children of path $path in dir $pathRoot")

    Option(pathRoot.listFiles(pathFileFilter)) map { files =>
      log.debug("Filtering children %s in path %s".format(files.mkString("[", ", ", "]"), path))

      val childMetadata: IO[List[Option[PathMetadata]]] = files.toList traverse { f =>
        val childPath = unescapePath(path / Path(f.getName))
        currentPathMetadata(baseDir, childPath).fold[Option[PathMetadata]](
          {
            case NotFound(message) =>
              log.trace(s"No child data found for ${childPath.path}: $message")
              None
            case error =>
              log.error("Encountered corruption or error searching child paths: %s".format(error.messages.list.toList.mkString("; ")))
              None
          },
          pathMetadata => Some(pathMetadata))
      }

      childMetadata.map(_.flatten.toSet): IO[Set[PathMetadata]]
    } getOrElse {
      log.debug("Path dir %s for path %s is not a directory!".format(pathRoot, path))
      IO(Set.empty)
    }
  }

  def currentPathMetadata(baseDir: File, path: Path): EitherT[IO, ResourceError, PathMetadata] = {
    def containsNonemptyChild: IO[Boolean] =
      findChildren(baseDir, path) flatMap { children =>
        IO(children.nonEmpty)
      }

    val pathDir0 = pathDir(baseDir, path)

    EitherT {
      IO(pathDir0.isDirectory) flatMap {
        case true =>
          VersionLog.currentVersionEntry(pathDir0).run flatMap { currentVersionV =>
            currentVersionV.fold[IO[ResourceError \/ PathMetadata]](
              {
                case NotFound(message) =>
                  containsNonemptyChild map {
                    case true =>
                      \/.right(PathMetadata(path, PathMetadata.PathOnly))
                    case false =>
                      \/.left(NotFound("All subpaths of %s appear to be empty.".format(path.path)))
                  }

                case otherError =>
                  IO(\/.left(otherError))
              },
              {
                case VersionEntry(uuid, dataType, timestamp) =>
                  log.debug(s"Got a version entry for $pathDir0.")
                  containsNonemptyChild map {
                    case true => \/.right(PathMetadata(path, PathMetadata.DataDir(dataType.contentType)))
                    case false => \/.right(PathMetadata(path, PathMetadata.DataOnly(dataType.contentType)))
                  }
              })
          }

        case false =>
          IO(\/.left(NotFound("No data found at path %s".format(path.path))))
      }
    }
  }
}
