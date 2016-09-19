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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar.fs.QueryFile
import quasar.fs.QueryFile._
import quasar.fs._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fp.free._
import quasar.contrib.pathy._

import java.io.File
import java.nio.file._

import pathy.Path._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

object queryfile {

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: Task :<: S): QueryFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.queryFile[QueryFile](prefix)

  def interpret[S[_]](implicit
    s0: Task :<: S): QueryFile ~> Free[S, ?] =
    new (QueryFile ~> Free[S, ?]) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case FileExists(f) => fileExists(f)
        case ListContents(dir) => listContents(dir)
        case _ => ???
      }
    }

  private def fileExists[S[_]](f: AFile)(implicit
    s0: Task :<: S): Free[S, Boolean] =
    injectFT[Task, S].apply {
      Task.delay {
        Files.exists(Paths.get(posixCodec.unsafePrintPath(f)))
      }
    }

  private def listContents[S[_]](d: ADir)(implicit
    s0: Task :<: S): Free[S, FileSystemError \/ Set[PathSegment]] =
    injectFT[Task, S].apply {
      Task.delay {
        val directory = new File(posixCodec.unsafePrintPath(d))
        if(directory.exists()) {
          \/.fromTryCatchNonFatal{
            directory.listFiles.toSet[File].map {
              case file if file.isFile() => FileName(file.getName()).right[DirName]
              case directory => DirName(directory.getName()).left[FileName]
            }
          }
            .leftMap {
            case e =>
              pathErr(invalidPath(d, e.getMessage()))
          }
        } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
      }
    }
}
