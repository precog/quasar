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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.free.lift
import quasar.fp.numeric.Positive
import quasar.fs._, FileSystemError.pathErr, PathError.pathNotFound
import quasar.fs.impl._
import quasar.physical.marklogic.xcc._

import scalaz._, Scalaz._

object readfile {

  def interpret[S[_]](
    chunkSize: Positive
  )(
    implicit
    S0:    ContentSourceIO :<: S,
    state: KeyValueStore.Ops[ReadFile.ReadHandle, ReadStream[ContentSourceIO], S],
    seq:   MonotonicSeq.Ops[S]
  ): ReadFile ~> Free[S, ?] = {
    def dataProcess(file: AFile, skip: Int, limit: Option[Int]): ReadStream[ContentSourceIO] = {
      val ltd = ops.readFile(file).drop(skip)

      limit.fold(ltd)(ltd.take)
        .chunk(chunkSize.get.toInt)
        .map(_.right[FileSystemError])
    }

    readFromProcess { (file, readOpts) =>
      lift(ContentSourceIO.runSessionIO(ops.exists(file)) map { doesExist =>
        if (doesExist)
          dataProcess(file, readOpts.offset.get.toInt, readOpts.limit.map(_.get.toInt))
            .right[FileSystemError]
        else
          pathErr(pathNotFound(file)).left[ReadStream[ContentSourceIO]]
      }).into[S]
    }
  }
}
