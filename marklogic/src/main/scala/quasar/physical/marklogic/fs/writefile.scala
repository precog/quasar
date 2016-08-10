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
import quasar._
import quasar.fs._
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.effect.{Read, KeyValueStore, MonotonicSeq}
import quasar.physical.marklogic._
import quasar.physical.marklogic.WriteError._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {

  def asString(data: Vector[Data]): String = ???

  def interpret[S[_]](implicit
    S0:      Task :<: S,
    client:  Read.Ops[Client,S],
    cursors: KeyValueStore.Ops[WriteFile.WriteHandle, Unit, S],
    seq:     MonotonicSeq.Ops[S]
   ): WriteFile ~> Free[S,?] = new (WriteFile ~> Free[S, ?]) {
    def apply[A](fa: WriteFile[A]): Free[S, A] = fa match {
      case WriteFile.Open(file) =>
        Client.exists(posixCodec.printPath(file)).ifM(
          for {
            id <- seq.next
            writeHandle = WriteFile.WriteHandle(file, id)
            _ <- cursors.put(writeHandle, ())
          } yield writeHandle.right,
          pathErr(pathNotFound(file)).left.pure[Free[S,?]])

      case WriteFile.Write(h, data) =>
        cursors.get(h).isDefined.ifM(
          Client.write(posixCodec.printPath(h.file), asString(data)).map( errors =>
            errors.map {
              case ResourceNotFound(msg) => pathErr(pathNotFound(h.file))
              case Forbidden(msg)        => writeFailed(Data.Arr(data.toList), "Quasar is not authorized to perform this operation on the MarkLogic server")
              case FailedRequest(msg)    => writeFailed(Data.Arr(data.toList), "An unknown error occured at the MarkLogic REST API level")
            }.toList.toVector),
          Vector.empty[FileSystemError].pure[Free[S,?]])

      case WriteFile.Close(h) =>
        cursors.delete(h)
    }
  }
}
