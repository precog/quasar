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
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.physical.marklogic._

import argonaut._
import pathy.Path._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

object readfile {

  def toData(s: Json): Vector[Data] = Vector(DataCodec.Precise.decode(s).getOrElse(Data.Arr(Nil)))

  def interpret[S[_]](implicit
    S0:           Task :<: S,
    S1:           Read[Client, ?] :<: S,
    state:        KeyValueStore.Ops[ReadFile.ReadHandle, Process[Task, Vector[Data]], S],
    seq:          MonotonicSeq.Ops[S]
  ): ReadFile ~> Free[S,?] =
    impl.readFromProcess( (file, readOpts) =>
      Client.readDocument(posixCodec.printPath(file)).map(_.bimap(
        _ => pathErr(pathNotFound(file)), // Convert the markLogic "not found" error into our own
        _.map(toData)))) // Convert the stream of JSON into quasar.Data

}
