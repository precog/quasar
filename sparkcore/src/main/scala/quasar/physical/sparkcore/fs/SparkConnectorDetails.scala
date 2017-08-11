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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.fp.free._
import quasar.fs._

import org.apache.spark.rdd.RDD
import scalaz._

trait SparkConnectorDetails[A]

object SparkConnectorDetails {

  final case class FileExists(afile: AFile) extends SparkConnectorDetails[Boolean]
  final case object ReadChunkSize extends SparkConnectorDetails[Int]
  final case class StoreData(rdd: RDD[Data], out: AFile) extends SparkConnectorDetails[Unit]
  final case class ListContents(dir: ADir) extends SparkConnectorDetails[FileSystemError \/ Set[PathSegment]]

  class Ops[S[_]](implicit S: SparkConnectorDetails :<: S) {
    def fileExists(afile: AFile): Free[S, Boolean] =
      lift(FileExists(afile)).into[S]
    def readChunkSize: Free[S, Int] =
      lift(ReadChunkSize).into[S]
    def storeData(rdd: RDD[Data], out: AFile): Free[S, Unit] =
      lift(StoreData(rdd, out)).into[S]
    def listContents(dir: ADir): EitherT[Free[S, ?], FileSystemError, Set[PathSegment]] =
      EitherT(lift(ListContents(dir)).into[S])
  }

  object Ops {
    implicit def apply[S[_]](implicit S: SparkConnectorDetails :<: S): Ops[S] =
      new Ops[S]
  }


}
