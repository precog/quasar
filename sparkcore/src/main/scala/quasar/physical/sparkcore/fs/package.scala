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

package quasar.physical.sparkcore

import quasar.Predef._
import quasar.fs._
import quasar.fs.mount._, FileSystemDef._

import org.apache.spark._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task


package object fs {

  final case class SparkFSDef[HS[_], S[_]](run: Free[HS, ?] ~> Free[S, ?], close: Free[S, Unit])

  def definition[HS[_],S[_], T](
    fsType: FileSystemType,
    parseUri: ConnectionUri => DefinitionError \/ (SparkConf, T),
    sparkFsDef: SparkConf => Free[S, SparkFSDef[HS, S]],
    fsInterpret: T => (FileSystem ~> Free[HS, ?])
  )(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (fsType, uri) =>
        for {
          config <- EitherT(parseUri(uri).point[Free[S, ?]])
          (sparkConf, t) = config
          res <- {
            sparkFsDef(sparkConf).map {
              case SparkFSDef(run, close) =>
                FileSystemDef.DefinitionResult[Free[S, ?]](
                  fsInterpret(t) andThen run,
                  close)
            }.liftM[DefErrT]
          }
        }  yield res
    }

}
