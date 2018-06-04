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

package quasar.mimir

import slamdata.Predef._

import quasar.blueeyes.json.JValue
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fs._
import quasar.mimir.MimirCake._
import quasar.precog.common.RValue
import quasar.qscript._
import quasar.yggdrasil.TransSpecModule.paths.{Key, Value}
import quasar.yggdrasil.bytecode.JType

import fs2.interop.scalaz._
import io.chrisdavenport.scalaz.task._
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class ShiftedReadPlanner[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad](
    lift: FileSystemErrT[CakeM, ?] ~> F) {

  def plan: AlgebraM[F, Const[ShiftedRead[AFile], ?], MimirRepr] = {
    case Const(ShiftedRead(path, status)) => {
      type X[A] = EitherT[CakeM, FileSystemError, A]

      val pathStr: String = pathy.Path.posixCodec.printPath(path)

      val loaded: X[MimirRepr] =
        for {
          connectors <- cake[X]
          (_, lwfs) = connectors

          repr <- MimirRepr.meld[X, LightweightFileSystem](
            new DepFn1[Cake, λ[`P <: Cake` => X[P#Table]]] {
              def apply(P: Cake): X[P.Table] = {

                val read: EitherT[Task, FileSystemError, P.Table] =
                  P.Table.constString(Set(pathStr))
                    .load(JType.JUniverseT)
                    .mapT(_.to[Task])
                    .leftMap { err =>
                      val msg = err.messages.toList.reduce(_ + ";" + _)
                      FileSystemError.readFailed(posixCodec.printPath(path), msg)
                    }

                val et: Task[FileSystemError \/ P.Table] = for {
                  precogRead <- read.run

                  et <- precogRead match {
                    // read from mimir
                    case right @ \/-(_) =>
                      Task.now(right: FileSystemError \/ P.Table)

                    // read from the lwc
                    case -\/(_) =>
                      lwfs.read(path) flatMap {
                        case Some(stream) => for {
                          // FIXME this pages the entire fs2.Stream into memory
                          values <- stream
                            .map(data => RValue.fromJValueRaw(JValue.fromData(data)))
                            .runLog
                          } yield {
                            import P.trans._

                            // TODO depending on the id status we may not need to wrap the table
                            P.Table.fromRValues(values.toVector)
                              .transform(OuterObjectConcat(
                                WrapObject(
                                  Scan(Leaf(Source), P.freshIdScanner),
                                  Key.name),
                                WrapObject(
                                  Leaf(Source),
                                  Value.name))).right[FileSystemError]
                          }

                        case None =>
                          Task.now(FileSystemError.readFailed(
                            posixCodec.printPath(path),
                            "read from lightweight connector failed").left[P.Table])
                      }
                  }
                } yield et

                EitherT.eitherT(et.liftM[CakeMT[?[_], ?]])
              }
            })
        } yield {
          import repr.P.trans._

          status match {
            case IdOnly =>
              repr.map(_.transform(constants.SourceKey.Single))

            case IncludeId =>
              val ids = constants.SourceKey.Single
              val values = constants.SourceValue.Single

              // note that ids are already an array
              repr.map(_.transform(InnerArrayConcat(ids, WrapArray(values))))

            case ExcludeId =>
              repr.map(_.transform(constants.SourceValue.Single))
          }
        }

      lift(loaded)
    }
  }
}
