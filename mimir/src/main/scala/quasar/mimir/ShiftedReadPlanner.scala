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
import quasar.precog.common.Path
import quasar.qscript._
import quasar.yggdrasil.bytecode.JType

import delorean._
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

import scala.concurrent.ExecutionContext.Implicits.global

final class ShiftedReadPlanner[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad, FS <: LightweightFileSystem](
    lift: FileSystemErrT[CakeM[FS, ?], ?] ~> F) {

  def plan: AlgebraM[F, Const[ShiftedRead[AFile], ?], MimirRepr] = {
    case Const(ShiftedRead(path, status)) => {
      type X[A] = EitherT[CakeM[FS, ?], FileSystemError, A]

      val pathStr: String = pathy.Path.posixCodec.printPath(path)

      val loaded: X[MimirRepr] =
        for {
          connectors <- cake[X, FS]
          (_, lwfs) = connectors

          repr <-
            MimirRepr.meld[X, FS](
              new DepFn1[Cake, λ[`P <: Cake` => X[P#Table]]] {
                def apply(P: Cake): X[P.Table] = {
                  // read, if it fails ingest and then read again
                  def read: EitherT[Task, FileSystemError, P.Table] =
                    P.Table.constString(Set(pathStr)).load(JType.JUniverseT).mapT(_.toTask).leftMap { err =>
                      val msg = err.messages.toList.reduce(_ + ";" + _)
                      FileSystemError.readFailed(posixCodec.printPath(path), msg)
                    }


                  val et = for {
                    precogRead <- read.run

                    et <- precogRead match {
                      case -\/(_) =>
                        lwfs.read(path) flatMap {
                          case Some(stream) =>
                            (P.ingest(Path(pathStr), stream.map(JValue.fromData)).run >> read.run): Task[FileSystemError \/ P.Table]
                          case None =>
                            Task.now(FileSystemError.readFailed(
                              posixCodec.printPath(path),
                              "read from lightweight connector failed").left[P.Table])
                        }

                      case right @ \/-(_) => Task.now(right: FileSystemError \/ P.Table)
                    }
                  } yield et

                  EitherT.eitherT(et.liftM[CakeMT[?[_], FS, ?]])// leftMap { err =>
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
