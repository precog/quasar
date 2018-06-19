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

package quasar.yggdrasil.vfs

import quasar.contrib.pathy.{ADir, AFile, APath, RPath}
import quasar.contrib.iota.{:<<:, ACopK}

import fs2.{Sink, Stream}

import scalaz.Free

import scodec.bits.ByteVector

import java.util.UUID

object POSIX {
  import POSIXOp._

  def genUUID[S[a] <: ACopK[a]](implicit S: POSIXOp :<<: S): Free[S, UUID] =
    Free.liftF(S.inj(GenUUID))

  def openW[S[a] <: ACopK[a]](target: AFile)(implicit S: POSIXOp :<<: S): Free[S, Sink[POSIXWithIO, ByteVector]] =
    Free.liftF(S.inj(OpenW(target)))

  def openR[S[a] <: ACopK[a]](target: AFile)(implicit S: POSIXOp :<<: S): Free[S, Stream[POSIXWithIO, ByteVector]] =
    Free.liftF(S.inj(OpenR(target)))

  def ls[S[a] <: ACopK[a]](target: ADir)(implicit S: POSIXOp :<<: S): Free[S, List[RPath]] =
    Free.liftF(S.inj(Ls(target)))

  def mkDir[S[a] <: ACopK[a]](target: ADir)(implicit S: POSIXOp :<<: S): Free[S, Unit] =
    Free.liftF(S.inj(MkDir(target)))

  def linkDir[S[a] <: ACopK[a]](src: ADir, target: ADir)(implicit S: POSIXOp :<<: S): Free[S, Boolean] =
    Free.liftF(S.inj(LinkDir(src, target)))

  def linkFile[S[a] <: ACopK[a]](src: AFile, target: AFile)(implicit S: POSIXOp :<<: S): Free[S, Boolean] =
    Free.liftF(S.inj(LinkFile(src, target)))

  def move[S[a] <: ACopK[a]](src: AFile, target: AFile)(implicit S: POSIXOp :<<: S): Free[S, Unit] =
    Free.liftF(S.inj(Move(src, target)))

  def exists[S[a] <: ACopK[a]](target: APath)(implicit S: POSIXOp :<<: S): Free[S, Boolean] =
    Free.liftF(S.inj(Exists(target)))

  def delete[S[a] <: ACopK[a]](target: APath)(implicit S: POSIXOp :<<: S): Free[S, Unit] =
    Free.liftF(S.inj(Delete(target)))
}
