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

import fs2.{Stream, Sink}

import scodec.bits.ByteVector

import java.util.UUID

sealed trait POSIXOp[A] extends Product with Serializable

object POSIXOp {

  case object GenUUID extends POSIXOp[UUID]

  final case class OpenW(target: AFile) extends POSIXOp[Sink[POSIXWithIO, ByteVector]]
  final case class OpenR(target: AFile) extends POSIXOp[Stream[POSIXWithIO, ByteVector]]

  final case class Ls(target: ADir) extends POSIXOp[List[RPath]]

  final case class MkDir(target: ADir) extends POSIXOp[Unit]

  final case class LinkDir(src: ADir, target: ADir) extends POSIXOp[Boolean]
  final case class LinkFile(src: AFile, target: AFile) extends POSIXOp[Boolean]

  final case class Move(src: AFile, target: AFile) extends POSIXOp[Unit]
  final case class Exists(target: APath) extends POSIXOp[Boolean]
  final case class Delete(target: APath) extends POSIXOp[Unit]
}
