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

package quasar.fs.mount

import slamdata.Predef._
import quasar.fp.ski._
import quasar.fs.FileSystemType

import monocle.Prism
import scalaz._, Scalaz._

sealed abstract class MountType {
  import MountType._

  def fold[X](fs: FileSystemType => X, v: => X, m: => X): X =
    this match {
      case FileSystemMount(t) => fs(t)
      case ViewMount          => v
      case ModuleMount        => m
    }

  override def toString: String =
    this.shows
}

object MountType {
  final case class FileSystemMount(fsType: FileSystemType) extends MountType
  case object ViewMount extends MountType
  case object ModuleMount extends MountType

  val fileSystemMount: Prism[MountType, FileSystemType] =
    Prism.partial[MountType, FileSystemType] {
      case FileSystemMount(fs) => fs
    } (FileSystemMount)

  val viewMount: Prism[MountType, Unit] =
    Prism.partial[MountType, Unit] {
      case ViewMount => ()
    } (κ(ViewMount))

  val moduleMount: Prism[MountType, Unit] =
    Prism.partial[MountType, Unit] {
      case ModuleMount => ()
    } (κ(ModuleMount))

  implicit val order: Order[MountType] =
    Order.orderBy(mt => (viewMount.nonEmpty(mt), fileSystemMount.getOption(mt)))

  implicit val show: Show[MountType] =
    Show.shows(_.fold(t => s"FileSystemMount(${t.shows})", "ViewMount", "ModuleMount"))
}
