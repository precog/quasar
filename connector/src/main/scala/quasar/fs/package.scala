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

package quasar

import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadError_
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.free._
import quasar.fp.ski._

import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

package object fs extends PhysicalErrorPrisms {
  type FileSystem[A] = (QueryFile :\: ReadFile :\: WriteFile :/: ManageFile)#M[A]

  type BackendEffect[A] = Coproduct[Analyze, FileSystem, A]

  type FileSystemFailure[A] = Failure[FileSystemError, A]
  type FileSystemErrT[F[_], A] = EitherT[F, FileSystemError, A]

  type MonadFsErr[F[_]] = MonadError_[F, FileSystemError]

  object MonadFsErr {
    def apply[F[_]](implicit F: MonadFsErr[F]): MonadFsErr[F] = F
  }

  sealed trait Node {
    def segment: PathSegment
    def `type`: Node.Type
  }
  sealed trait FileNode extends Node {
    def name: FileName
    def segment: PathSegment = name.right
  }
  sealed trait DirNode  extends Node {
    def name: DirName
    def segment: PathSegment = name.left
  }
  object Node {

    sealed trait Type
    case object View        extends Type
    case object Function    extends Type
    case object Data        extends Type
    case object Module      extends Type
    case object ImplicitDir extends Type

    final case class View(name: FileName) extends FileNode {
      override def `type` = View
    }
    final case class Function(name: FileName) extends FileNode {
      override def `type` = Function
    }
    final case class Data(name: FileName) extends FileNode {
      override def `type` = Data
    }
    final case class Module(name: DirName) extends DirNode {
      override def `type` = Module
    }
    final case class ImplicitDir(name: DirName) extends DirNode {
      override def `type` = ImplicitDir
    }

    /** Converts the `PathSegment` into a `ImplicitDir` `Node` if it's a directory path segment and
      * a `Data` `Node` if it's a file path segment
      */
    def fromSegment(p: PathSegment): Node =
      p.fold[Node](ImplicitDir(_), Data(_))

    implicit val equals: Equal[Node] = Equal.equalA
  }

  type PhysErr[A] = Failure[PhysicalError, A]

  def interpretFileSystem[M[_]](
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    q :+: r :+: w :+: m

  def interpretBackendEffect[M[_]](
    a: Analyze ~> M,
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): BackendEffect ~> M =
    a :+: q :+: r :+: w :+: m

  def nearDir(path: APath): ADir =
    refineType(path).fold(ι, fileParent)

}
