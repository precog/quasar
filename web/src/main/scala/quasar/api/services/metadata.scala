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

package quasar.api.services

import quasar.Predef._
import quasar.SKI._
import quasar.api.AsPath
import quasar.fs._
import quasar.fs.mount._

import scala.math.Ordering

import argonaut._, Argonaut._
import org.http4s._, argonaut._, dsl._
import org.http4s.server.HttpService
import pathy.Path._
import scalaz._, concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.std.list._

object metadata {
  import MountConfig2._

  final case class FsNode(name: String, typ: String, mount: Option[String])

  object FsNode {
    def apply(pathName: PathName, mount: Option[String]): FsNode =
      FsNode(
        pathName.fold(_.value, _.value),
        pathName.fold(κ("directory"), κ("file")),
        mount)

    implicit val fsNodeOrdering: Ordering[FsNode] =
      Ordering.by(n => (n.name, n.typ, n.mount))

    implicit val fsNodeOrder: Order[FsNode] =
      Order.fromScalaOrdering

    implicit val fsNodeEncodeJson: EncodeJson[FsNode] =
      EncodeJson { case FsNode(name, typ, mount) =>
        ("name" := name)    ->:
        ("type" := typ)     ->:
        ("mount" :=? mount) ->?:
        jEmptyObject
      }

    implicit val fsNodeDecodeJson: DecodeJson[FsNode] =
      jdecode3L(FsNode.apply)("name", "type", "mount")
  }

  def service[S[_]: Functor](f: S ~> Task)(implicit Q: QueryFile.Ops[S], M: Mounting.Ops[S]): HttpService = {
    val mountType: MountConfig2 => String = {
      case ViewConfig(_, _)         => "view"
      case FileSystemConfig(typ, _) => typ.value
    }

    def mkNode(parent: ADir, name: PathName): Q.M[FsNode] =
      M.lookup(parent </> name.fold(dir1, file1))
        .run.map(cfg => FsNode(name, cfg map mountType))
        .liftM[FileSystemErrT]

    def dirMetadata(d: ADir): Q.F[Task[Response]] =
      Q.ls(d).flatMap(_.toList.traverse(mkNode(d, _))).fold(
        fileSystemErrorResponse,
        nodes => Ok(Json.obj("children" := nodes.sorted)))

    def fileMetadata(f: AFile): Q.F[Task[Response]] =
      Q.fileExists(f).fold(
        fileSystemErrorResponse,
        _ ? Ok(Json.obj()) | NotFound(Json("error" := s"File not found: ${posixCodec.printPath(f)}")))

    HttpService {
      case GET -> AsPath(path) =>
        refineType(path).fold(dirMetadata, fileMetadata).foldMap(f).join
    }
  }
}
