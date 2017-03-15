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

package quasar.api.services

import quasar.Predef.{ -> => _, _ }
import quasar.fp.ski._
import quasar.api._
import quasar.contrib.pathy._
import quasar.fs._
import quasar.fs.mount._

import scala.math.Ordering

import argonaut._, Argonaut._
import org.http4s.dsl._
import pathy.Path._
import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

object metadata {

  final case class FsNode(name: String, typ: String, mount: Option[String])

  object FsNode {
    def apply(pathSegment: PathSegment, mount: Option[String]): FsNode =
      FsNode(
        pathSegment.fold(_.value, _.value),
        pathSegment.fold(κ("directory"), κ("file")),
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

  def service[S[_]](implicit Q: QueryFile.Ops[S], M: Mounting.Ops[S]): QHttpService[S] = {

    def mkNodes(parent: ADir, names: Set[PathSegment]): Q.M[Set[FsNode]] =
      M.havingPrefix(parent).map { mounts =>
        names map { name =>
          val path = name.fold(parent </> dir1(_), parent </> file1(_))
          FsNode(name, mounts.get(path).map(_.fold(_.value, "view", "module")))
        }
      }.liftM[FileSystemErrT]

    def dirMetadata(d: ADir): Free[S, QResponse[S]] = respond(
      Q.ls(d)
        .flatMap(mkNodes(d, _))
        .map(nodes => Json.obj("children" := nodes.toList.sorted))
        .run)

    def fileMetadata(f: AFile): Free[S, QResponse[S]] = respond(
      Q.fileExists(f)
        .map(_ either Json() or PathError.pathNotFound(f)))

    QHttpService {
      case GET -> AsPath(path) =>
        refineType(path).fold(dirMetadata, fileMetadata)
    }
  }
}
