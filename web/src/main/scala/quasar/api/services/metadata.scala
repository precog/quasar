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

import slamdata.Predef.{ -> => _, _ }
import quasar.fp.ski._
import quasar.api._
import quasar.contrib.pathy._
import quasar.fs._
import quasar.fs.mount._, MountConfig.moduleConfig
import quasar.sql.FunctionDecl

import scala.math.Ordering

import argonaut._, Argonaut._
import org.http4s.dsl._
import pathy.Path._
import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

object metadata {

  final case class FsNode(name: String, typ: String, mount: Option[String], args: Option[List[String]])

  object FsNode {
    @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
    def apply(pathSegment: PathSegment, mount: Option[String], args: Option[List[String]]): FsNode =
      FsNode(
        pathSegment.fold(_.value, _.value),
        pathSegment.fold(κ("directory"), κ("file")),
        mount,
        args)

    @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
    def apply(pathSegment: PathSegment, mount: Option[String]): FsNode = apply(pathSegment, mount, args = None)

    implicit val fsNodeOrdering: Ordering[FsNode] =
      Ordering.by(n => (n.name, n.typ, n.mount, n.args.map(_.toIterable))) // toIterable is because scala.math.Ordering is defined for Iterable

    implicit val fsNodeOrder: Order[FsNode] =
      Order.fromScalaOrdering

    implicit val fsNodeEncodeJson: EncodeJson[FsNode] =
      EncodeJson { case FsNode(name, typ, mount, args) =>
        ("name" := name)    ->:
        ("type" := typ)     ->:
        ("mount" :=? mount) ->?:
        ("args" :=? args)   ->?:
        jEmptyObject
      }

    implicit val fsNodeDecodeJson: DecodeJson[FsNode] =
      jdecode4L(FsNode.apply)("name", "type", "mount", "args")
  }

  def service[S[_]](implicit Q: QueryFile.Ops[S], M: Mounting.Ops[S]): QHttpService[S] = {

    def mkNodes(parent: ADir, names: Set[PathSegment]): Q.M[Set[FsNode]] =
      // First we check if this directory is a module, if so, we return `FsNode` that
      // have the additional args field
      //
      // We go through the trouble of "overlaying" things here as opposed to just using the MountConfig
      // so that interpreters are free to modify the results of `QueryFile` and those changes will be
      // reflected here
      M.lookupConfig(parent).flatMap(c => OptionT(moduleConfig.getOption(c).point[M.FreeS])).map { statements =>
        val args = statements.collect { case FunctionDecl(name, args, _ ) => name.value -> args.map(_.value) }.toMap
        names.map { name =>
          FsNode(name, mount = None, args = args.get(stringValue(name)))
        }
      // Or Else we optionally associate some metadata to FsNode's if they happen to be some kind of mount
      }.getOrElseF {
        M.havingPrefix(parent).map { mounts =>
          names map { name =>
            val path = name.fold(parent </> dir1(_), parent </> file1(_))
            FsNode(name, mounts.get(path).map(_.fold(_.value, "view", "module")), args = None)
          }
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
