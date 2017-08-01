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
import quasar.contrib.std._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._, MountConfig.moduleConfig
import quasar.sql.FunctionDecl

import argonaut._, Argonaut._, EncodeJsonScalaz._
import org.http4s.dsl._
import pathy.Path._
import scalaz._, Scalaz._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object metadata {
  import ToApiError.ops._

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

    implicit val fsNodeOrder: Order[FsNode] =
      Order.orderBy(n => (n.name, n.typ, n.mount, n.args))

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

  final case class InvalidMountNode(name: String, `type`: Option[String], error: MountingError)

  object InvalidMountNode {
    import MountingError._

    implicit val invalidMountNodeOrder: Order[InvalidMountNode] =
      Order.orderBy(n => (n.name, n.`type`, n.error.toApiError.status, n.error.shows))

    implicit val invalidMountNodeEncodeJson: EncodeJson[InvalidMountNode] =
      EncodeJson { case InvalidMountNode(name, tpe, error) =>
        ("name" := name)                               ->:
        ("type" :=? tpe)                               ->?:
        ("mount" := Json("error" := error.toApiError)) ->:
        jEmptyObject
      }
  }

  implicit def invalidMountNodeFsNodeEncodeJson: EncodeJson[InvalidMountNode \/ FsNode] =
    EncodeJson(_.fold(
      InvalidMountNode.invalidMountNodeEncodeJson(_),
      FsNode.fsNodeEncodeJson(_)))

  def service[S[_]](implicit Q: QueryFile.Ops[S], M: Mounting.Ops[S]): QHttpService[S] = {

    def mkNodes(parent: ADir, names: Set[PathSegment]): Q.M[Set[InvalidMountNode \/ FsNode]] =
      // First we check if this directory is a module, if so, we return `FsNode` that
      // have the additional args field
      //
      // We go through the trouble of "overlaying" things here as opposed to just using the MountConfig
      // so that interpreters are free to modify the results of `QueryFile` and those changes will be
      // reflected here
      M.lookupConfig(parent).run
        .flatMap(i => OptionT((i.toOption >>= (c => moduleConfig.getOption(c))).point[M.FreeS]))
        .map { statements =>
          val args = statements.collect {
            case FunctionDecl(name, args, _ ) => name.value -> args.map(_.value)
          }.toMap

          names.map(name => FsNode(name, mount = None, args = args.get(stringValue(name))).right[InvalidMountNode])
        }
        // Or Else we optionally associate some metadata to FsNode's if they happen to be some kind of mount
        .getOrElseF {
          M.havingPrefix(parent).map { mounts =>
            names map { name =>
              val path = name.fold(parent </> dir1(_), parent </> file1(_))
              mounts.get(path).cata(
                _.bimap(
                  e => InvalidMountNode(
                    name.fold(_.value, _.value),
                    MountingError.invalidMount.getOption(e) ∘ (_._1.fold(_.value, "view", "module")),
                    e),
                  t => FsNode(name, t.fold(_.value, "view", "module").some, none)),
                FsNode(name, none, none).right)
            }
          }
        }.liftM[FileSystemErrT]

    def dirMetadata(d: ADir, offset: Natural, limit: Option[Positive]): Free[S, QResponse[S]] = respond(
      (offset.value.toIntSafe.toRightDisjunction(ApiError.fromMsg(BadRequest, "offset value is too large")) |@|
       limit.traverse(_.value.toIntSafe.toRightDisjunction(ApiError.fromMsg(BadRequest, "limit value is too large")))) { (off, lim) =>
        Q.ls(d)
          .flatMap(mkNodes(d, _))
          .map { nodes =>
            val withOffset = nodes.toIList.sorted.drop(off)
            Json.obj("children" := lim.fold(withOffset)(l => withOffset.take(l)))
          }.run
      }.sequence)

    def fileMetadata(f: AFile): Free[S, QResponse[S]] = respond(
      Q.fileExists(f)
        .map(_ either Json() or PathError.pathNotFound(f)))

    QHttpService {
      case GET -> AsPath(path) :? Offset(offsetParam) +& Limit(limitParam) =>
        respond((offsetOrInvalid(offsetParam) |@| limitOrInvalid(limitParam)) { (offset, limit) =>
          refineType(path).fold(dirMetadata(_, offset, limit), fileMetadata)
        }.sequence)
    }
  }
}
