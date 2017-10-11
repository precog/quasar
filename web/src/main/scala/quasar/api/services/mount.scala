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
import quasar.api._, ToApiError.ops._
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, Timing}
import quasar.fp._, ski._
import quasar.fs.mount._
import quasar.fs.mount.cache.{VCache, ViewCache}
import quasar.fs.ManageFile

import argonaut._, Argonaut._
import org.http4s._, Method.MOVE
import org.http4s.dsl._
import org.http4s.CacheDirective.`max-age`
import org.http4s.headers.`Cache-Control`
import pathy.Path, Path._
import pathy.argonaut.PosixCodecJson._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object mount {
  import posixCodec.printPath

  def service[S[_]](
    implicit
    M: Mounting.Ops[S],
    S0: Task :<: S,
    S1: ManageFile :<: S,
    S2: MountingFailure :<: S,
    S3: PathMismatchFailure :<: S,
    S4: VCache :<: S,
    S5: Timing :<: S
  ): QHttpService[S] =
    QHttpService {
      case GET -> AsPath(path) =>
        def err = ApiError.fromMsg(
          NotFound withReason "Mount point not found.",
          s"There is no mount point at ${printPath(path)}",
          "path" := path)
        respondT(EitherT(
          M.lookupConfig(path).run.run ∘ (_ \/> (err) >>= (_
            leftMap(ToApiError[MountingError].toApiError)))))

      case req @ MOVE -> AsPath(src) =>
        respondT(requiredHeader(Destination, req).map(_.value).fold(
          _.raiseError[ApiErrT[M.FreeS, ?], String],
          dst => refineType(src).fold(
            srcDir  => move[S, Dir](srcDir, dst, UriPathCodec.parseAbsDir,  "directory"),
            srcFile => move[S, File](srcFile, dst, UriPathCodec.parseAbsFile, "file"))))

      case req @ POST -> AsDirPath(parent) =>
        respondT(for {
          hdr <- EitherT.fromDisjunction[M.FreeS](requiredHeader(XFileName, req))
          fn  =  hdr.value
          dst <- EitherT.fromDisjunction[M.FreeS](
                  (UriPathCodec.parseRelDir(fn) orElse UriPathCodec.parseRelFile(fn))
                    .flatMap(sandbox(currentDir, _))
                    .map(parent </> _)
                    .toRightDisjunction(ApiError.apiError(
                      BadRequest withReason "Filename must be a relative path.",
                      "fileName" := fn)))
          _   <- mount[S](dst, req, replaceIfExists = false)
        } yield s"added ${printPath(dst)}")

      case req @ PUT -> AsPath(path) =>
        respondT(mount[S](path, req, replaceIfExists = true) map { upd =>
          s"${upd ? "updated" | "added"} ${printPath(path)}"
        })

      case DELETE -> AsPath(p) =>
        val deleteViewIfExists: OptionT[Free[S, ?], Unit] =
          OptionT(refineType(p).toOption.η[Free[S, ?]]) >>= (f =>
            vcache.get(f) >> (vcache.delete(f).liftM[OptionT]))

        respond((M.unmount(p) *> deleteViewIfExists.run).as(s"deleted ${printPath(p)}"))
    }

  ////

  private def vcache[S[_]](implicit S0: VCache :<: S) = KeyValueStore.Ops[AFile, ViewCache, S]

  private def move[S[_], T](
    src: AbsPath[T],
    dstStr: String,
    parse: String => Option[Path[Abs, T, Unsandboxed]],
    typeStr: String
  )(implicit
    M: Mounting.Ops[S],
    S0: MountingFailure :<: S,
    S1: PathMismatchFailure :<: S,
    S2: VCache :<: S
  ): EitherT[Free[S, ?], ApiError, String] =
    parse(dstStr).map(unsafeSandboxAbs).cata(dst =>
      ((refineType(src) ⊛ refineType(dst))((s, d) =>
        vcache.get(s) >>= (vc => (vcache.put(d, vc) >> vcache.delete(s)).liftM[OptionT])
      ) | ().η[OptionT[Free[S, ?], ?]]).run.liftM[ApiErrT] >>
      M.remount[T](src, dst)
        .as(s"moved ${printPath(src)} to ${printPath(dst)}")
        .liftM[ApiErrT],
      ApiError.apiError(
        BadRequest withReason s"Expected an absolute $typeStr.",
        "path" := transcode(UriPathCodec, posixCodec)(dstStr)
      ).raiseError[ApiErrT[Free[S, ?], ?], String])

  private def mount[S[_]](
    path: APath,
    req: Request,
    replaceIfExists: Boolean
  )(implicit
    M:  Mounting.Ops[S],
    MF: ManageFile.Ops[S],
    T:  Timing.Ops[S],
    S0: Task :<: S,
    S1: MountingFailure :<: S,
    S2: PathMismatchFailure :<: S,
    S3: VCache :<: S
  ): EitherT[Free[S, ?], ApiError, Boolean] =
    for {
      body  <- free.lift(EntityDecoder.decodeString(req))
                 .into[S].liftM[ApiErrT]
      bConf <- EitherT.fromDisjunction[Free[S, ?]](Parse.decodeWith[ApiError \/ MountConfig, MountConfig](
                 body,
                 (_: MountConfig).right[ApiError],
                 parseErrorMsg => ApiError.fromMsg_(
                   BadRequest withReason "Malformed input.",
                   parseErrorMsg).left,
                 (msg, _) => ApiError.fromMsg_(
                   BadRequest, msg).left))
      exists <- M.lookupType(path).run.isDefined.liftM[ApiErrT]
      _      <- (replaceIfExists && exists).fold(M.replace(path, bConf), M.mount(path, bConf)).liftM[ApiErrT]
      maxAge =  req.headers.get(`Cache-Control`).flatMap(_.values.list.collectFirst(_ match {
                  case `max-age`(s) => s
                }))
      _      <- (refineType(path).toOption ⊛ MountConfig.viewConfig.getOption(bConf).map(MountConfig.ViewConfig.tupled) ⊛ maxAge)(createNewViewCache[S])
                  .getOrElse(().point[EitherT[Free[S, ?], ApiError, ?]])
    } yield exists

  private def createNewViewCache[S[_]](
    viewPath: AFile, 
    viewConfig: MountConfig.ViewConfig, 
    maxAge: scala.concurrent.duration.Duration
  )(implicit
    MF: ManageFile.Ops[S],
    T:  Timing.Ops[S],
    S0: Task :<: S,
    S1: VCache :<: S
  ): EitherT[Free[S, ?], ApiError, Unit] =
    for {
      tempFile      <- MF.tempFile(viewPath).leftMap(_.toApiError)
      timeStamp     <- T.timestamp.liftM[ApiErrT]
     
      refreshAfter  <- free.lift(Task.fromDisjunction(ViewCache.expireAt(timeStamp, maxAge))).into.liftM[ApiErrT]
      newViewCache  = ViewCache(
                       viewConfig = viewConfig,
                       lastUpdate = none,
                       executionMillis = none,
                       cacheReads = 0,
                       assignee = none,
                       assigneeStart = none,
                       maxAgeSeconds = maxAge.toSeconds,
                       refreshAfter = refreshAfter,
                       status = ViewCache.Status.Pending,
                       errorMsg = none,
                       dataFile = tempFile,
                       tmpDataFile = none)
      _             <- vcache.get(viewPath).fold(
                          κ(vcache.modify(viewPath, _.copy(
                            viewConfig = newViewCache.viewConfig,
                            maxAgeSeconds = newViewCache.maxAgeSeconds,
                            refreshAfter = newViewCache.refreshAfter,
                            status = newViewCache.status,
                            dataFile = newViewCache.dataFile))),
                          vcache.put(viewPath, newViewCache)).join.liftM[ApiErrT]
    } yield ()
}
