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

package quasar.fs.cache

import slamdata.Predef._
import quasar.common.PhaseResultT
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz.eitherT._
import quasar.effect.Timing
import quasar.frontend.SemanticErrsT
import quasar.fp.free
import quasar.fs.FileSystemError, FileSystemError._
import quasar.fs.mount.{MountConfig, Mounting}
import quasar.fs.mount.cache.{ViewCache, VCache}
import quasar.fs.MoveSemantics.Overwrite
import quasar.fs.PathError._
import quasar.fs.{ManageFile, QueryFile, WriteFile}
import quasar.main.FilesystemQueries
import quasar.metastore.{MetaStoreAccess, PathedViewCache, Queries}

import java.time.{Duration => JDuration, Instant}
import scala.concurrent.duration._

import doobie.free.connection.ConnectionIO
import eu.timepit.refined.auto._
import pathy.Path._
import scalaz._, Scalaz._, NonEmptyList.nels
import scalaz.concurrent.Task

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object ViewCacheRefresh {

  def updateCache[S[_]](
    viewPath: AFile,
    assigneeId: String
  )(implicit
    Q: QueryFile.Ops[S],
    T: Timing.Ops[S],
    S0: WriteFile :<: S,
    S1: ManageFile :<: S,
    S2: Mounting :<: S,
    S3: ConnectionIO :<: S,
    S4: Task :<: S
  ): Q.transforms.CompExecM[Unit] = {
    import Q.transforms._

    val M = ManageFile.Ops[S]

    val getCachedView: CompExecM[ViewCache] =
      lift(MetaStoreAccess.lookupViewCache(viewPath) ∘ (_ \/> pathErr(pathNotFound(viewPath))))

    def updatePerSuccessfulCacheRefresh(
      viewPath: AFile, lastUpdate: Instant, executionMillis: Long, refreshAfter: Instant
    ): CompExecM[Unit] =
      lift(Queries.updatePerSuccesfulCacheRefresh(viewPath, lastUpdate, executionMillis, refreshAfter).run.void ∘ (
        _.right[FileSystemError]))

    for {
      vc  <- getCachedView
      tf  <- lift(VCache.cacheFile(viewPath).run)
      ts1 <- lift(T.timestamp ∘ (_.right[FileSystemError]))
      _   <- lift(assigneeStart(viewPath, assigneeId, ts1, tf) ∘ (_.right[FileSystemError]))
      _   <- writeViewCache[S](fileParent(viewPath), tf, vc.viewConfig)
      _   <- lift(M.moveFile(tf, vc.dataFile, Overwrite).run)
      ts2 <- lift(T.timestamp ∘ (_.right[FileSystemError]))
      em  <- lift(free.lift(Task.delay(JDuration.between(ts1, ts2).toMillis)).into[S] ∘ (_.right[FileSystemError]))
      e   <- lift(free.lift(
               Task.fromDisjunction(ViewCache.expireAt(ts2, vc.maxAgeSeconds.seconds))
             ).into[S] ∘ (_.right[FileSystemError]))
      _   <- updatePerSuccessfulCacheRefresh(viewPath, ts2, em, e)
    } yield ()
  }

  // Naïve stale cache selection for the moment
  def selectCacheForRefresh[S[_]](implicit
    Q: QueryFile.Ops[S],
    T: Timing.Ops[S],
    S0: ConnectionIO :<: S
  ): Q.transforms.CompExecM[Option[PathedViewCache]] =
    lift(T.timestamp ∘ (_.right[FileSystemError])) >>= (ts =>
      lift(MetaStoreAccess.staleCachedViews(ts) ∘ (_.right[FileSystemError])) ∘ (_.headOption))

  def assigneeStart(path: AFile, assigneeId: String, start: Instant, tmpDataPath: AFile): ConnectionIO[Int] =
    Queries.cacheRefreshAssigneStart(path, assigneeId, start, tmpDataPath).run

  def writeViewCache[S[_]](
    basePath: ADir, tmpDataPath: AFile, view: MountConfig.ViewConfig
  )(implicit
    Q: QueryFile.Ops[S],
    W: WriteFile.Ops[S],
    S1: ManageFile :<: S,
    S2: Mounting :<: S,
    S3: Task :<: S
  ): Q.transforms.CompExecM[Unit] = {
    val fsQ = new FilesystemQueries[S]

    for {
      q       <- EitherT(EitherT(
                   (quasar.resolveImports_[S](view.query, basePath).leftMap(nels(_)).run.run ∘ (_.sequence))
                     .liftM[PhaseResultT]))
      _       <- fsQ.executeQuery(q, view.vars, basePath, tmpDataPath)
    } yield ()
  }

  def lift[S[_], A](
    v: FileSystemError \/ A
  )(implicit
    Q: QueryFile.Ops[S]
  ): Q.transforms.CompExecM[A] =
    lift(v.η[Free[S, ?]])

  def lift[S[_], A](
    v: Free[S, FileSystemError \/ A]
  )(implicit
    Q: QueryFile.Ops[S]
  ): Q.transforms.CompExecM[A] =
    EitherT(v.liftM[PhaseResultT].liftM[SemanticErrsT])

  def lift[S[_], A](
    cio: ConnectionIO[FileSystemError \/ A]
  )(implicit
    S0: ConnectionIO :<: S,
    Q: QueryFile.Ops[S]
  ): Q.transforms.CompExecM[A] =
    lift(Free.liftF(S0(cio)))
}
