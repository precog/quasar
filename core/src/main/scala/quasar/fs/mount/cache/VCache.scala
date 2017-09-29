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

package quasar.fs.mount.cache

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.effect.{Failure, KeyValueStore}
import quasar.fp.free.injectFT
import quasar.fs.{FileSystemError, FileSystemFailure, ManageFile}
import quasar.fs.FileSystemError.PathErr
import quasar.fs.PathError.PathNotFound
import quasar.metastore._, KeyValueStore._, MetaStoreAccess._

import doobie.imports.ConnectionIO
import scalaz._, Scalaz._

object VCache {
  type Ops[S[_]] = KeyValueStore.Ops[AFile, ViewCache, S]

  implicit def Ops[S[_]](implicit S0: VCache :<: S): Ops[S] =
    KeyValueStore.Ops[AFile, ViewCache, S]

  def deleteFiles[S[_]](
      files: List[AFile]
  )(
      implicit
      M: ManageFile.Ops[S],
      E: Failure.Ops[FileSystemError, S]): Free[S, Unit] =
    E.unattempt(
      files.traverse_(M.delete(_)).run ∘ (_.bimap(
        {
          case PathErr(PathNotFound(_)) => ().right[FileSystemError]
          case e => e.left
        },
        _.right
      ).merge))

  def deleteVCacheFilesThen[S[_], A](
      k: AFile,
      op: ConnectionIO[A]
  )(
      implicit
      M: ManageFile.Ops[S],
      E: Failure.Ops[FileSystemError, S],
      S0: ConnectionIO :<: S): Free[S, A] =
    for {
      vc <- injectFT[ConnectionIO, S].apply(lookupViewCache(k))
      _ <- vc.foldMap(c => deleteFiles[S](c.dataFile :: c.tmpDataFile.toList))
      r <- injectFT[ConnectionIO, S].apply(op)
    } yield r

  def interp[S[_]](
      implicit
      S0: ManageFile :<: S,
      S1: FileSystemFailure :<: S,
      S2: ConnectionIO :<: S): VCache ~> Free[S, ?] = λ[VCache ~> Free[S, ?]] {
    case Keys() =>
      injectFT[ConnectionIO, S].apply(Queries.viewCachePaths.list ∘ (_.toVector))
    case Get(k) =>
      injectFT[ConnectionIO, S].apply(lookupViewCache(k))
    case Put(k, v) =>
      deleteVCacheFilesThen(k, insertOrUpdateViewCache(k, v))
    case CompareAndPut(k, expect, v) =>
      injectFT[ConnectionIO, S].apply(lookupViewCache(k)) >>= (vc =>
        (vc ≟ expect).fold(
          {
            vc.foldMap(
              c =>
                // Only delete files that differ from the replacement view cache
                deleteFiles(((c.dataFile ≠ v.dataFile) ?? List(c.dataFile)) :::
                  ((c.tmpDataFile ≠ v.tmpDataFile) ?? c.tmpDataFile.toList))) >>
              injectFT[ConnectionIO, S].apply(insertOrUpdateViewCache(k, v))
          }.as(true),
          false.η[Free[S, ?]]
        ))
    case Delete(k) =>
      deleteVCacheFilesThen(k, runOneRowUpdate(Queries.deleteViewCache(k)))
  }
}
