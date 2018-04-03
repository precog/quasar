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

package quasar.fs.mount.cache

import slamdata.Predef._
import quasar.contrib.pathy.AFile
import quasar.effect.{Failure, KeyValueStore, Read, Write}
import quasar.fp.free.injectFT
import quasar.fs.{FileSystemError, FileSystemFailure, ManageFile, TempFilePrefix}
import quasar.fs.FileSystemError.PathErr
import quasar.fs.PathError.PathNotFound
import quasar.metastore._, KeyValueStore._, MetaStoreAccess._

import java.time.{Duration, Instant}

import doobie.imports.ConnectionIO
import scalaz._, Scalaz._

object VCache {
  type VCacheKVS[A] = KeyValueStore[AFile, ViewCache, A]

  object VCacheKVS {
    type Ops[S[_]] = KeyValueStore.Ops[AFile, ViewCache, S]

    def Ops[S[_]](implicit S0: VCacheKVS :<: S): Ops[S] = KeyValueStore.Ops[AFile, ViewCache, S]
  }

  type VCacheExpR[A] = Read[MinOption[Expiration], A]
  type VCacheExpW[A] = Write[MinOption[Expiration], A]

  object VCacheExpR {
    type Ops[S[_]] = Read.Ops[MinOption[Expiration], S]
  }

  object VCacheExpW {
    type Ops[S[_]] = Write.Ops[MinOption[Expiration], S]
  }

  type ExpirationsT[F[_], A] = WriterT[F, List[Expiration], A]

  final case class Expiration(v: Instant)

  object Expiration {
    implicit val order: Order[Expiration] =
      Order.order((a, b) => Ordering.fromInt(a.v compareTo b.v))
  }

  def cacheFile[S[_]](
    viewPath: AFile)(
    implicit MF: ManageFile.Ops[S])
      : EitherT[Free[S, ?], FileSystemError, AFile]  = {
    val f = pathy.Path.fileName(viewPath).value
    MF.tempFile(viewPath, TempFilePrefix(s"cache_${f}_").some)
  }

  def deleteFiles[S[_]](
    files: List[AFile]
  )(implicit
    M: ManageFile.Ops[S],
    E: Failure.Ops[FileSystemError, S]
  ): Free[S, Unit] =
    E.unattempt(
      files.traverse_(M.delete(_)).run ∘ (_.bimap(
        {
          case PathErr(PathNotFound(_)) => ().right[FileSystemError]
          case e => e.left
        },
        _.right
    ).merge))

  def deleteVCacheFilesThen[S[_], A](
    k: AFile, op: ConnectionIO[A]
  )(implicit
    M: ManageFile.Ops[S],
    E: Failure.Ops[FileSystemError, S],
    S0: ConnectionIO :<: S
  ): Free[S, A] =
    for {
      vc <- injectFT[ConnectionIO, S].apply(lookupViewCache(k))
      _  <- vc.foldMap(c => deleteFiles[S](c.dataFile :: c.tmpDataFile.toList))
      r  <- injectFT[ConnectionIO, S].apply(op)
    } yield r

  def interp[S[_]](implicit
    W: VCacheExpW.Ops[S],
    S0: ManageFile :<: S,
    S1: FileSystemFailure :<: S,
    S2: ConnectionIO :<: S
  ): VCacheKVS ~> Free[S, ?] = λ[VCacheKVS ~> Free[S, ?]] {
    case Keys() =>
      injectFT[ConnectionIO, S].apply(Queries.viewCachePaths.list ∘ (_.toVector))
    case Get(k) =>
      injectFT[ConnectionIO, S].apply(lookupViewCache(k)) >>= { vc =>
        val expirations =
          Tags.Min(
            vc >>= (c => c.lastUpdate ∘ (lu =>
              Expiration(
                \/.fromTryCatchNonFatal(
                  lu.plus(Duration.ofSeconds(c.maxAgeSeconds))
                ) | Instant.MAX))))

        W.tell(expirations).as(vc)
      }
    case Put(k, v) =>
      deleteVCacheFilesThen(k, insertOrUpdateViewCache(k, v))
    case CompareAndPut(k, expect, v) =>
      injectFT[ConnectionIO, S].apply(lookupViewCache(k)) >>= (vc =>
        (vc ≟ expect).fold(
          {
            vc.foldMap(c =>
              // Only delete files that differ from the replacement view cache
              deleteFiles(
                ((c.dataFile ≠ v.dataFile) ?? List(c.dataFile)) :::
                ((c.tmpDataFile ≠ v.tmpDataFile) ?? c.tmpDataFile.toList))) >>
            injectFT[ConnectionIO, S].apply(insertOrUpdateViewCache(k, v))
          }.as(true),
          false.η[Free[S, ?]]))
    case Delete(k) =>
      deleteVCacheFilesThen(k, runOneRowUpdate(Queries.deleteViewCache(k)))
  }
}
