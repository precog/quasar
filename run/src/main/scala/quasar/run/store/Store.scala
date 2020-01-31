/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.run.store

import slamdata.Predef._

import cats.Show
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.show._
import cats.instances.string._

import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}, IndexedStore._

import java.nio.file.Path
import java.util.UUID
import org.mapdb._

import argonaut.{CodecJson, Parse}
import monocle.Prism

import shims.monadToScalaz

object Store {
  val MapDBTableName = "default"

  def codecStore[A: Show, F[_]: Sync: ContextShift: MonadError_[?[_], StoreError]](
    path: Path,
    codec: CodecJson[A],
    blocker: Blocker)
    : Resource[F, IndexedStore[F, UUID, A]] =
    for {
      db <- mapDB[F](path)
      table <- Resource.liftF(uuidTable[F](db))
    } yield {
      val prismaticCodec: Prism[String, A] =
        Prism[String, A](str =>
          Parse.parseOption(str).flatMap(codec.Decoder.decodeJson(_).toOption))(
          codec.Encoder.encode(_).nospaces)

      transformValue(errorFromShow[String]("CodecJson"))(
        ConcurrentMapIndexedStore(table, Sync[F].delay { db.commit() }, blocker), prismaticCodec)
    }

  def errorFromShow[A: Show](desc: String)(a: A): StoreError =
    StoreError.corrupt(s"Failed to decode '$desc' from '${a.show}'")

  def mapDB[F[_]: Sync](path: Path): Resource[F, DB] =
    Resource[F, DB](Sync[F].delay {
      val isWindows: Boolean =
        java.lang.System.getProperty("os.name").toLowerCase.indexOf("windows") > -1

      val rawMaker =
        DBMaker
          .fileDB(path.toFile)
          .checksumHeaderBypass
          .transactionEnable
          .fileLockDisable

      val mmapedMaker =
        if (isWindows)
          rawMaker
        else
          rawMaker
            .fileMmapEnableIfSupported
            .fileMmapPreclearDisable
            .cleanerHackEnable

      val db = mmapedMaker.make

      (db, Sync[F].delay(db.close))
    })

  def stringTable[F[_]: Sync](db: DB): F[BTreeMap[String, String]] =
    Sync[F].delay {
      db.treeMap(MapDBTableName)
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.STRING)
        .createOrOpen
    }

  def uuidTable[F[_]: Sync](db: DB): F[BTreeMap[UUID, String]] =
    Sync[F].delay {
      db.treeMap(MapDBTableName)
        .keySerializer(Serializer.UUID)
        .valueSerializer(Serializer.STRING)
        .createOrOpen
    }
}
