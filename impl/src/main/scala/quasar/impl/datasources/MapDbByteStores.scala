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

package quasar.impl.datasources

import cats.Applicative
import cats.effect.Sync

import java.lang.String

import quasar.connector.ByteStore

import org.mapdb.{BTreeMap, DB, Serializer}
import org.mapdb.serializer.SerializerArrayTuple

import scala.{AnyRef, Array, Boolean, Byte, Option, Unit}

final class MapDbByteStores[F[_]: Sync, K <: AnyRef] private (db: DB, store: BTreeMap[Array[AnyRef], Array[Byte]])
    extends ByteStores[F, K] {

  private class PrefixByteStore(prefix: K) extends ByteStore[F] {
    def lookup(key: String): F[Option[Array[Byte]]] =
      Sync[F].delay(Option(store.get(composite(key))))

    def insert(key: String, value: Array[Byte]): F[Unit] =
      Sync[F] delay {
        store.put(composite(key), value)
        db.commit()
      }

    def delete(key: String): F[Boolean] =
      Sync[F] delay {
        val prev = store.remove(composite(key))
        db.commit()
        prev != null
      }

    private def composite(k: String): Array[AnyRef] =
      Array(prefix, k)
  }

  def get(prefix: K): F[ByteStore[F]] =
    Applicative[F].pure(new PrefixByteStore(prefix))

  def clear(prefix: K): F[Unit] =
    Sync[F] delay {
      store.prefixSubMap(Array[AnyRef](prefix)).clear()
      db.commit()
    }
}

object MapDbByteStores {
  def apply[F[_]: Sync, K <: AnyRef](
      name: String,
      db: DB,
      prefixSerializer: Serializer[K])
      : F[ByteStores[F, K]] =
    Sync[F] delay {
      val store =
        db.treeMap(name)
          .keySerializer(new SerializerArrayTuple(prefixSerializer, Serializer.STRING))
          .valueSerializer(Serializer.BYTE_ARRAY)
          .createOrOpen()

      new MapDbByteStores(db, store)
    }
}
