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

package quasar.connector

import slamdata.Predef._

import quasar.Store

import java.lang.String
import java.nio.charset.Charset

import cats.{Applicative, Functor}
import cats.effect.Concurrent

object ByteStore {
  /* An in-memory `ByteStore` without any persistence. */
  def ephemeral[F[_]: Concurrent]: F[ByteStore[F]] =
    Store.ephemeral[F, String, Array[Byte]]

  /* An empty `ByteStore` that ignores all inserts. */
  def void[F[_]: Applicative]: ByteStore[F] =
    Store.void[F, String, Array[Byte]]

  object ops {
    private lazy val utf8 = Charset.forName("UTF-8")

    final implicit class ByteStoreOps[F[_]](val bs: ByteStore[F]) extends scala.AnyVal {
      def lookupString(key: String)(implicit F: Functor[F]): F[Option[String]] =
        F.map(bs.lookup(key))(_.map(new String(_, utf8)))

      def insertString(key: String, value: String): F[Unit] =
        bs.insert(key, value.getBytes(utf8))
    }
  }
}
