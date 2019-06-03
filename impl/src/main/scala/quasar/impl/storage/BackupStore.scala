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

package quasar.impl.storage

import slamdata.Predef._

import cats.effect._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.Applicative

import quasar.concurrent.BlockingContext

import io.atomix.core._
import io.atomix.core.lock._
import io.atomix.core.value._

import java.util.concurrent.ConcurrentMap
import java.nio.file.Path

import AtomixSetup._

class BackupStore[F[_]: Async: ContextShift, K, V](underlying: BackupAsyncAtomicMap[K, V])
  extends AsyncAtomicIndexedStore[F, K, V, BackupAsyncAtomicMap[K, V]](underlying) {

  def restore: F[Boolean] = AtomixSetup.cfToAsync(underlying.restore)
}

object BackupStore {
  val ThreadPrefix: String = "backup-store-thread"
  val InitilizationLock: String = "initializing"
  val InitializedFlag: String = "initializing"

  def apply[F[_]: Concurrent: ContextShift, K, V](
      atomix: Atomix,
      name: String,
      backup: ConcurrentMap[K, V],
      pool: BlockingContext,
      thisNode: NodeInfo,
      logPath: Path,
      seeds: List[NodeInfo])
      : Resource[F, BackupStore[F, K, V]] = {

    def backupMap(atomix: Atomix): F[BackupAsyncAtomicMap[K, V]] = Sync[F].delay {
      BackupAsyncAtomicMap[F, K, V](atomix.atomicMapBuilder[K, V](name).build().async(), backup, pool)
    }
    def fLock(atomix: Atomix): F[AsyncAtomicLock] = Sync[F].delay {
      atomix.atomicLockBuilder(InitilizationLock).build().async()
    }
    def fInitializedFlag(atomix: Atomix): F[AsyncAtomicValue[Boolean]] =
      Sync[F].delay(atomix.atomicValueBuilder[Boolean](InitializedFlag).build().async())

    AtomixSetup.atomix(thisNode, seeds, logPath, ThreadPrefix) evalMap {
      case (atomix, nodes) => for {
        map <- backupMap(atomix)
        store = new BackupStore(map)
        lock <- fLock(atomix)
        _ <- cfToAsync(lock.lock())
        flag <- fInitializedFlag(atomix)
        initialized <- cfToAsync(flag.get)
        _ <- Applicative[F].unlessA(initialized){ for {
          _ <- store.restore
          _ <- cfToAsync(flag.set(true))
          _ <- cfToAsync(lock.unlock())
        } yield () }
      } yield store
    }
  }
}
