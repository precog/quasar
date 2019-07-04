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

import io.atomix.core.collection.AsyncDistributedCollection
import io.atomix.core.map.{AtomicMap, AsyncAtomicMap, AtomicMapEventListener, AtomicMapEvent}
import io.atomix.core.map.impl._
import io.atomix.core.set.AsyncDistributedSet
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive
import io.atomix.core.transaction.{TransactionId, TransactionLog}
import io.atomix.utils.time.Versioned

import java.time.Duration
import java.util.concurrent.{CompletableFuture, Executor, ConcurrentHashMap, ConcurrentMap}
import java.util.{Map => JMap}, JMap.Entry
import java.util.function.{BiFunction, Consumer, Predicate, Supplier}
import java.util.stream.Collectors

import quasar.concurrent.BlockingContext

import scalaz.syntax.tag._

final class BackupAsyncAtomicMap[K, V](
    backing: AsyncAtomicMap[K, V],
    backup: ConcurrentMap[K, V],
    executor: Executor)
    extends DelegatingAsyncPrimitive[AsyncAtomicMap[K, V]](backing)
    with AsyncAtomicMap[K, V] {

  private val listenersMap: JMap[AtomicMapEventListener[K, V], Executor] = new ConcurrentHashMap()

  private val updater: AtomicMapEventListener[K, V] = new AtomicMapEventListener[K, V] {
    def event(ev: AtomicMapEvent[K, V]) = {
      Option(ev.newValue) match {
        case None =>
          backup.remove(ev.key)
        case Some(a) =>
          backup.put(ev.key, a.value)
      }
      listenersMap forEach { (listener, executor) => executor.execute(() => listener.event(ev)) }
    }
  }

  delegate.addListener(updater, executor)

  private def cf[A](sup: Supplier[A]): CompletableFuture[A] =
    CompletableFuture.supplyAsync(sup, executor)

  def restore: CompletableFuture[Boolean] =
    if (backup.size < 1) CompletableFuture.completedFuture(true)
    else {
      val updates: java.util.List[MapUpdate[K, V]] =
        backup.entrySet.stream map[MapUpdate[K, V]] { e =>
          MapUpdate
            .builder[K, V]
            .withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
            .withKey(e.getKey)
            .withValue(e.getValue)
            .withVersion(1L)
            .build
        } collect(Collectors.toList())

      val transactionId = TransactionId.from(java.util.UUID.randomUUID().toString())
      val transaction = new TransactionLog(transactionId, 1L, updates)
      prepare(transaction) thenCompose { r =>
        if (r.booleanValue) commit(transactionId) thenApply { k => true }
        else CompletableFuture.completedFuture(false)
      }
    }

  override def size(): CompletableFuture[java.lang.Integer] =
    cf(() => new java.lang.Integer(backup.size))

  override def containsKey(k: K): CompletableFuture[java.lang.Boolean] =
    cf(() => new java.lang.Boolean(backup.containsKey(k)))

  override def containsValue(v: V): CompletableFuture[java.lang.Boolean] =
    cf(() => new java.lang.Boolean(backup.containsValue(v)))

  override def get(k: K): CompletableFuture[Versioned[V]] =
    delegate.get(k)

  override def getAllPresent(ks: java.lang.Iterable[K]): CompletableFuture[JMap[K, Versioned[V]]] =
    delegate.getAllPresent(ks)

  override def getOrDefault(k: K, v: V): CompletableFuture[Versioned[V]] =
    delegate.getOrDefault(k, v)

  override def computeIf(k: K, p: Predicate[_ >: V], remap: BiFunction[_ >: K, _ >: V, _ <: V]): CompletableFuture[Versioned[V]] =
    delegate.computeIf(k, p, remap)

  override def put(k: K, v: V, ttl: Duration): CompletableFuture[Versioned[V]] = {
    delegate.put(k, v, ttl) thenApply { oldValue =>
      backup.put(k, v)
      oldValue
    }
  }

  override def putAndGet(k: K, v: V, ttl: Duration): CompletableFuture[Versioned[V]] =
    delegate.putAndGet(k, v, ttl) thenCompose { newVal => cf { () =>
      backup.put(k, newVal.value)
      newVal
    }}

  override def remove(k: K): CompletableFuture[Versioned[V]] = {
    delegate.remove(k) thenApply { oldV =>
      backup.remove(k)
      oldV
    }
  }

  override def clear(): CompletableFuture[java.lang.Void] =
    delegate.clear()

  override def keySet(): AsyncDistributedSet[K] =
    delegate.keySet()

  override def values(): AsyncDistributedCollection[Versioned[V]] =
    delegate.values()

  override def entrySet(): AsyncDistributedSet[Entry[K, Versioned[V]]] =
    delegate.entrySet()

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def putIfAbsent(k: K, v: V, ttl: Duration): CompletableFuture[Versioned[V]] =
    delegate.putIfAbsent(k, v, ttl) thenApply { oldValue =>
      backup.put(k, v)
      oldValue
    }

  override def remove(k: K, v: V): CompletableFuture[java.lang.Boolean] =
    delegate.remove(k, v) thenApply { removed =>
      if (removed.booleanValue) backup.remove(k) else ()
      removed
    }

  override def remove(k: K, version: Long): CompletableFuture[java.lang.Boolean] =
    delegate.remove(k, version) thenApply { removed =>
      if (removed.booleanValue) backup.remove(k) else ()
      removed
    }

  override def replace(k: K, v: V): CompletableFuture[Versioned[V]] =
    delegate.replace(k, v) thenApply { prevVal =>
      backup.put(k, v)
      prevVal
    }

  override def replace(k: K, v: V, newV: V): CompletableFuture[java.lang.Boolean] =
    delegate.replace(k, v, newV) thenApply { replaced =>
      if (replaced.booleanValue) {
        backup.put(k, v)
        replaced
      }
      else replaced
    }

  override def replace(k: K, oldVersion: Long, v: V): CompletableFuture[java.lang.Boolean] =
    delegate.replace(k, oldVersion, v) thenApply { replaced =>
      if (replaced.booleanValue) {
        backup.put(k, v)
        replaced
      }
      else replaced
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def addListener(listener: AtomicMapEventListener[K, V], executor: Executor): CompletableFuture[java.lang.Void] = {
    listenersMap.put(listener, executor)
    CompletableFuture.completedFuture(null)
  }
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def removeListener(listener: AtomicMapEventListener[K, V]): CompletableFuture[java.lang.Void] = {
    listenersMap.remove(listener, executor)
    CompletableFuture.completedFuture(null)
  }

  override def prepare(log: TransactionLog[MapUpdate[K, V]]): CompletableFuture[java.lang.Boolean] =
    delegate.prepare(log)

  override def commit(id: TransactionId): CompletableFuture[java.lang.Void] =
    delegate.commit(id)

  override def rollback(id: TransactionId): CompletableFuture[java.lang.Void] =
    delegate.rollback(id)

  override def addStateChangeListener(listener: Consumer[PrimitiveState]): Unit =
    delegate.addStateChangeListener(listener)

  override def removeStateChangeListener(listener: Consumer[PrimitiveState]): Unit =
    delegate.removeStateChangeListener(listener)

  override def sync(timeout: Duration): AtomicMap[K, V] =
    new BlockingAtomicMap[K, V](this, timeout.toMillis)

  override def delete(): CompletableFuture[java.lang.Void] = {
    delegate.removeListener(updater) thenCompose { x => delegate.delete() }
  }
}

object BackupAsyncAtomicMap {
  def apply[F[_]: Async: ContextShift, K, V](
      async: AsyncAtomicMap[K, V],
      backup: ConcurrentMap[K, V],
      blockingPool: BlockingContext)
      : BackupAsyncAtomicMap[K, V] = {
    val executor = new Executor { def execute(r: java.lang.Runnable) = blockingPool.unwrap.execute(r) }
    new BackupAsyncAtomicMap(async, backup, executor)
  }
}

class Foo(x: Int) {
  private final val xx: Int = x
}

class Bar(x: Int) extends Foo(x) {
  private final val xx: Int = x + 1
}
