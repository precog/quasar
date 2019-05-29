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

import cats.syntax.functor._
import cats.effect._

import io.atomix.core.collection.AsyncDistributedCollection
import io.atomix.core.map.{AtomicMap, AsyncAtomicMap, AtomicMapEventListener, AtomicMapEvent}
import io.atomix.core.map.impl.{BlockingAtomicMap, MapUpdate, AtomicMapClient}
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

import org.mapdb._

final class MapDBAsyncAtomicMap[K, V](
    backing: AsyncAtomicMap[K, V],
    cache: ConcurrentMap[K, Versioned[V]],
    executor: Executor)
    extends DelegatingAsyncPrimitive[AsyncAtomicMap[K, V]](backing)
    with AsyncAtomicMap[K, V] {

  private val listenersMap: JMap[AtomicMapEventListener[K, V], Executor] = new ConcurrentHashMap()

  private val updater: AtomicMapEventListener[K, V] = new AtomicMapEventListener[K, V] {
    def event(ev: AtomicMapEvent[K, V]) = {
      Option(ev.newValue) match {
        case None =>
          cache.remove(ev.key)
        case Some(a) =>
          cache.put(ev.key, a)
      }
      listenersMap forEach { (listener, executor) => executor.execute(() => listener.event(ev)) }
    }
  }

  delegate.addListener(updater, executor)

  private def cf[A](sup: Supplier[A]): CompletableFuture[A] =
    CompletableFuture.supplyAsync(sup, executor)

  def initialize: CompletableFuture[Boolean] =
    if (cache.size < 1) CompletableFuture.completedFuture(true)
    else {
      val updates: java.util.List[MapUpdate[K, V]] =
        cache.entrySet.stream map[MapUpdate[K, V]] { e =>
          MapUpdate
            .builder[K, V]
            .withType(MapUpdate.Type.PUT_IF_VERSION_MATCH)
            .withKey(e.getKey)
            .withValue(e.getValue.value)
            .withVersion(e.getValue.version)
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
    cf(() => new java.lang.Integer(cache.size))

  override def containsKey(k: K): CompletableFuture[java.lang.Boolean] =
    cf(() => new java.lang.Boolean(cache.containsKey(k)))

  override def containsValue(v: V): CompletableFuture[java.lang.Boolean] =
    cf(() => new java.lang.Boolean(cache.containsValue(v)))

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
      cache.put(k, Option(oldValue) match {
        case None => new Versioned(v, 1, java.lang.System.currentTimeMillis())
        case Some(oldV) => new Versioned(v, oldV.version() + 1, oldV.creationTime())
      })
      oldValue
    }
  }

  override def putAndGet(k: K, v: V, ttl: Duration): CompletableFuture[Versioned[V]] =
    delegate.putAndGet(k, v, ttl) thenCompose { newVal => cf { () =>
      cache.put(k, newVal)
      newVal
    }}

  override def remove(k: K): CompletableFuture[Versioned[V]] = {
    delegate.remove(k) thenApply { oldV =>
      cache.remove(k)
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
      cache.put(k, Option(oldValue) match {
        case None => new Versioned(v, 1, java.lang.System.currentTimeMillis())
        case Some(oldV) => new Versioned(v, oldV.version() + 1, oldV.creationTime())
      })
    }

  override def remove(k: K, v: V): CompletableFuture[java.lang.Boolean] =
    delegate.remove(k, v) thenApply { removed =>
      if (removed.booleanValue) cache.remove(k) else ()
      removed
    }

  override def remove(k: K, version: Long): CompletableFuture[java.lang.Boolean] =
    delegate.remove(k, version) thenApply { removed =>
      if (removed.booleanValue) cache.remove(k) else ()
      removed
    }

  override def replace(k: K, v: V): CompletableFuture[Versioned[V]] =
    delegate.replace(k, v) thenApply { prevVal =>
      cache.put(k, Option(prevVal) match {
        case None => new Versioned(v, 1, java.lang.System.currentTimeMillis())
        case Some(oldV) => new Versioned(v, oldV.version() + 1, oldV.creationTime())
      })
      prevVal
    }

  override def replace(k: K, v: V, newV: V): CompletableFuture[java.lang.Boolean] =
    delegate.replace(k, v, newV) thenCompose { replaced =>
      if (replaced.booleanValue) delegate.get(k) thenApply { newVal =>
        cache.put(k, newVal)
        replaced
      }
      else CompletableFuture.completedFuture(replaced)
    }

  override def replace(k: K, oldVersion: Long, v: V): CompletableFuture[java.lang.Boolean] =
    delegate.replace(k, oldVersion, v) thenCompose { replaced =>
      if (replaced.booleanValue) delegate.get(k) thenApply { newVal =>
        cache.put(k, newVal)
        replaced
      }
      else CompletableFuture.completedFuture(replaced)
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

object MapDBAsyncAtomicMap {
  def apply[F[_]: Async: ContextShift, K, V](
      async: AsyncAtomicMap[K, V],
      cache: ConcurrentMap[K, Versioned[V]],
      blockingPool: BlockingContext)
      : F[Option[AsyncAtomicMap[K, V]]] = {
    val executor = new Executor { def execute(r: java.lang.Runnable) = blockingPool.unwrap.execute(r) }
    val delegate = new MapDBAsyncAtomicMap(async, cache, executor)
    AsyncAtomicIndexedStore.toF(delegate.initialize) map { (ok: Boolean) =>
      if (ok) Some(delegate) else None
    }
  }

  def versionedSerializer[A](serializer: Serializer[A]): Serializer[Versioned[A]] = new Serializer[Versioned[A]] {
    def serialize(out: DataOutput2, a: Versioned[A]): Unit = {
      Serializer.LONG.serialize(out, new java.lang.Long(a.creationTime))
      Serializer.LONG.serialize(out, new java.lang.Long(a.version))
      serializer.serialize(out, a.value)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    def deserialize(in: DataInput2, available: Int): Versioned[A] = {
      if (available < 16) throw new Throwable("versioned serializer input is too small")
      else {
        val creationTime = Serializer.LONG.deserialize(in, 8)
        val version = Serializer.LONG.deserialize(in, 8)
        val a = serializer.deserialize(in, available - 16)
        new Versioned(a, version.longValue, creationTime.longValue)
      }

    }
  }
}
