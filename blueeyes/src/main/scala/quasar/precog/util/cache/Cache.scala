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

package quasar.precog.util.cache

import com.google.common.cache.{Cache => GCache, _}

import scalaz.Validation

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.concurrent.duration.Duration

class SimpleCache[K, V] (private val backing: GCache[K, V]) extends Map[K, V] {
  def += (kv: (K, V)) = { backing.put(kv._1, kv._2); this }
  def -= (key: K) = { backing.invalidate(key); this }
  def get(key: K): Option[V] = Option(backing.getIfPresent(key))
  def iterator: Iterator[(K, V)] = backing.asMap.entrySet.iterator.asScala.map { kv => (kv.getKey, kv.getValue) }
  def invalidateAll = backing.invalidateAll
}

class AutoCache[K, V] (private val backing: LoadingCache[K, V]) extends Map[K, V] {
  def += (kv: (K, V)) = { backing.put(kv._1, kv._2); this }
  def -= (key: K) = { backing.invalidate(key); this }
  def get (key: K): Option[V] = getFull(key).toOption
  def iterator: Iterator[(K, V)] = backing.asMap.entrySet.iterator.asScala.map { kv => (kv.getKey, kv.getValue) }
  def invalidateAll = backing.invalidateAll

  def getFull(key: K): Validation[Throwable, V] = Validation fromTryCatchNonFatal {
    backing.get(key)
  }
}


object Cache {
  sealed trait CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]): CacheBuilder[K, V]
  }

  case class MaxSize[K, V](size: Long) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.maximumSize(size)
  }

  case class ExpireAfterAccess[K, V](timeout: Duration) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.expireAfterAccess(timeout.length, timeout.unit)
  }

  case class ExpireAfterWrite[K, V](timeout: Duration) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.expireAfterWrite(timeout.length, timeout.unit)
  }

  case class OnRemoval[K, V](onRemove: (K, V, RemovalCause) => Unit) extends CacheOption[K, V] {
    def apply(builder: CacheBuilder[K, V]) = builder.removalListener(new RemovalListener[K, V] {
      def onRemoval(notification: RemovalNotification[K, V]) = onRemove(notification.getKey, notification.getValue, notification.getCause)
    })
  }

  private def createBuilder[K, V](options: Seq[CacheOption[K, V]]): CacheBuilder[K, V] =
    options.foldLeft(CacheBuilder.newBuilder.asInstanceOf[CacheBuilder[K, V]]) {
      case (acc, opt) => opt.apply(acc)
    }

  def simple[K, V] (options: CacheOption[K, V]*): SimpleCache[K, V] = {
    new SimpleCache[K, V](createBuilder(options).build())
  }

  def auto[K, V] (options: CacheOption[K, V]*)(loader: K => V): AutoCache[K, V] = {
    val backing = createBuilder(options).build(new CacheLoader[K, V] {
      def load(key : K) = loader(key)
    })
    new AutoCache[K, V](backing)
  }
}
