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

import quasar.concurrent.BlockingContext

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.Deferred
import cats.syntax.apply._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.eq._
import cats.instances.string._
import fs2.Stream
import scalaz.syntax.tag._

import org.apache.ignite._
import org.apache.ignite.lang.{IgniteFuture, IgniteBiInClosure, IgnitePredicate}
import org.apache.ignite.configuration.{IgniteConfiguration, CacheConfiguration, DataStorageConfiguration}
import org.apache.ignite.cache.{CacheWriteSynchronizationMode, CacheMode}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import org.apache.ignite.cache.store.CacheStore
import org.apache.ignite.events.{EventType, CacheEvent}

import scala.collection.JavaConverters._
import javax.cache.Cache.Entry
import scala.collection.JavaConverters._
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap}
import javax.cache.configuration.Factory
import java.nio.file.{Paths, Path => NPath}


final class IgniteStore[F[_]: Async: ContextShift, K, V](
    val cache: IgniteCache[K, V], pool: BlockingContext)
    extends IndexedStore[F, K, V] {

  import IgniteStore._

  private val F = Sync[F]

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.eval(evalOnPool(F.delay(cache.iterator.asScala)))
    entry <- evalStreamOnPool(Stream.fromIterator[F, Entry[K, V]](iterator))
  } yield (entry.getKey, entry.getValue)

  def lookup(k: K): F[Option[V]] =
    igfToF(cache.getAsync(k)) map (Option(_))

  def insert(k: K, v: V): F[Unit] =
    igfToF(cache.putAsync(k, v)) as (())

  def delete(k: K): F[Boolean] =
    igfToF(cache.removeAsync(k)) map (_.booleanValue)

  private def evalOnPool[A](fa: F[A]): F[A] =
    ContextShift[F].evalOn[A](pool.unwrap)(fa)

  private def evalStreamOnPool[A](s: Stream[F, A]): Stream[F, A] =
    s translate new FunctionK[F, F] {
      def apply[A](fa: F[A]): F[A] = evalOnPool(fa)
    }
}

object Ignite {
  final case class IgniteCfg(name: String, port: Int, seeds: List[String]) extends Product with Serializable

  def mkCfg[F[_]: Sync](our: IgniteCfg): F[IgniteConfiguration] = Sync[F].delay {
    val cfg = new IgniteConfiguration()
    cfg.setIgniteInstanceName(our.name)
    val tcp = new TcpDiscoverySpi()
    val ipFinder = new TcpDiscoveryVmIpFinder()
    ipFinder.setAddresses((s"localhost:${our.port}" :: our.seeds).asJava)
    tcp.setIpFinder(ipFinder)

    tcp.setLocalPort(our.port)
    cfg.setDiscoverySpi(tcp)

    cfg.setIncludeEventTypes(EventType.EVTS_CACHE:_*);
    cfg
  }

  def persistentCfg[F[_]: Sync](cfg: IgniteConfiguration, path: NPath): F[IgniteConfiguration] = Sync[F].delay {
    val dsCfg = new DataStorageConfiguration()
    dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true)
    dsCfg.setStoragePath(path.toAbsolutePath.toString)
    cfg.setDataStorageConfiguration(dsCfg)
    cfg
  }

  def createCache[F[_]: Sync, K, V](name: String, ig: Ignite): F[IgniteCache[K, V]] = Sync[F].delay {
    val config = new CacheConfiguration[K, V](name)
    config.setCacheMode(CacheMode.REPLICATED)
    ig.getOrCreateCache[K, V](config)
  }

  def mapStoreCache[F[_]: Sync, K, V](name: String, ig: Ignite, mp: ConcurrentMap[K, V]): F[IgniteCache[K, V]] = Sync[F].delay {
    val listener: IgnitePredicate[CacheEvent] = (evt: CacheEvent) => {
      if (evt.cacheName === name) evt.`type` match {
        case EventType.EVT_CACHE_OBJECT_PUT => evt.newValue match {
          case v: V => mp.put(evt.key[K], v); ()
          case _ => ()
        }
        case EventType.EVT_CACHE_OBJECT_REMOVED =>
          mp.remove(evt.key[K]); ()
        case _ => ()
      }
      true
    }
    ig.events.localListen(
      listener,
      EventType.EVT_CACHE_OBJECT_PUT,
      EventType.EVT_CACHE_OBJECT_READ,
      EventType.EVT_CACHE_OBJECT_REMOVED)
    val config = new CacheConfiguration[K, V](name)
    config.setCacheMode(CacheMode.REPLICATED)
    val factory = new Factory[CacheStore[K, V]] {
      def create(): CacheStore[K, V] = new CacheStore[K, V] {
        def load(k: K): V = mp.get(k)
        def loadAll(ks: java.lang.Iterable[_ <: K]): java.util.Map[K, V] = {
          val result = new ConcurrentHashMap[K, V]()
          ks.asScala.foreach { k =>
            Option(mp.get(k)) match {
              case Some(v) => result.put(k, v)
              case None => ()
            }
          }
          result
        }
        def delete(objK: Any): Unit = objK match {
          case k: K => mp.remove(k); ()
          case _ => ()
        }
        def deleteAll(ks: java.util.Collection[_]): Unit = {
          ks.asScala.foreach {
            case k: K => mp.remove(k); ()
            case _ => ()
          }
        }
        def write(entry: Entry[_ <: K, _ <: V]): Unit = {
          mp.put(entry.getKey, entry.getValue)
        }
        def writeAll(entries: java.util.Collection[Entry[_ <: K, _ <: V]]): Unit = {
          entries.asScala.foreach { entry =>
            mp.put(entry.getKey, entry.getValue)
          }
        }
        def loadCache(clo: IgniteBiInClosure[K, V], args: java.lang.Object*): Unit = {
          mp.forEach(new java.util.function.BiConsumer[K, V] {
            def accept(k: K, v: V): Unit = clo.apply(k, v)
          })
        }
        def sessionEnd(b: Boolean): Unit = ()
      }
    }
    config.setCacheStoreFactory(factory)
    config.setReadThrough(true)
    ig.getOrCreateCache[K, V](config)
  }

  def startIgnite[F[_]: ConcurrentEffect](igniteConfig: IgniteConfiguration): F[Ignite] = for {
    d <- Deferred.tryable[F, Ignite]
    _ <- Sync[F].delay(println(s"STARTING ::: ${igniteConfig.getIgniteInstanceName}"))
    creationListener = new IgnitionListener {
      def onStateChange(name: String, state: IgniteState): Unit = {
        if (name =!= igniteConfig.getIgniteInstanceName) ()
        else state match {
          case IgniteState.STARTED =>
            ConcurrentEffect[F].runAsync(d.complete(Ignition.ignite(igniteConfig.getIgniteInstanceName)))(x => IO.unit).unsafeRunSync
          case _ => ()
        }
      }
    }
    _ <- Sync[F].delay(Ignition.addListener(creationListener))
    _ <- Concurrent[F].start(Sync[F].delay(try { Ignition.start(igniteConfig) } catch { case e => println(e) }))
    i <- d.get
    _ <- Sync[F].delay(Ignition.removeListener(creationListener))
    _ <- Sync[F].delay(println(s"STARTED ::: ${igniteConfig.getIgniteInstanceName}"))
  } yield i

  def addMe[F[_]: Sync](ignite: Ignite): F[Unit] = for {
    _ <- Sync[F].delay(ignite.cluster.active(true))
    _ <- Sync[F].delay(ignite.cluster.setBaselineTopology(ignite.cluster.forServers.nodes))
  } yield ()

  def stopIgnite[F[_]: ConcurrentEffect](ignite: Ignite): F[Unit] = for {
    d <- Deferred.tryable[F, Unit]
    stoppingListener = new IgnitionListener {
      def onStateChange(name: String, state: IgniteState): Unit = {
        if (name =!= ignite.name) ()
        else state match {
          case IgniteState.STARTED => ()
          case _ => ConcurrentEffect[F].runAsync(d.complete(()))(x => IO.unit).unsafeRunSync
        }
      }
    }
    _ <- Sync[F].delay(Ignition.addListener(stoppingListener))
    _ <- Concurrent[F].start(Sync[F].delay(Ignition.stop(ignite.name, true)))
    _ <- d.get
    _ <- Sync[F].delay(Ignition.removeListener(stoppingListener))
    _ <- Sync[F].delay(println(s"STOPPED ${ignite.name}"))
  } yield ()

  def resource[F[_]: ConcurrentEffect](cfg: IgniteCfg): Resource[F, Ignite] =
    Resource.make(mkCfg(cfg).flatMap(startIgnite(_)))(i => stopIgnite(i))

  def persistentResource[F[_]: ConcurrentEffect](cfg: IgniteCfg, storagePath: NPath): Resource[F, Ignite] =
    Resource.make(
      mkCfg(cfg).flatMap(persistentCfg(_, storagePath)).flatMap(startIgnite(_)).flatMap((i: Ignite) => addMe(i) as i)){ (ignite: Ignite) =>
      stopIgnite(ignite)
//      removeMe(ignite) *> stopIgnite(ignite)
    }


}

object IgniteStore {
  def apply[F[_]: Async: ContextShift, K, V](
      cache: IgniteCache[K, V],
      pool: BlockingContext)
      : IndexedStore[F, K, V] = {
    new IgniteStore(cache, pool)
  }

  def igfToF[F[_]: Async: ContextShift, A](igf: IgniteFuture[A]): F[A] = {
    if (igf.isDone) {
      igf.get.pure[F]
    }
    else {
      Async[F].async { (cb: Either[Throwable, A] => Unit) =>
        val _ = igf.listen { (ig: IgniteFuture[A]) => cb(Right(ig.get)) }
      } productL ContextShift[F].shift
    }
  }
}
