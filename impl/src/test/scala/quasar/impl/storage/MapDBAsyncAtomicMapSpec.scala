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

import cats.effect.IO

import quasar.concurrent.BlockingContext

import io.atomix.core._
import io.atomix.core.map._
import io.atomix.utils.time.Versioned
import io.atomix.cluster.discovery._
import io.atomix.protocols.backup.partition._
import org.mapdb._

import scalaz.std.string._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import java.nio.file.{Paths, Path => NPath}

import org.specs2.specification._

final class MapDBAsyncAtomicMapSpec extends IndexedStoreSpec[IO, String, String] with AfterAll with BeforeAll{
  val pool = BlockingContext.cached("mapdb-async-atomic-map")
//  sequential
  val atomix: Atomix = Atomix.builder()
    .withMemberId("member-1")
    .withAddress("localhost", 5000)
    .withMulticastEnabled()
    .withManagementGroup(PrimaryBackupPartitionGroup.builder("system")
      .withNumPartitions(1)
      .build())
    .withPartitionGroups(PrimaryBackupPartitionGroup.builder("data")
      .withNumPartitions(32)
      .build())
    .build();

  def beforeAll: Unit = atomix.start().join()
  def afterAll: Unit = atomix.stop().join()

  def emptyStore: IO[IndexedStore[IO, String, String]] = {
    val mkMap = for {
      mapName <- IO(java.util.UUID.randomUUID().toString)
      aMap <- IO{
        atomix.atomicMapBuilder[String, String](mapName)
//          .withCacheEnabled
//          .withCacheSize(1)
          .build()
          .async()
      }
      db <- IO(DBMaker.fileDB(Paths.get(mapName ++ ".db").toFile)
        .checksumHeaderBypass
//        .transactionEnable
        .closeOnJvmShutdown
        .fileLockDisable
        .make)
      mMap <- IO{
        db.hashMap("map")
          .keySerializer(Serializer.STRING)
          .valueSerializer(MapDBAsyncAtomicMap.versionedSerializer(Serializer.STRING))
          .createOrOpen

      }
      map <- MapDBAsyncAtomicMap[IO, String, String](aMap, mMap, pool, (() => db.commit()))
    } yield (atomix, map)
    mkMap flatMap {
      case (_, None) =>
        emptyStore
      case (atomix, Some(m)) => IO(AsyncAtomicIndexedStore[IO, String, String](m))
    }
  }

  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)
}
