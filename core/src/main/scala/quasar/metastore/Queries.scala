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

package quasar.metastore

import slamdata.Predef._
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.fs.mount.cache.ViewCache
import quasar.fs.mount.{MountConfig, MountType}, MountConfig.FileSystemConfig

import java.time.Instant

import doobie.imports._
import pathy.Path, Path._

/** Raw query and update terms, which may be `checked` against a DB schema
  * without requiring or affecting any data.
  */
trait Queries {
  val fsMounts: Query0[(APath, FileSystemConfig)] =
    sql"SELECT path, type, connectionUri FROM Mounts WHERE (type != 'view' AND type != 'module')".query[(APath, FileSystemConfig)]

  def mounts: Query0[PathedMountConfig]  =
    sql"SELECT * FROM Mounts".query[PathedMountConfig]

  def mountsHavingPrefix(dir: ADir): Query0[(APath, MountType)] = {
    val patternChars = List("\\\\", "_", "%") // NB: Order is important here to avoid double-escaping
    val patternEscaped = patternChars.foldLeft(posixCodec.printPath(dir))((s, c) =>
                           s.replaceAll(c, s"\\\\$c"))
    sql"SELECT path, type FROM Mounts WHERE path LIKE ${patternEscaped + "_%"}"
      .query[(APath, MountType)]
  }

  def lookupMountType(path: APath): Query0[MountType] =
    sql"SELECT type FROM Mounts WHERE path = ${refineType(path)}".query[MountType]

  def lookupMountConfig(path: APath): Query0[Mount] =
    sql"SELECT type, connectionUri FROM Mounts WHERE path = ${refineType(path)}".query[Mount]

  def insertMount(path: APath, cfg: MountConfig): Update0 = {
    val mnt = Mount.fromMountConfig(cfg)
    insertPathedMountConfig(PathedMountConfig(path, mnt.`type`, mnt.uri))
  }

  def insertPathedMountConfig(pmc: PathedMountConfig): Update0 =
    sql"""INSERT INTO Mounts (path, type, connectionUri)
          VALUES (${refineType(pmc.path)}, ${pmc.mt}, ${pmc.uri})
          """.update

  def deleteMount(path: APath): Update0 =
    sql"DELETE FROM Mounts where path = ${refineType(path)}".update

  val viewCachePaths: Query0[AFile] =
    sql"SELECT path FROM view_cache".query[AFile]

  def viewCaches: Query0[PathedViewCache] =
    sql"SELECT * FROM view_cache".query[PathedViewCache]

  def lookupViewCache(path: AFile): Query0[PathedViewCache] =
    sql"SELECT * FROM view_cache WHERE path = $path".query[PathedViewCache]

  def insertViewCache(path: AFile, viewCache: ViewCache): Update0 =
    sql"""INSERT INTO view_cache values (
            $path,
            ${viewCache.viewConfig.asUri},
            ${viewCache.lastUpdate},
            ${viewCache.executionMillis},
            ${viewCache.cacheReads},
            ${viewCache.assignee},
            ${viewCache.assigneeStart},
            ${viewCache.maxAgeSeconds},
            ${viewCache.refreshAfter},
            ${viewCache.status},
            ${viewCache.errorMsg},
            ${viewCache.dataFile},
            ${viewCache.tmpDataFile})""".update

  def updateViewCache(path: AFile, viewCache: ViewCache): Update0 =
    sql"""UPDATE view_cache
          SET
            path = $path,
            query = ${viewCache.viewConfig.asUri},
            last_update = ${viewCache.lastUpdate},
            execution_millis = ${viewCache.executionMillis},
            cache_reads = ${viewCache.cacheReads},
            assignee = ${viewCache.assignee},
            assignee_start = ${viewCache.assigneeStart},
            max_age_seconds = ${viewCache.maxAgeSeconds},
            refresh_after = ${viewCache.refreshAfter},
            status = ${viewCache.status},
            error_msg = ${viewCache.errorMsg},
            data_file = ${viewCache.dataFile},
            tmp_data_file = ${viewCache.tmpDataFile}
          WHERE PATH = $path""".update

  def updateViewCacheErrorMsg(path: AFile, errorMsg: String): Update0 =
    sql"""UPDATE view_cache
          SET
            status = ${ViewCache.Status.Failed: ViewCache.Status},
            error_msg = $errorMsg
          WHERE PATH = $path""".update

  def deleteViewCache(path: AFile): Update0 =
    sql"""DELETE FROM view_cache WHERE path = $path""".update

  def staleCachedViews(now: Instant): Query0[PathedViewCache] =
    sql"""SELECT *
          FROM view_cache
          WHERE $now > refresh_after""".query[PathedViewCache]

  def cacheRefreshAssigneStart(path: AFile, assigneeId: String, start: Instant, tmpDataPath: AFile): Update0 =
    sql"""UPDATE view_cache
          SET assignee = $assigneeId, assignee_start = $start, tmp_data_file = $tmpDataPath
          WHERE path = $path""".update

  def updatePerSuccesfulCacheRefresh(path: AFile, lastUpdate: Instant, executionMillis: Long, refreshAfter: Instant): Update0 =
    sql"""UPDATE view_cache
          SET
            assignee = null,
            last_update = $lastUpdate,
            execution_millis = $executionMillis,
            refresh_after = $refreshAfter,
            status = 'successful',
            error_msg = null,
            tmp_data_file = null
          WHERE path = $path""".update
}

object Queries extends Queries
