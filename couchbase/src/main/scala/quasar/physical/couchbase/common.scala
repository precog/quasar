/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.effect.Read
import quasar.fp.free._
import quasar.fs.{PathError, FileSystemError}

import scala.collection.JavaConverters._

import com.couchbase.client.java.{Bucket, Cluster}
import com.couchbase.client.java.cluster.ClusterManager
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery, N1qlQueryRow}
import com.couchbase.client.java.query.consistency.ScanConsistency
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object common {

  final case class Context(cluster: Cluster, manager: ClusterManager)

  final case class BucketCollection(bucket: String, collection: String)

  final case class DocIdType(id: String, tpe: String)

  final case class Cursor(result: Vector[Data])

  def bucketCollectionFromPath(f: APath): FileSystemError \/ BucketCollection =
    Path.flatten(None, None, None, Some(_), Some(_), f)
      .toIList.unite.uncons(
        FileSystemError.pathErr(PathError.invalidPath(f, "no bucket specified")).left,
        (h, t) => BucketCollection(h, t.intercalate("/")).right)

  def docIdTypesWithTypePrefix[S[_]](
    bucket: Bucket,
    prefix: String
  ): Task[List[DocIdType]] = Task.delay {
    val qStr = s"""SELECT meta(`${bucket.name}`).id, type FROM `${bucket.name}`
                   WHERE type LIKE "${prefix}%"""";

    bucket.query(n1qlQuery(qStr)).allRows.asScala.toList
      .map(r => DocIdType(r.value.getString("id"), r.value.getString("type")))
  }

  def existsWithPrefix[S[_]](
    bucket: Bucket,
    prefix: String
  ): Task[Boolean] = Task.delay {
    val qStr = s"""SELECT count(*) > 0 v FROM `${bucket.name}`
                   WHERE type = "${prefix}" OR type like "${prefix}/%""""

    bucket.query(n1qlQuery(qStr)).allRows.asScala.toList
      .exists(_.value.getBoolean("v").booleanValue === true)
  }

  def pathSegments(paths: List[List[String]]): Set[PathSegment] =
    paths.collect {
      case h :: Nil => FileName(h).right
      case h :: _   => DirName(h).left
    }.toSet

  def pathSegmentsFromBucketCollections(bktCols: List[BucketCollection]): Set[PathSegment] =
    pathSegments(bktCols.map(bc => bc.bucket :: bc.collection.split("/").toList))

  def pathSegmentsFromPrefixDocIds(prefix: String, docIds: List[DocIdType]): Set[PathSegment] =
    pathSegments(docIds.map(_.tpe.stripPrefix(prefix).stripPrefix("/").split("/").toList))

  def n1qlQuery(query: String): N1qlQuery =
    N1qlQuery.simple(
      query,
      N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS))

  def getBucket[S[_]](
    name: String
  )(implicit
    S0: Task :<: S,
    context: Read.Ops[Context, S]
  ): Free[S, FileSystemError \/ Bucket] =
    context.ask.flatMap(ctx => lift(
      Task.delay(ctx.manager.hasBucket(name).booleanValue).ifM(
        Task.delay(ctx.cluster.openBucket(name).right),
        Task.now(FileSystemError.pathErr(PathError.pathNotFound(rootDir </> dir(name))).left))
    ).into)

  def resultsFromCursor(cursor: Cursor): (Cursor, Vector[Data]) =
    (Cursor(Vector.empty), cursor.result)

  def rowToData(row: N1qlQueryRow): FileSystemError \/ Data = {
    val rowStr = new String(row.byteValue)

    DataCodec.parse(rowStr)(DataCodec.Precise).leftMap(err =>
      FileSystemError.readFailed(rowStr, err.shows))
  }

}
