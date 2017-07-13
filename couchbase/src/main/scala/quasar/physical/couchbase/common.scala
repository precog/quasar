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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.{Data, DataCodec}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadReader_
import quasar.fs.FileSystemError

import scala.collection.JavaConverters._

import com.couchbase.client.java.{Bucket, Cluster}
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{N1qlParams, N1qlQuery, N1qlQueryRow}
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.view.{DefaultView, DesignDocument, View, ViewQuery, Stale}
import pathy.Path, Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object common {

  type ContextReader[F[_]] = MonadReader_[F, Context]

  object ContextReader {
    def apply[F[_]](implicit R: MonadReader_[F, Context]) = R
  }

  final case class BucketName(v: String)

  final case class Config(ctx: ClientContext, cluster: Cluster)

  final case class ClientContext(bucket: Bucket, docTypeKey: DocTypeKey, lcView: ListContentsView)

  final case class Context(bucket: BucketName, docTypeKey: DocTypeKey)

  final case class Collection(bucket: Bucket, docTypeValue: DocTypeValue)

  final case class DocTypeKey(v: String)
  final case class DocTypeValue(v: String)

  final case class Cursor(result: Vector[Data])

  final case class ListContentsView(docTypeKey: DocTypeKey) {
    val designDocName: String = s"quasar_${docTypeKey.v}"
    val viewName: String = s"lc_${docTypeKey.v}"

    val view: View =
      DefaultView.create(
        viewName,
        s"""function (doc, meta) { emit(null, doc["${docTypeKey.v}"]); }""",
        """function(key, values, rereduce) {
          |  var o = {};
          |
          |  values.forEach(function(v) {
          |    if (rereduce) {
          |      Object.keys(v).forEach(function(i) {
          |        o[i] = 0;
          |      });
          |    }
          |    else {
          |      o[v] = 0;
          |    }
          |  });
          |
          |  return o;
          |}""".stripMargin)

    val designDoc: DesignDocument =
      DesignDocument.create(designDocName, List(view).asJava)

    val query: ViewQuery =
      ViewQuery
        .from(designDocName, viewName)
        .stale(Stale.FALSE)
  }

  val CBDataCodec = DataCodec.Precise

  def docTypeValueFromPath(p: APath): DocTypeValue =
    DocTypeValue(Path.flatten(None, None, None, Some(_), Some(_), p).toIList.unite.intercalate("/"))

  def deleteHavingPrefix(
    ctx: ClientContext,
    prefix: String
  ): Task[FileSystemError \/ Unit] = {
    val qStr = s"""DELETE FROM `${ctx.bucket.name}` WHERE `${ctx.docTypeKey.v}` LIKE "${prefix}%""""

    query(ctx.bucket, qStr).map(_.void)
  }

  def docTypeValuesFromPrefix(
    ctx: ClientContext,
    prefix: String
  ): Task[FileSystemError \/ List[DocTypeValue]] = Task.delay {
    ctx.bucket.query(ctx.lcView.query).allRows.asScala.toList.traverseM {
      _.value match {
        case o: JsonObject =>
          o.getNames.asScala.toList.collect {
            case v if v.startsWith(prefix) => DocTypeValue(v)
          }.right
        case _ =>
          FileSystemError.readFailed(ctx.lcView.viewName, "not a JsonObject").left
      }
    }
  }

  def existsWithPrefix(
    ctx: ClientContext,
    prefix: String
  ): Task[FileSystemError \/ Boolean] =
    docTypeValuesFromPrefix(ctx, prefix) ∘ (_ ∘ (_.nonEmpty))

  def pathSegments(paths: List[List[String]]): Set[PathSegment] =
    paths.collect {
      case h :: Nil => FileName(h).right
      case h :: _   => DirName(h).left
    }.toSet

  def pathSegmentsFromPrefixDocTypeValues(prefix: String, types: List[DocTypeValue]): Set[PathSegment] =
    pathSegments(types.map(_.v.stripPrefix(prefix).stripPrefix("/").split("/").toList))

  def query(bucket: Bucket, query: String): Task[FileSystemError \/ Vector[N1qlQueryRow]] =
    Task.delay {
      val qr = bucket.query(N1qlQuery.simple(
        query, N1qlParams.build().consistency(ScanConsistency.STATEMENT_PLUS)))
      qr.errors.asScala.toVector.toNel.cata(
        errors => FileSystemError.readFailed(
          query,"[" ⊹ errors.map(_.toString).intercalate(", ") ⊹ "]").left,
        qr.allRows.asScala.toVector.right)
    }

  def queryData(bucket: Bucket, query: String): Task[FileSystemError \/ Vector[Data]] =
    this.query(bucket, query) ∘ (_ >>= (_.traverse(rowToData)))

  def resultsFromCursor(cursor: Cursor): (Cursor, Vector[Data]) =
    (Cursor(Vector.empty), cursor.result)

  def rowToData(row: N1qlQueryRow): FileSystemError \/ Data = {
    val rowStr = new String(row.byteValue)

    DataCodec.parse(rowStr)(DataCodec.Precise).leftMap(err =>
      FileSystemError.readFailed(rowStr, err.shows))
  }
}
