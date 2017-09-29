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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._
import quasar.common.PhaseResultT
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fs._
import quasar.physical.mongodb._

import matryoshka._
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

final case class QueryContext[M[_]](
    statistics: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]],
    listContents: qscript.DiscoverPath.ListContents[M])

object QueryContext {

  import queryfileTypes._
  import FileSystemError._

  type MongoLogWF[C, A] = PhaseResultT[QueryRT[MongoDbIO, C, ?], A]
  type MongoLogWFR[C, A] = FileSystemErrT[MongoLogWF[C, ?], A]

  def paths[T[_[_]]: BirecursiveT](qs: T[MongoDb.QSM[T, ?]]): ISet[APath] =
    ISet.fromFoldable(qs.cata(qscript.ExtractPath[MongoDb.QSM[T, ?], APath].extractPath[DList]))

  def collections[T[_[_]]: BirecursiveT](
      qs: T[MongoDb.QSM[T, ?]]): PathError \/ Set[Collection] =
    // NB: documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    paths(qs).toList
      .flatMap(f => maybeFile(f).toList)
      .traverse(file => Collection.fromFile(mkAbsolute(rootDir, file)))
      .map(_.toSet)

  def lookup[T[_[_]]: BirecursiveT, A](
      qs: T[MongoDb.QSM[T, ?]],
      f: Collection => MongoDbIO[A]): EitherT[MongoDbIO, FileSystemError, Map[Collection, A]] =
    for {
      colls <- EitherT.fromDisjunction[MongoDbIO](collections(qs).leftMap(pathErr(_)))
      a <- colls.toList.traverse(c => f(c).strengthL(c)).map(Map(_: _*)).liftM[FileSystemErrT]
    } yield a

  def queryContext[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, M[_]](
      qs: T[MongoDb.QSM[T, ?]],
      lc: qscript.DiscoverPath.ListContents[M]): MQPhErr[QueryContext[M]] = {

    def lift[A](fa: FileSystemErrT[MongoDbIO, A]): MongoLogWFR[BsonCursor, A] =
      EitherT[MongoLogWF[BsonCursor, ?], FileSystemError, A](
        fa.run.liftM[QueryRT[?[_], BsonCursor, ?]].liftM[PhaseResultT])

    val x: FileSystemErrT[MongoDbIO, QueryContext[M]] =
      (lookup(qs, MongoDbIO.collectionStatistics) |@|
        lookup(qs, MongoDbIO.indexes))((stats, idxs) =>
        QueryContext(stats.get(_), idxs.get(_), lc))

    lift(x).run.run
  }
}
