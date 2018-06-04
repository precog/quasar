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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._
import quasar.common.PhaseResultT
import quasar.contrib.pathy._
import quasar.contrib.scalaz.kleisli._
import quasar.fs._
import quasar.contrib.iota.copkTraverse
import quasar.physical.mongodb._

import matryoshka._
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

final case class QueryContext(
  statistics: Collection => Option[CollectionStatistics],
  indexes: Collection => Option[Set[Index]])

object QueryContext {

  import queryfileTypes._
  import FileSystemError._

  type MongoLogWF[C, A]  = PhaseResultT[QueryRT[MongoDbIO, C, ?], A]
  type MongoLogWFR[C, A] = FileSystemErrT[MongoLogWF[C, ?], A]

  def paths[T[_[_]]: BirecursiveT](qs: T[MongoDb.QSM[T, ?]]): ISet[APath] =
    ISet.fromFoldable(
      qs.cata(qscript.ExtractPath[MongoDb.QSM[T, ?], APath].extractPath[DList]))

  def collections[T[_[_]]: BirecursiveT](qs: T[MongoDb.QSM[T, ?]]): PathError \/ Set[Collection] =
    // NB: documentation on `QueryFile` guarantees absolute paths, so calling `mkAbsolute`
    paths(qs).toList
      .flatMap(f => maybeFile(f).toList)
      .traverse(file => Collection.fromFile(mkAbsolute(rootDir, file)))
      .map(_.toSet)

  def queryContext[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT, M[_]]
    (qs: T[MongoDb.QSM[T, ?]])
      : MQPhErr[QueryContext] = {

    def lift[A](fa: FileSystemErrT[MongoDbIO, A]): MongoLogWFR[BsonCursor, A] =
      EitherT[MongoLogWF[BsonCursor, ?], FileSystemError, A](
        fa.run.liftM[QueryRT[?[_], BsonCursor, ?]].liftM[PhaseResultT])

    val qctx: FileSystemErrT[MongoDbIO, QueryContext] =
      for {
        colls <- EitherT.fromDisjunction[MongoDbIO](collections(qs) leftMap (pathErr(_)))
        stats <- byCollection(colls, MongoDbIO.collectionStatistics).liftM[FileSystemErrT]
        idxs  <- byCollection(colls, MongoDbIO.indexes).liftM[FileSystemErrT]
      } yield QueryContext(stats.get, idxs.get)

    lift(qctx).run.run
  }

  ////

  private def byCollection[A](colls: Set[Collection], f: Collection => MongoDbIO[A])
      : MongoDbIO[Map[Collection, A]] =
    colls.foldLeftM(Map[Collection, A]()) { (m, c) =>
      f(c) map (m.updated(c, _))
    }
}
