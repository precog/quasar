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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.connector.{EnvironmentError, EnvErrT}
import quasar.fp.ski._
import quasar.fs._
import quasar.physical.mongodb.MapReduce._
import quasar.physical.mongodb.execution._
import quasar.physical.mongodb.mongoiterable._
import quasar.physical.mongodb.workflow.$SortF
import quasar.physical.mongodb.workflowtask._

import java.lang.{Boolean => JBoolean}
import scala.Predef.classOf

import com.mongodb._
import com.mongodb.async.client._
import com.mongodb.client.model.CountOptions
import org.bson.{BsonDocument, BsonValue}
import scalaz._, Scalaz._

/** Implementation class for a WorkflowExecutor in the `MongoDbIO` monad. */
private[mongodb] final class MongoDbIOWorkflowExecutor
  extends WorkflowExecutor[MongoDbIO, BsonCursor] {

  private def foldS[F[_]: Foldable, S, A](fa: F[A])(f: (A, S) => S): State[S, Unit] =
    fa.traverseS_[S, Unit](a => MonadState[State[S,?], S].modify(f(a, _)))

  protected def aggregate(src: Collection, pipeline: Pipeline) =
    MongoDbIO.aggregate(src, pipeline map (_.bson), true)

  protected def aggregateCursor(src: Collection, pipeline: Pipeline) =
    toCursor(
      MongoDbIO.aggregateIterable(src, pipeline map (_.bson), true)
        .map(_.useCursor(new JBoolean(true))))

  protected def count(src: Collection, cfg: Count) = {
    val qry =
      cfg.query.fold(Bson.Doc())(_.bson)

    val countOpts = List(
      foldS(cfg.skip)((n, opts: CountOptions) => opts.skip(n.toInt)),
      foldS(cfg.limit)((n, opts: CountOptions) => opts.limit(n.toInt))
    ).sequenceS_[CountOptions, Unit].exec(new CountOptions)

    MongoDbIO.collection(src)
      .flatMap(c => MongoDbIO.async[java.lang.Long](c.count(qry, countOpts, _)))
      .map(_.longValue)
  }

  protected def distinct(src: Collection, cfg: Distinct, field: BsonField.Name) = {
    type DIT = DistinctIterable[BsonValue]
    type MIT = MongoIterable[BsonDocument]

    val wrapVal: BsonValue => BsonDocument =
      new BsonDocument(field.asText, _)

    val distinct0 =
      foldS(cfg.query)((q, dit: DIT) => dit.filter(q.bson))
        .flatMap(κ(State.iModify[DIT, MIT](Functor[MongoIterable].lift(wrapVal))))

    toCursor(MongoDbIO.collection(src) map (c =>
      distinct0.exec(c.distinct(cfg.field.asText, classOf[BsonValue]))))
  }

  protected def drop(c: Collection) =
    MongoDbIO.dropCollection(c)

  protected def find(src: Collection, cfg: Find) = {
    type FIT = FindIterable[BsonDocument]

    val configure = List(
      foldS(cfg.query)((q, fit: FIT) => fit.filter(q.bson)),
      foldS(cfg.projection)((p, fit: FIT) => fit.projection(p)),
      foldS(cfg.sort)((keys, fit: FIT) => fit.sort($SortF.keyBson(keys))),
      foldS(cfg.skip)((n, fit: FIT) => fit.skip(n.toInt)),
      foldS(cfg.limit)((n, fit: FIT) => fit.limit(n.toInt))
    ).sequenceS_[FIT, Unit].exec _

    toCursor(MongoDbIO.collection(src) map (c => configure(c.find)))
  }

  protected def insert(dst: Collection, values: List[Bson.Doc]) =
    MongoDbIO.insert(dst, values map (_.repr))

  protected def mapReduce(src: Collection, dst: OutputCollection, mr: MapReduce) =
    MongoDbIO.mapReduce(src, dst, mr)

  protected def mapReduceCursor(src: Collection, mr: MapReduce) =
    toCursor(MongoDbIO.mapReduceIterable(src, mr))

  private def toCursor[I <: MongoIterable[BsonDocument]](
    bcIO: MongoDbIO[I]
  ): MongoDbIO[BsonCursor] =
    bcIO flatMap (bc => MongoDbIO.async(bc.batchCursor))
}

private[mongodb] object MongoDbIOWorkflowExecutor {
  import EnvironmentError._

  /** Catch MongoExceptions and attempt to convert to EnvironmentError. */
  val liftEnvErr: MongoDbIO ~> EnvErrT[MongoDbIO, ?] =
    new (MongoDbIO ~> EnvErrT[MongoDbIO, ?]) {
      def apply[A](m: MongoDbIO[A]) = EitherT(m.attemptMongo.run flatMap {
        case -\/(UnhandledFSError(ex)) => ex match {
          case _: MongoSocketOpenException =>
            connectionFailed(ex).left.point[MongoDbIO]
          case _: MongoSocketException =>
            connectionFailed(ex).left.point[MongoDbIO]
          case _ =>
            if (ex.getMessage contains "Command failed with error 18: 'auth failed'")
              invalidCredentials(ex.getMessage).left.point[MongoDbIO]
            else MongoDbIO.fail(ex)
        }
        case \/-(a) => a.right.point[MongoDbIO]
      })
    }
}
