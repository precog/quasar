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

package quasar.run

import slamdata.Predef._

import quasar.api.QueryEvaluator
import quasar.api.datasource.{DatasourceRef, Datasources}
import quasar.api.resource.ResourcePath
import quasar.api.table.{TableRef, Tables}
import quasar.common.PhaseResultTell
import quasar.connector.{Datasource, QueryResult}
import quasar.contrib.std.uuid._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.impl.DatasourceModule
import quasar.impl.datasource.{AggregateResult, CompositeResult}
import quasar.impl.datasources._
import quasar.impl.datasources.middleware._
import quasar.impl.schema.{SstConfig, SstEvalConfig}
import quasar.impl.storage.IndexedStore
import quasar.impl.table.{DefaultTables, PreparationsManager}
import quasar.qscript.{QScriptEducated, construction, Map => QSMap}
import quasar.run.implicits._

import java.util.UUID

import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.JsonScalaz._

import cats.Functor
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.contravariant._
import cats.syntax.functor._

import fs2.Stream

import matryoshka.data.Fix

import org.slf4s.Logging

import pathy.Path.posixCodec

import scalaz.{~>, IMap}
import scalaz.syntax.show._

import shims._

import spire.std.double._

final class Quasar[F[_], R, S](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json, SstConfig[Fix[EJson], Double]],
    val tables: Tables[F, UUID, SqlQuery, R, S],
    val queryEvaluator: QueryEvaluator[F, SqlQuery, R])

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object Quasar extends Logging {

  type EvalResult[F[_]] = Either[QueryResult[F], AggregateResult[F, QSMap[Fix, QueryResult[F]]]]

  type LookupRunning[F[_]] = UUID => F[Option[ManagedDatasource[Fix, F, Stream[F, ?], EvalResult[F]]]]

  /** What it says on the tin. */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer, R, S](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      tableRefs: IndexedStore[F, UUID, TableRef[SqlQuery]],
      qscriptEvaluator: LookupRunning[F] => QueryEvaluator[F, Fix[QScriptEducated[Fix, ?]], R],
      preparationsManager: QueryEvaluator[F, SqlQuery, R] => Stream[F, PreparationsManager[F, UUID, SqlQuery, R]],
      lookupTableData: UUID => F[Option[R]],
      lookupTableSchema: UUID => F[Option[S]])(
      datasourceModules: List[DatasourceModule],
      sstEvalConfig: SstEvalConfig)(
      implicit
      ec: ExecutionContext)
      : Stream[F, Quasar[F, R, S]] = {

    for {
      configured <- datasourceRefs.entries.fold(IMap.empty[UUID, DatasourceRef[Json]])(_ + _)

      _ <- Stream.eval(Sync[F] delay {
        datasourceModules.groupBy(_.kind) foreach {
          case (kind, sources) =>
            if (sources.length > 1) {
              log.warn(s"Found duplicate modules for type ${kind.shows}")
            }
        }
      })

      (dsErrors, onCondition) <- Stream.eval(DefaultDatasourceErrors[F, UUID])

      moduleMap = IMap.fromList(datasourceModules.map(ds => ds.kind -> ds))

      dsManager <-
        Stream.resource(DefaultDatasourceManager.Builder[UUID, Fix, F]
          .withMiddleware(ConditionReportingMiddleware(onCondition)(_, _))
          .withMiddleware(ChildAggregatingMiddleware(SourceKey, ValueKey)(_, _))
          .build(moduleMap, configured))

      freshUUID = ConcurrentEffect[F].delay(UUID.randomUUID)

      resourceSchema = CompositeResourceSchema[F, Fix[EJson], Double](sstEvalConfig, SourceKey, ValueKey)

      datasources =
        DefaultDatasources[F, UUID, Json, SstConfig[Fix[EJson], Double], Fix, EvalResult[F]](
          freshUUID, datasourceRefs, dsErrors, dsManager, toEvalResultSchema(resourceSchema))

      lookupRunning =
        (id: UUID) => dsManager.managedDatasource(id)

      sqlEvaluator = Sql2QueryEvaluator(qscriptEvaluator(lookupRunning))

      prepManager <- preparationsManager(sqlEvaluator)

      tables =
        DefaultTables(freshUUID, tableRefs, sqlEvaluator, prepManager, lookupTableData, lookupTableSchema)

    } yield new Quasar(datasources, tables, sqlEvaluator)
  }

  ////

  private val SourceKey = "source"
  private val ValueKey = "value"

  private val rec = construction.RecFunc[Fix]
  private val ejs = Fixed[Fix[EJson]]

  def toEvalResultSchema[F[_], J, N](
      compositeSchema: ResourceSchema[F, SstConfig[J, N], (ResourcePath, CompositeResult[F, QueryResult[F]])])
      : ResourceSchema[F, SstConfig[J, N], (ResourcePath, EvalResult[F])] =
    compositeSchema.contramap {
      case (rp, evalResult) => (rp, toCompositeResult(evalResult))
    }

  def toCompositeResult[F[_]](evalResult: EvalResult[F]): CompositeResult[F, QueryResult[F]] =
    // Just returning `src` of the FreeMap here as opposed to changing the ResourceSchema implementation
    // to apply the FreeMap to each result of the aggregate
    evalResult.fold(Left(_), ar => Right(ar.map { case (rp, fm) => (rp, fm.src) }))
}
