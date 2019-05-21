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
import quasar.api.destination.DestinationRef
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.api.table.{TableRef, Tables}
import quasar.common.PhaseResultTell
import quasar.connector.{Datasource, DestinationModule, QueryResult}
import quasar.contrib.std.uuid._
import quasar.ejson.EJson
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
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.functor._
import fs2.Stream
import matryoshka.data.Fix
import org.slf4s.Logging
import scalaz.{IMap, ~>}
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

  type LookupRunning[F[_]] = UUID => F[Option[ManagedDatasource[Fix, F, Stream[F, ?], EvalResult[F], ResourcePathType]]]

  /** What it says on the tin. */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer, R, S](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      destinationRefs: IndexedStore[F, UUID, DestinationRef[Json]],
      tableRefs: IndexedStore[F, UUID, TableRef[SqlQuery]],
      qscriptEvaluator: LookupRunning[F] => QueryEvaluator[F, Fix[QScriptEducated[Fix, ?]], R],
      preparationsManager: QueryEvaluator[F, SqlQuery, R] => Resource[F, PreparationsManager[F, UUID, SqlQuery, R]],
      lookupTableData: UUID => F[Option[R]],
      lookupTableSchema: UUID => F[Option[S]])(
      datasourceModules: List[DatasourceModule],
      destinationModules: List[DestinationModule],
      sstEvalConfig: SstEvalConfig)(
      implicit
      ec: ExecutionContext)
      : Resource[F, Quasar[F, R, S]] = {

    for {
      configured <-
        Resource.liftF(
          datasourceRefs.entries
            .compile.fold(IMap.empty[UUID, DatasourceRef[Json]])(_ + _))

      _ <- Resource.liftF(Sync[F] delay {
        datasourceModules.groupBy(_.kind) foreach {
          case (kind, sources) =>
            if (sources.length > 1) {
              log.warn(s"Found duplicate modules for type ${kind.shows}")
            }
        }
      })

      (dsErrors, onCondition) <- Resource.liftF(DefaultDatasourceErrors[F, UUID])

      moduleMap = IMap.fromList(datasourceModules.map(ds => ds.kind -> ds))

      dsManager <-
        DefaultDatasourceManager.Builder[UUID, Fix, F]
          .withMiddleware(AggregatingMiddleware(_, _))
          .withMiddleware(ConditionReportingMiddleware(onCondition)(_, _))
          .build(moduleMap, configured)

      freshUUID = Sync[F].delay(UUID.randomUUID)

      resourceSchema = SimpleCompositeResourceSchema[F, Fix[EJson], Double](sstEvalConfig)

      datasources =
        DefaultDatasources(freshUUID, datasourceRefs, dsErrors, dsManager, resourceSchema)

      lookupRunning =
        (id: UUID) => dsManager.managedDatasource(id).map(_.map(_.modify(reifiedAggregateDs)))

      sqlEvaluator = Sql2QueryEvaluator(qscriptEvaluator(lookupRunning))

      prepManager <- preparationsManager(sqlEvaluator)

      tables =
        DefaultTables(freshUUID, tableRefs, sqlEvaluator, prepManager, lookupTableData, lookupTableSchema)

    } yield new Quasar(datasources, tables, sqlEvaluator)
  }

  ////

  private val rec = construction.RecFunc[Fix]

  private def reifiedAggregateDs[F[_]: Functor, G[_], P <: ResourcePathType]
      : Datasource[F, G, ?, CompositeResult[F, QueryResult[F]], P] ~> Datasource[F, G, ?, EvalResult[F], P] =
    new (Datasource[F, G, ?, CompositeResult[F, QueryResult[F]], P] ~> Datasource[F, G, ?, EvalResult[F], P]) {
      def apply[A](ds: Datasource[F, G, A, CompositeResult[F, QueryResult[F]], P]) = {
        val l = Datasource.pevaluator[F, G, A, CompositeResult[F, QueryResult[F]], A, EvalResult[F], P]
        l.modify(_.map(_.map(reifyAggregateStructure)))(ds)
      }
    }

  private def reifyAggregateStructure[F[_], A](s: Stream[F, (ResourcePath, A)])
      : Stream[F, (ResourcePath, QSMap[Fix, A])] =
    s map { case (rp, a) =>
      (rp, QSMap(a, rec.Hole))
    }
}
