/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.api.{QueryEvaluator, SchemaConfig}
import quasar.api.datasource.{DatasourceRef, DatasourceType, Datasources}
import quasar.api.destination.{DestinationRef, DestinationType, Destinations}
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.api.table.{TableRef, Tables}
import quasar.common.PhaseResultTell
import quasar.connector.{Datasource, DestinationModule, QueryResult}
import quasar.contrib.std.uuid._
import quasar.ejson.implicits._
import quasar.impl.DatasourceModule
import quasar.impl.datasource.{AggregateResult, CompositeResult}
import quasar.impl.datasources._
import quasar.impl.datasources.middleware._
import quasar.impl.destinations.{DefaultDestinationManager, DefaultDestinations}
import quasar.impl.storage.IndexedStore
import quasar.impl.table.DefaultTables
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
import scalaz.{IMap, Show, ~>}
import scalaz.syntax.show._
import shims.{monadToScalaz, functorToCats, functorToScalaz, orderToScalaz}

final class Quasar[F[_], R, C <: SchemaConfig](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json, C],
    val destinations: Destinations[F, Stream[F, ?], UUID, Json],
    val tables: Tables[F, UUID, SqlQuery],
    val queryEvaluator: QueryEvaluator[F, SqlQuery, R])

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object Quasar extends Logging {

  type EvalResult[F[_]] = Either[QueryResult[F], AggregateResult[F, QSMap[Fix, QueryResult[F]]]]

  type LookupRunning[F[_]] = UUID => F[Option[ManagedDatasource[Fix, F, Stream[F, ?], EvalResult[F], ResourcePathType]]]

  /** What it says on the tin. */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer, R, C <: SchemaConfig](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      destinationRefs: IndexedStore[F, UUID, DestinationRef[Json]],
      tableRefs: IndexedStore[F, UUID, TableRef[SqlQuery]],
      qscriptEvaluator: LookupRunning[F] => QueryEvaluator[F, Fix[QScriptEducated[Fix, ?]], R])(
      datasourceModules: List[DatasourceModule],
      destinationModules: List[DestinationModule],
      resourceSchema: ResourceSchema[F, C, (ResourcePath, CompositeResult[F, QueryResult[F]])])(
      implicit
      ec: ExecutionContext)
      : Resource[F, Quasar[F, R, C]] = {

    for {
      configured <-
        Resource.liftF(
          datasourceRefs.entries
            .compile.fold(IMap.empty[UUID, DatasourceRef[Json]])(_ + _))

      _ <- Resource.liftF(warnDuplicates[F, DatasourceModule, DatasourceType](datasourceModules)(_.kind))
      _ <- Resource.liftF(warnDuplicates[F, DestinationModule, DestinationType](destinationModules)(_.destinationType))

      (dsErrors, onCondition) <- Resource.liftF(DefaultDatasourceErrors[F, UUID])

      moduleMap = IMap.fromList(datasourceModules.map(ds => ds.kind -> ds))

      dsManager <-
        DefaultDatasourceManager.Builder[UUID, Fix, F]
          .withMiddleware(AggregatingMiddleware(_, _))
          .withMiddleware(ConditionReportingMiddleware(onCondition)(_, _))
          .build(moduleMap, configured)

      destModules = IMap.fromList(destinationModules.map(dest => dest.destinationType -> dest))

      freshUUID = Sync[F].delay(UUID.randomUUID)

      destManager <- Resource.liftF(DefaultDestinationManager.empty[UUID, F](destModules))
      destinations = DefaultDestinations[UUID, Json, F](freshUUID, destinationRefs, destManager)

      datasources =
        DefaultDatasources(freshUUID, datasourceRefs, dsErrors, dsManager, resourceSchema)

      lookupRunning =
        (id: UUID) => dsManager.managedDatasource(id).map(_.map(_.modify(reifiedAggregateDs)))

      sqlEvaluator = Sql2QueryEvaluator(qscriptEvaluator(lookupRunning))

      tables = DefaultTables(freshUUID, tableRefs)

    } yield new Quasar(datasources, destinations, tables, sqlEvaluator)
  }

  ////

  private def warnDuplicates[F[_]: Sync, A, B: Show](l: List[A])(fn: A => B): F[Unit] =
    Sync[F].delay(l.groupBy(fn) foreach {
      case (a, grouped) =>
        if (grouped.length > 1) {
          log.warn(s"Found duplicate modules for type ${a.shows}")
        }
    })

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
