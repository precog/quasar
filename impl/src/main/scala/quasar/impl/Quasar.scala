/*
 * Copyright 2020 Precog Data
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

package quasar.impl

import slamdata.Predef._

import quasar.{Condition, RateLimiting, Store}
import quasar.api.QueryEvaluator
import quasar.api.datasource.{DatasourceRef, DatasourceType, Datasources}
import quasar.api.destination.{DestinationRef, DestinationType, Destinations}
import quasar.api.discovery.{Discovery, SchemaConfig}
import quasar.api.intentions.Intentions
import quasar.api.push.{OffsetKey, Push, ResultPush}
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.api.scheduler.SchedulerType
import quasar.api.scheduler.{Schedulers, SchedulerRef}
import quasar.common.PhaseResultTell
import quasar.connector.{Offset, QueryResult, ResourceSchema}
import quasar.connector.datasource.Datasource
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import quasar.connector.evaluate._
import quasar.connector.render.ResultRender
import quasar.connector.scheduler.{Scheduler, SchedulerBuilder}
import quasar.ejson.implicits._
import quasar.impl.datasource.{AggregateResult, CompositeResult}
import quasar.impl.datasources._
import quasar.impl.datasources.middleware._
import quasar.impl.destinations._
import quasar.impl.discovery.DefaultDiscovery
import quasar.impl.evaluate._
import quasar.impl.implicits._
import quasar.impl.intentions.DefaultIntentions
import quasar.impl.push.DefaultResultPush
import quasar.impl.scheduler._
import quasar.impl.storage.{IndexedStore, PrefixStore}
import quasar.qscript.{construction, Map => QSMap}

import java.lang.Runtime
import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.JsonScalaz._

import cats.{~>, Functor, Show}
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.kernel.Hash
import cats.implicits._

import fs2.Stream

import matryoshka.data.Fix

import org.slf4s.Logging

import shapeless._

import shims.{monadToScalaz, functorToCats, functorToScalaz, orderToScalaz, showToCats, equalToCats}

import skolems.∃

final class Quasar[F[_], R, C <: SchemaConfig](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json],
    val destinations: Destinations[F, Stream[F, ?], UUID, Json],
    val schedulers: Schedulers.Aux[F, Stream[F, ?], UUID, Json, SchedulerBuilder[F], SchedulerType],
    val queryEvaluator: QueryEvaluator[Resource[F, ?], SqlQuery, R],
    val discovery: Discovery[Resource[F, ?], Stream[F, ?], UUID, C],
    val resultPush: ResultPush[F, UUID, SqlQuery],
    val intentions: Intentions[F, Stream[F, ?], UUID, Array[Byte], Json])

object Quasar extends Logging {

  type EvalResult[F[_]] = Either[QueryResult[F], AggregateResult[F, QSMap[Fix, QueryResult[F]]]]

  /** What it says on the tin. */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer, R, C <: SchemaConfig, A: Hash](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      destinationRefs: IndexedStore[F, UUID, DestinationRef[Json]],
      schedulerRefs: IndexedStore[F, UUID, SchedulerRef[Json]],
      pushes: PrefixStore[F, UUID :: ResourcePath :: HNil, ∃[Push[?, SqlQuery]]],
      offsets: Store[F, UUID :: ResourcePath :: HNil, ∃[OffsetKey.Actual]],
      queryFederation: QueryFederation[Fix, Resource[F, ?], QueryAssociate[Fix, Resource[F, ?], EvalResult[F]], R],
      resultRender: ResultRender[F, R],
      resourceSchema: ResourceSchema[F, C, (ResourcePath, CompositeResult[F, QueryResult[F]])],
      rateLimiting: RateLimiting[F, A],
      byteStores: ByteStores[F, UUID],
      pushPull: PushmiPullyu[F])(
      maxConcurrentPushes: Int,
      datasourceModules: List[DatasourceModule],
      destinationModules: List[DestinationModule],
      schedulerBuilders: List[SchedulerBuilder[F]])(
      implicit
      ec: ExecutionContext)
      : Resource[F, Quasar[F, R, C]] = {

    val destModules =
      DestinationModules[F](destinationModules, pushPull)

    for {
      _ <- Resource.liftF(warnDuplicates[F, DatasourceModule, DatasourceType](datasourceModules)(_.kind))
      _ <- Resource.liftF(warnDuplicates[F, DestinationModule, DestinationType](destinationModules)(_.destinationType))

      modulesByKind = datasourceModules.groupBy(_.kind) collect {
        case (kind, List(module)) => (kind, module)
      }

      concurrency <- Resource.liftF(Sync[F].delay(Runtime.getRuntime.availableProcessors))

      _ <- migrateDatasourceConfigs[F](datasourceRefs, modulesByKind)(concurrency)

      (dsErrors, onCondition) <- Resource.liftF(DefaultDatasourceErrors[F, UUID])

      report = (id: UUID, c: Condition[Throwable]) => c match {
        case cond @ Condition.Abnormal(ex: Exception) => onCondition(id, Condition.abnormal(ex))
        case Condition.Abnormal(_) => ().pure[F]
        case Condition.Normal() => onCondition(id, Condition.normal())
      }

      dsModules =
        DatasourceModules[Fix, F, UUID, A](datasourceModules, rateLimiting, byteStores)
          .withMiddleware(AggregatingMiddleware(_, _))
          .withMiddleware(ConditionReportingMiddleware(report)(_, _))

      freshUUID = Sync[F].delay(UUID.randomUUID)

      dsCache <- ResourceManager[
        F, UUID, QuasarDatasource[Fix, Resource[F, ?], Stream[F, ?], CompositeResult[F, QueryResult[F]], ResourcePathType]]
      datasources <- Resource.liftF(DefaultDatasources(freshUUID, datasourceRefs, dsModules, dsCache, dsErrors, byteStores))

      destCache <- ResourceManager[F, UUID, Destination[F]]
      destinations <- Resource.liftF(DefaultDestinations(freshUUID, destinationRefs, destCache, destModules))

      sBuilders <- Resource.liftF(SchedulerBuilders[F](schedulerBuilders))
      sCache <- ResourceManager[F, UUID, Scheduler[F, Array[Byte], Json]]
      schedulers <- Resource.liftF(DefaultSchedulers(freshUUID, schedulerRefs, sCache, sBuilders))

      lookupRunning =
        (id: UUID) => datasources.quasarDatasourceOf(id).map(_.map(_.modify(reifiedAggregateDs)))

      sqlEvaluator =
        Sql2Compiler[Fix, F]
          .first[Option[Offset]]
          .andThen(QueryFederator(ResourceRouter(UuidString, lookupRunning)))
          .mapF(Resource.liftF(_))
          .andThen(queryFederation)

      discovery = DefaultDiscovery(datasources.quasarDatasourceOf, resourceSchema)

      resultPush <-
        DefaultResultPush[F, UUID, SqlQuery, R](
          maxConcurrentPushes,
          destinations.destinationOf(_).map(_.toOption),
          sqlEvaluator,
          resultRender,
          pushes,
          offsets)

      intentions = DefaultIntentions(
        schedulerRefs.entries.map(_._1),
        schedulers.schedulerOf(_: UUID).map(_.toOption))

    } yield {
      new Quasar[F, R, C](
        datasources,
        destinations,
        schedulers,
        sqlEvaluator.local((_, None)),
        discovery,
        resultPush,
        intentions)
    }
  }

  ////

  private def warnDuplicates[F[_]: Sync, A, B: Show](l: List[A])(fn: A => B): F[Unit] =
    Sync[F].delay(l.groupBy(fn) foreach {
      case (b, grouped) =>
        if (grouped.length > 1) {
          log.warn(s"Found duplicate modules for type ${b.show}")
        }
    })

  private val rec = construction.RecFunc[Fix]

  private def reifiedAggregateDs[F[_]: Functor, G[_], H[_], P <: ResourcePathType]
      : Datasource[F, G, ?, CompositeResult[H, QueryResult[H]], P] ~> Datasource[F, G, ?, EvalResult[H], P] =
    new (Datasource[F, G, ?, CompositeResult[H, QueryResult[H]], P] ~> Datasource[F, G, ?, EvalResult[H], P]) {
      def apply[A](ds: Datasource[F, G, A, CompositeResult[H, QueryResult[H]], P]) = {
        val l = Datasource.ploaders[F, G, A, CompositeResult[H, QueryResult[H]], A, EvalResult[H], P]
        l.modify(_.map(_.map(reifyAggregateStructure)))(ds)
      }
    }

  private def reifyAggregateStructure[F[_], A](s: Stream[F, (ResourcePath, A)])
      : Stream[F, (ResourcePath, QSMap[Fix, A])] =
    s map { case (rp, a) =>
      (rp, QSMap(a, rec.Hole))
    }

  private def migrateDatasourceConfigs[F[_]: Concurrent](
      datasourceRefs: IndexedStore[F, UUID, DatasourceRef[Json]],
      modules: Map[DatasourceType, DatasourceModule])(
      concurrency: Int)
      : Resource[F, Unit] = {
    val inserts: Stream[F, Unit] =
      datasourceRefs.entries.parEvalMap[F, Unit](concurrency) {
        case (id, ref) =>
          modules.get(ref.kind) match {
            case Some(module) =>
              module.migrateConfig(ref.config) match {
                case Right(c) =>
                  if (c === ref.config)
                    ().pure[F]
                  else
                    datasourceRefs.insert(id, ref.copy(config = c))
                case Left(_) => ().pure[F]
              }
            case None => ().pure[F]
          }
      }

    inserts.compile.resource.drain
  }
}
