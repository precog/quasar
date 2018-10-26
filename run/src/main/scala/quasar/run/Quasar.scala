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

import slamdata.Predef.{Array, Double, SuppressWarnings}
import quasar.api.QueryEvaluator
import quasar.api.datasource.{DatasourceRef, Datasources}
import quasar.api.table.Tables
import quasar.common.PhaseResultTell
import quasar.contrib.pathy.ADir
import quasar.contrib.std.uuid._
import quasar.ejson.EJson
import quasar.impl.DatasourceModule
import quasar.impl.datasource.local.LocalDatasourceModule
import quasar.impl.datasources.{DatasourceManagement, DefaultDatasources}
import quasar.impl.evaluate.FederatingQueryEvaluator
import quasar.impl.external.{ExternalConfig, ExternalDatasources}
import quasar.impl.schema.{SstConfig, SstEvalConfig}
import quasar.impl.table.{DefaultTables, PreparationsManager}
import quasar.mimir.{MimirRepr, Precog}
import quasar.mimir.evaluate.{MimirQueryFederation, Pushdown, PushdownControl}
import quasar.mimir.storage.{MimirIndexedStore, MimirPTableStore, PTableSchema, StoreKey}
import quasar.run.implicits._
import quasar.run.optics._

import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut.Json
import argonaut.JsonScalaz._
import cats.~>
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.flatMap._
import fs2.Stream
import matryoshka.data.Fix
import pathy.Path._
import scalaz.IMap
import scalaz.syntax.foldable._
import scalaz.syntax.functor._
import shims._
import spire.std.double._

final class Quasar[F[_]](
    val datasources: Datasources[F, Stream[F, ?], UUID, Json, SstConfig[Fix[EJson], Double]],
    val tables: Tables[F, UUID, SqlQuery, Stream[F, MimirRepr], PTableSchema],
    val queryEvaluator: QueryEvaluator[F, SqlQuery, Stream[F, MimirRepr]],
    val pushdown: PushdownControl[F])

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object Quasar {
  // The location of the datasource refs tables within `mimir`.
  val DatasourceRefsLocation: ADir =
    rootDir </> dir("quasar") </> dir("datasource-refs")

  val TableRefsLocation: ADir =
    rootDir </> dir("quasar") </> dir("table-refs")

  val PreparationLocation: ADir =
    rootDir </> dir("quasar") </> dir("preparations")

  /** What it says on the tin.
    *
    * TODO: If we want to divest from `mimir` completely, we'll need to convert
    *       all the abstractions that use it into arguments to this constructor.
    *
    * @param precog Precog instance to use by Quasar
    * @param extConfig datasource plugin configuration
    * @param sstSampleSize the number of records to sample when generating SST schemas
    * @param sstParallelism the number of chunks to process in parallel when generating SST schemas
    */
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer](
      precog: Precog,
      extConfig: ExternalConfig,
      sstEvalConfig: SstEvalConfig,
      blockingPool: ExecutionContext)(
      implicit
      cs: ContextShift[IO],
      ec: ExecutionContext)
      : Stream[F, Quasar[F]] = {

    for {
      pushdownRef <- Stream.eval(Ref.of[F, Pushdown](Pushdown.EnablePushdown))
      pushdown = new PushdownControl(pushdownRef)

      extMods <- ExternalDatasources[F](extConfig, blockingPool)

      modules = extMods.insert(
        LocalDatasourceModule.kind,
        DatasourceModule.Lightweight(LocalDatasourceModule))

      datasourceRefs =
        MimirIndexedStore.transformValue(
          MimirIndexedStore.transformIndex(
            MimirIndexedStore[F](precog, DatasourceRefsLocation),
            "UUID",
            StoreKey.stringIso composePrism stringUuidP),
          "DatasourceRef",
          rValueDatasourceRefP(rValueJsonP))

      tableRefs =
        MimirIndexedStore.transformValue(
          MimirIndexedStore.transformIndex(
            MimirIndexedStore[F](precog, TableRefsLocation),
            "UUID",
            StoreKey.stringIso composePrism stringUuidP),
          "TableRef",
          rValueTableRefP(rValueSqlQueryP))

      configured <- datasourceRefs.entries.fold(IMap.empty[UUID, DatasourceRef[Json]])(_ + _)

      mr <- Stream.bracket(
        DatasourceManagement[Fix, F, UUID, Double](
          modules,
          configured,
          sstEvalConfig))(
        { case (_, r) => r.get.flatMap(_.traverse_(_.dispose)) })

      (mgmt, running) = mr

      freshUUID = ConcurrentEffect[F].delay(UUID.randomUUID)

      datasources = DefaultDatasources[F, UUID, Json, SstConfig[Fix[EJson], Double]](
        freshUUID, datasourceRefs, mgmt, mgmt)

      federation = MimirQueryFederation[Fix, F](precog, pushdown)

      pTableStore = MimirPTableStore[F](precog, PreparationLocation)

      (queryEvaluatorIO: QueryEvaluator[F, SqlQuery, Stream[IO, MimirRepr]]) =
        Sql2QueryEvaluator(FederatingQueryEvaluator(federation, ResourceRouter(running.get)))

      queryEvaluator = queryEvaluatorIO.map(_.translate(λ[IO ~> F](_.to[F])))

      preparationsManager <- PreparationsManager[F, UUID, SqlQuery, Stream[F, MimirRepr]](queryEvaluator) {
        case (key, table) =>
          ConcurrentEffect[F].delay {
            table.flatMap(data =>
              pTableStore.write(
                storeKeyUuidP.reverseGet(key),
                data.table.asInstanceOf[pTableStore.cake.Table])) // yolo
          }
      }

      tables = DefaultTables[F, UUID, SqlQuery, Stream[F, MimirRepr], PTableSchema](
        freshUUID,
        tableRefs,
        queryEvaluator,
        preparationsManager,
        key => pTableStore.read(storeKeyUuidP.reverseGet(key))
          .map(_.map(t =>
            Stream(MimirRepr(pTableStore.cake)(
              t.asInstanceOf[pTableStore.cake.Table])).covary[F])), // yolo x2
        i => pTableStore.schema(storeKeyUuidP.reverseGet(i)))

    } yield new Quasar(datasources, tables, queryEvaluator, pushdown)
  }
}
