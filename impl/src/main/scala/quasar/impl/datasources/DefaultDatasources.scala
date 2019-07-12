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

package quasar.impl.datasources

import slamdata.Predef.{Boolean, Exception, None, Option, Some, Unit}

import quasar.{Condition, IdStatus, RenderTreeT, ScalarStages}
import quasar.api.SchemaConfig
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.contrib.iota._
import quasar.impl.storage.IndexedStore
import quasar.qscript.{construction, educatedToTotal, InterpretedRead, QScriptEducated}

import scala.concurrent.duration._

import cats.effect.Sync

import fs2.Stream

import matryoshka.{BirecursiveT, EqualT, ShowT}

import scalaz.{\/, -\/, \/-, EitherT, Equal, ISet, OptionT}
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import shims._

final class DefaultDatasources[
    F[_]: Sync, I: Equal, C: Equal, OldS <: SchemaConfig, S <: SchemaConfig,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    R] private (
    freshId: F[I],
    refs: IndexedStore[F, I, DatasourceRef[C]],
    errors: DatasourceErrors[F, I],
    manager: DatasourceManager[I, C, T, F, Stream[F, ?], R, ResourcePathType],
    oldSchema: ResourceSchema[F, OldS, (ResourcePath, R)],
    schema: ResourceSchema[F, S, (ResourcePath, R)])
    extends Datasources[F, Stream[F, ?], I, C, OldS, S] {

  type PathType = ResourcePathType

  def addDatasource(ref: DatasourceRef[C]): F[CreateError[C] \/ I] =
    for {
      i <- freshId
      c <- addRef[CreateError[C]](i, ref)
    } yield Condition.disjunctionIso.get(c).as(i)

  def allDatasourceMetadata: F[Stream[F, (I, DatasourceMeta)]] =
    Sync[F].pure(refs.entries.evalMap {
      case (i, DatasourceRef(k, n, _)) =>
        errors.datasourceError(i)
          .map(e => (i, DatasourceMeta.fromOption(k, n, e)))
    })

  def datasourceRef(datasourceId: I): F[ExistentialError[I] \/ DatasourceRef[C]] =
    EitherT(lookupRef[ExistentialError[I]](datasourceId))
      .map(manager.sanitizedRef)
      .run

  def datasourceStatus(datasourceId: I): F[ExistentialError[I] \/ Condition[Exception]] =
    EitherT(lookupRef[ExistentialError[I]](datasourceId))
      .flatMap(_ => EitherT.rightT(errors.datasourceError(datasourceId)))
      .map(Condition.optionIso.reverseGet(_))
      .run

  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean] =
    withDatasource[ExistentialError[I], Boolean](datasourceId)(_.pathIsResource(path))

  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ Stream[F, (ResourceName, ResourcePathType)]] =
    withDatasource[DiscoveryError[I], Option[Stream[F, (ResourceName, ResourcePathType)]]](
      datasourceId)(
      _.prefixedChildPaths(prefixPath))
      .map(_.flatMap(_ \/> DatasourceError.pathNotFound[DiscoveryError[I]](prefixPath)))

  def removeDatasource(datasourceId: I): F[Condition[ExistentialError[I]]] =
    refs.delete(datasourceId).ifM(
      manager.shutdownDatasource(datasourceId).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(datasourceNotFound[I, ExistentialError[I]](datasourceId)).point[F])

  def replaceDatasource(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[DatasourceError[I, C]]] =
    affectsRunning(datasourceId, ref) flatMap {
      case \/-(true) => addRef[DatasourceError[I, C]](datasourceId, ref)

      case \/-(false) => setRef(datasourceId, ref)

      case -\/(err) => Condition.abnormal(err).point[F]
    }

  def oldResourceSchema(
      datasourceId: I,
      path: ResourcePath,
      schemaConfig: OldS,
      timeLimit: FiniteDuration)
      : F[DiscoveryError[I] \/ Option[schemaConfig.Schema]] =
    withDatasource[DiscoveryError[I], Option[schemaConfig.Schema]](datasourceId) { mds =>
      val fr = mds match {
        case ManagedDatasource.ManagedLightweight(lw) =>
          lw.evaluate(InterpretedRead(path, ScalarStages.Id))

        case ManagedDatasource.ManagedHeavyweight(hw) =>
          hw.evaluate(dsl.Read(path, IdStatus.ExcludeId))
      }

      fr.flatMap(r => oldSchema(schemaConfig, (path, r), timeLimit))
    }

  def resourceSchema(
      datasourceId: I,
      path: ResourcePath,
      schemaConfig: S)
      : F[DiscoveryError[I] \/ Option[schemaConfig.Schema]] =
    withDatasource[DiscoveryError[I], Option[schemaConfig.Schema]](datasourceId) { mds =>
      val fr = mds match {
        case ManagedDatasource.ManagedLightweight(lw) =>
          lw.evaluate(InterpretedRead(path, ScalarStages.Id))

        case ManagedDatasource.ManagedHeavyweight(hw) =>
          hw.evaluate(dsl.Read(path, IdStatus.ExcludeId))
      }

      fr.flatMap(r => schema(schemaConfig, (path, r), 0.seconds))
    }

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    manager.supportedDatasourceTypes

  ////

  private val dsl = construction.mkGeneric[T, QScriptEducated[T, ?]]

  /** Add the ref at the specified id, replacing any running datasource. */
  private def addRef[E >: CreateError[C] <: DatasourceError[I, C]](
      i: I, ref: DatasourceRef[C])
      : F[Condition[E]] = {

    type L[X[_], A] = EitherT[X, E, A]
    type M[A] = L[F, A]

    val added = for {
      _ <- EitherT(verifyNameUnique[E](ref.name, i))

      _ <- EitherT(manager.initDatasource(i, ref) map {
        case Condition.Normal() => ().right
        case Condition.Abnormal(e) => (e: E).left
      })

      _ <- refs.insert(i, ref).liftM[L]
    } yield ()

    added.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  /** Returns whether the changes implied by the updated ref
    * affect the running datasource (config or kind changed, etc).
    */
  private def affectsRunning(datasourceId: I, updated: DatasourceRef[C])
      : F[DatasourceError[I, C] \/ Boolean] =
    EitherT(lookupRef[DatasourceError[I, C]](datasourceId)).map {
      case DatasourceRef(k, _, c) => (updated.kind =/= k) || (updated.config =/= c)
    }.run

  private def lookupRef[E >: ExistentialError[I] <: DatasourceError[I, C]](
      datasourceId: I)
      : F[E \/ DatasourceRef[C]] =
    OptionT(refs.lookup(datasourceId))
      .toRight(datasourceNotFound[I, E](datasourceId))
      .run

  /** Sets the ref for the specified datasource id without affecting the running
    * instance.
    */
  private def setRef(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[DatasourceError[I, C]]] = {
    val set =
      for {
        _ <- EitherT(verifyNameUnique[DatasourceError[I, C]](ref.name, datasourceId))
        _ <- EitherT.rightT(refs.insert(datasourceId, ref))
      } yield ()

    set.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def verifyNameUnique[E >: CreateError[C] <: DatasourceError[I, C]](
      name: DatasourceName,
      currentId: I)
      : F[E \/ Unit] =
    refs.entries
      .exists(t => t._2.name === name && t._1 =/= currentId)
      .compile.fold(false)(_ || _)
      .map(_ ? datasourceNameExists[E](name).left[Unit] | ().right)

  private def withDatasource[E >: ExistentialError[I] <: DatasourceError[I, C], A](
      datasourceId: I)(
      f: ManagedDatasource[T, F, Stream[F, ?], R, ResourcePathType] => F[A])
      : F[E \/ A] =
    manager.managedDatasource(datasourceId) flatMap {
      case Some(ds) =>
        f(ds).map(_.right[E])

      case None =>
        DatasourceError.datasourceNotFound[I, E](datasourceId)
          .left[A].point[F]
    }
}

object DefaultDatasources {
  def apply[
      F[_]: Sync, I: Equal, C: Equal, OldS <: SchemaConfig, S <: SchemaConfig,
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      R](
      freshId: F[I],
      refs: IndexedStore[F, I, DatasourceRef[C]],
      errors: DatasourceErrors[F, I],
      manager: DatasourceManager[I, C, T, F, Stream[F, ?], R, ResourcePathType],
      oldSchema: ResourceSchema[F, OldS, (ResourcePath, R)],
      schema: ResourceSchema[F, S, (ResourcePath, R)])
      : Datasources[F, Stream[F, ?], I, C, OldS, S] =
    new DefaultDatasources(freshId, refs, errors, manager, oldSchema, schema)
}
