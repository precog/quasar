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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.Condition
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.connector.datasource.Reconfiguration
import quasar.contrib.scalaz.MonadError_
import quasar.impl.{CachedGetter, IndexedSemaphore, QuasarDatasource, ResourceManager}, CachedGetter.Signal._
import quasar.impl.storage.IndexedStore

import cats.{~>, Applicative}
import cats.effect.{Concurrent, ContextShift, Sync, Resource}

import fs2.Stream

import scalaz.{\/, -\/, \/-, ISet, EitherT, Equal, OptionT}
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import shims.{monadToScalaz, equalToCats}

private[impl] final class DefaultDatasources[
    T[_[_]],
    F[_]: Sync: MonadError_[?[_], CreateError[C]],
    G[_], H[_],
    I: Equal, C: Equal, R] private (
    semaphore: IndexedSemaphore[F, I],
    freshId: F[I],
    refs: IndexedStore[F, I, DatasourceRef[C]],
    modules: DatasourceModules[T, F, G, H, I, C, R, ResourcePathType],
    getter: CachedGetter[F, I, DatasourceRef[C]],
    cache: ResourceManager[F, I, QuasarDatasource[T, G, H, R, ResourcePathType]],
    errors: DatasourceErrors[F, I],
    byteStores: ByteStores[F, I])
    extends Datasources[F, Stream[F, ?], I, C] {

  def addDatasource(ref: DatasourceRef[C]): F[CreateError[C] \/ I] = for {
    i <- freshId
    c <- addRef[CreateError[C]](i, Reconfiguration.Preserve, ref)
  } yield Condition.disjunctionIso.get(c).as(i)

  def allDatasourceMetadata: F[Stream[F, (I, DatasourceMeta)]] =
    Sync[F].pure(refs.entries.evalMap {
      case (i, DatasourceRef(k, n, _)) =>
        errors.datasourceError(i) map { e =>
          (i, DatasourceMeta.fromOption(k, n, e))
        }
    })

  def datasourceRef(i: I): F[ExistentialError[I] \/ DatasourceRef[C]] =
    EitherT(lookupRef[ExistentialError[I]](i))
      .map(modules.sanitizeRef(_))
      .run

  def datasourceStatus(i: I): F[ExistentialError[I] \/ Condition[Exception]] =
    EitherT(lookupRef[ExistentialError[I]](i))
      .flatMap(_ => EitherT.rightT(errors.datasourceError(i)))
      .map(Condition.optionIso.reverseGet(_))
      .run

  def removeDatasource(i: I): F[Condition[ExistentialError[I]]] =
    refs.delete(i).ifM(
      dispose(i, true).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(datasourceNotFound[I, ExistentialError[I]](i)).point[F])

  def replaceDatasource(i: I, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] =
    replaceDatasourceImpl(i, Reconfiguration.Reset, ref)

  private def replaceDatasourceImpl(i: I, reconf: Reconfiguration, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] = {
    lazy val notFound =
      Condition.abnormal(datasourceNotFound[I, DatasourceError[I, C]](i))

    def doReplace(prev: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] =
      if (ref === prev)
        Condition.normal[DatasourceError[I, C]]().point[F]
      else if (DatasourceRef.atMostRenamed(ref, prev))
        setRef(i, ref)
      else
        addRef[DatasourceError[I, C]](i, reconf, ref)

    throughSemaphore(i) {
      getter(i).flatMap {
        // We're replacing, emit abnormal condition if there was no ref
        case Empty =>
          notFound.point[F]

        // it's removed, but resource hasn't been finalized
        case Removed(_) =>
          dispose(i, true).as(notFound)

        // The last value we knew about has since been externally updated,
        // but our update replaces it, so we ignore the new value and
        // replace the old.
        case Updated(_, old) =>
          doReplace(old)

        case Present(value) =>
          doReplace(value)
      } <* getter(i)
    }
  }

  def reconfigureDatasource(datasourceId: I, patch: C)
      : F[Condition[DatasourceError[I, C]]] =
    lookupRef[DatasourceError[I, C]](datasourceId) flatMap {
      case -\/(err) => Condition.abnormal(err: DatasourceError[I, C]).point[F]
      case \/-(ref) => modules.reconfigureRef(ref, patch) match {
        case Left(err) =>
          Condition.abnormal(err: DatasourceError[I, C]).point[F]

        case Right((reconf, patched)) =>
          replaceDatasourceImpl(datasourceId, reconf, patched)
      }
    }

  def renameDatasource(datasourceId: I, name: DatasourceName)
      : F[Condition[DatasourceError[I, C]]] =
    lookupRef[DatasourceError[I, C]](datasourceId) flatMap {
      case -\/(err) => Condition.abnormal(err: DatasourceError[I, C]).point[F]
      case \/-(ref) => replaceDatasource(datasourceId, ref.copy(name = name))
    }

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.supportedTypes

  type QDS = QuasarDatasource[T, G, H, R, ResourcePathType]

  def quasarDatasourceOf(i: I): F[Option[QDS]] = {
    def create(ref: DatasourceRef[C]): F[QDS] =
      for {
        allocated <- createErrorHandling(modules.create(i, ref)).allocated
        _ <- cache.manage(i, allocated)
      } yield allocated._1

    def fromCacheOrCreate(ref: DatasourceRef[C]): F[QDS] =
      OptionT(cache.get(i)) getOrElseF create(ref)

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          (None: Option[QDS]).pure[F]

        case Removed(_) =>
          dispose(i, true).as(None: Option[QDS])

        case Updated(incoming, old) if DatasourceRef.atMostRenamed(incoming, old) =>
          fromCacheOrCreate(incoming).map(_.some)

        case Updated(incoming, old) =>
          dispose(i, true) >> create(incoming).map(_.some)

        case Present(value) =>
          fromCacheOrCreate(value).map(_.some)
      }
    }
  }

  private def addRef[E >: CreateError[C] <: DatasourceError[I, C]](i: I, reconf: Reconfiguration, ref: DatasourceRef[C]): F[Condition[E]] = {
    val clearBS = reconf match {
      case Reconfiguration.Reset => true
      case Reconfiguration.Preserve => false
    }

    val action = for {
      _ <- verifyNameUnique[E](ref.name, i)
      // Grab managed ds and if it's presented shut it down
      mbCurrent <- EitherT.rightT(cache.get(i))
      _ <- EitherT.rightT(mbCurrent.fold(().point[F])(_ => dispose(i, clearBS)))
      allocated <- EitherT(modules.create(i, ref).run.allocated map {
        case (-\/(e), _) => -\/(e: E)
        case (\/-(a), finalize) => \/-((a, finalize))
      })
      _ <- EitherT.rightT(refs.insert(i, ref))
      _ <- EitherT.rightT(cache.manage(i, allocated))
    } yield ()

    action.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def setRef(i: I, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] = {
    val action = for {
      _ <- verifyNameUnique[DatasourceError[I, C]](ref.name, i)
      _ <- EitherT.rightT(refs.insert(i, ref))
    } yield ()

    action.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def lookupRef[E >: ExistentialError[I] <: DatasourceError[I, C]](i: I): F[E \/ DatasourceRef[C]] =
    refs.lookup(i).map {
      case None => datasourceNotFound[I, E](i).left
      case Some(a) => a.right
    }

  private def verifyNameUnique[E >: CreateError[C] <: DatasourceError[I, C]](name: DatasourceName, i: I): EitherT[F, E, Unit] =
    EitherT {
      refs.entries
        .exists(t => t._2.name === name && t._1 =/= i)
        .compile
        .fold(false)(_ || _)
        .map(_ ? datasourceNameExists[E](name).left[Unit] | ().right)
    }

  private def dispose(i: I, clear: Boolean): F[Unit] =
    cache.shutdown(i) >> (if (clear) byteStores.clear(i) else Applicative[F].unit)

  private val createErrorHandling: EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?] =
    λ[EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?]]( inp =>
      inp.run.flatMap(_.fold(
        (x: CreateError[C]) => Resource.liftF(MonadError_[F, CreateError[C]].raiseError(x)),
        _.point[Resource[F, ?]])))

  private def throughSemaphore(i: I): F ~> F =
    λ[F ~> F](fa => semaphore.get(i).use(_ => fa))
}

object DefaultDatasources {
  private[impl] def apply[
      T[_[_]],
      F[_]: Concurrent: ContextShift: MonadError_[?[_], CreateError[C]],
      G[_], H[_],
      I: Equal, C: Equal, R](
      freshId: F[I],
      refs: IndexedStore[F, I, DatasourceRef[C]],
      modules: DatasourceModules[T, F, G, H, I, C, R, ResourcePathType],
      cache: ResourceManager[F, I, QuasarDatasource[T, G, H, R, ResourcePathType]],
      errors: DatasourceErrors[F, I],
      byteStores: ByteStores[F, I])
      : F[DefaultDatasources[T, F, G, H, I, C, R]] = for {
    semaphore <- IndexedSemaphore[F, I]
    getter <- CachedGetter(refs.lookup(_))
  } yield new DefaultDatasources(semaphore, freshId, refs, modules, getter, cache, errors, byteStores)
}
