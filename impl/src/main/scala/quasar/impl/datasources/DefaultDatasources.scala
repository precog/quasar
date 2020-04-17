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
import quasar.contrib.scalaz.MonadError_
import quasar.impl.{CachedGetter, IndexedSemaphore, QuasarDatasource, ResourceManager}, CachedGetter.Signal._
import quasar.impl.storage.IndexedStore

import cats.effect.{Concurrent, ContextShift, Sync, Resource}
import cats.~>

import fs2.Stream

import scalaz.{\/, -\/, \/-, ISet, EitherT, Equal}
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

import shims.{monadToScalaz, equalToCats}
import shims.effect.scalazEitherTSync

private[quasar] final class DefaultDatasources[
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
    c <- addRef[CreateError[C]](i, ref)
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
      dispose(i).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(datasourceNotFound[I, ExistentialError[I]](i)).point[F])

  def replaceDatasource(i: I, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] =
    throughSemaphore[F](i, λ[F ~> F](x => x)) apply {
      lazy val notFound = Condition.abnormal(datasourceNotFound[I, DatasourceError[I, C]](i))

      getter(i) flatMap {
        // We're replacing, emit abnormal condition if there was no ref
        case Empty =>
          notFound.point[F]
        case Removed(_) =>
          // it's removed, but resource hasn't been finalized
          dispose(i).as(notFound)
        case existed => for {
          // We have a ref, start replacement
          _ <- refs.insert(i, ref)
          signal <- getter(i)
          res <- signal match {
            case Inserted(_) =>
              addRef[DatasourceError[I, C]](i, ref)
            case Preserved(_) =>
              Condition.normal[DatasourceError[I, C]]().point[F]
            case Updated(incoming, old) if DatasourceRef.atMostRenamed(incoming, old) =>
              setRef(i, incoming)
            case Updated(_, _) =>
              dispose(i) >> addRef[DatasourceError[I, C]](i, ref)
            // These two cases can't happen.
            case Empty =>
              notFound.point[F]
            case Removed(_) =>
              dispose(i).as(notFound)
          }
        } yield res
      }
    }

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.supportedTypes

  type QDS = QuasarDatasource[T, G, H, R, ResourcePathType]

  def quasarDatasourceOf(i: I): F[Option[QDS]] =
    getQDS[ExistentialError[I]](i).toOption.run

  private def getQDS[E >: ExistentialError[I] <: DatasourceError[I, C]](i: I): EitherT[F, E, QDS] = {
    type Res[A] = EitherT[F, E, A]
    type L[M[_], A] = EitherT[M, E, A]
    lazy val error: Res[QDS] = EitherT.pureLeft(datasourceNotFound[I, E](i))
    lazy val fromCache: Res[QDS] = cache.get(i).liftM[L] flatMap {
      case None => error
      case Some(a) => EitherT.pure(a)
    }
    throughSemaphore[Res](i, λ[F ~> Res](x => x.liftM[L])).apply {
      getter(i).liftM[L] flatMap {
        case Empty =>
          error
        case Removed(_) =>
          dispose(i).liftM[L] >> error
        case Inserted(ref) =>
          cache.get(i).liftM[L] flatMap {
            case None =>
              for {
                allocated <- createErrorHandling(modules.create(i, ref)).allocated.liftM[L]
                _ <- cache.manage(i, allocated).liftM[L]
              } yield allocated._1
            case Some(a) => EitherT.pure(a)
          }
        case Updated(incoming, old) if DatasourceRef.atMostRenamed(incoming, old) =>
          fromCache
        case Preserved(_) =>
          fromCache
        case Updated(ref, _) => for {
          _ <- dispose(i).liftM[L]
          allocated <- createErrorHandling(modules.create(i, ref)).allocated.liftM[L]
          _ <- cache.manage(i, allocated).liftM[L]
        } yield allocated._1
      }
    }
  }


  private def addRef[E >: CreateError[C] <: DatasourceError[I, C]](i: I, ref: DatasourceRef[C]): F[Condition[E]] = {
    val action = for {
      _ <- verifyNameUnique[E](ref.name, i)
      // Grab managed ds and if it's presented shut it down
      mbCurrent <- EitherT.rightT(cache.get(i))
      _ <- EitherT.rightT(mbCurrent.fold(().point[F])(_ => dispose(i)))
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

  private def dispose(i: I): F[Unit] =
    cache.shutdown(i) >> byteStores.clear(i)

  private val createErrorHandling: EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?] =
    λ[EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?]]( inp =>
      inp.run.flatMap(_.fold(
        (x: CreateError[C]) => Resource.liftF(MonadError_[F, CreateError[C]].raiseError(x)),
        _.point[Resource[F, ?]])))

  private def throughSemaphore[G[_]: Sync](i: I, fg: F ~> G): G ~> G = λ[G ~> G]{ ga =>
    semaphore.get(i).mapK(fg).use(_ => ga)
  }
}

object DefaultDatasources {
  def apply[
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
