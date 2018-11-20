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

import slamdata.Predef._
import quasar.{Condition, Disposable, IdStatus, RenderTreeT}
import quasar.api.datasource.{DatasourceError, DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError.{CreateError, DatasourceUnsupported, DiscoveryError, ExistentialError}
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{
  CompressionScheme,
  Datasource,
  MonadResourceErr,
  ParsableType,
  QueryResult
}
import quasar.connector.ParsableType.JsonVariant
import quasar.contrib.iota._
import quasar.contrib.scalaz._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp.ski.{κ, κ2}
import quasar.impl.DatasourceModule
import quasar.impl.DatasourceModule.{Heavyweight, Lightweight}
import quasar.impl.datasource.{ByNeedDatasource, ConditionReportingDatasource, FailedDatasource}
import quasar.impl.parsing.TectonicResourceError
import quasar.impl.schema._
import quasar.qscript._
import quasar.sst._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

import argonaut.Json
import argonaut.Argonaut.jEmptyObject
import cats.ApplicativeError
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.effect.concurrent.Ref
import fs2.{gzip, Chunk, Stream}
import fs2.concurrent.{Signal, SignallingRef}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import qdata.{QData, QDataEncode}
import qdata.tectonic.QDataPlate
import scalaz.{EitherT, IMap, ISet, Monad, OptionT, Order, Scalaz, \/}, Scalaz._
import shims._
import spire.algebra.Field
import spire.math.ConvertableTo
import tectonic.fs2.StreamParser
import tectonic.json.{Parser => TParser}

final class DatasourceManagement[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr,
    I: Order,
    N: ConvertableTo: Field: Order] private (
    modules: DatasourceManagement.Modules,
    errors: Ref[F, IMap[I, Exception]],
    running: SignallingRef[F, DatasourceManagement.Running[I, T, F]],
    sstEvalConfig: SstEvalConfig)(
    implicit tmr: Timer[F],
    ec: ExecutionContext)
    extends DatasourceControl[F, Stream[F, ?], I, Json, SstConfig[T[EJson], N]]
    with DatasourceErrors[F, I] {

  import DatasourceManagement._

  type DS = DatasourceManagement.DS[T, F]
  type Running = DatasourceManagement.Running[I, T, F]

  // DatasourceControl

  def sanitizeRef(ref: DatasourceRef[Json]): DatasourceRef[Json] = {
    modules.lookup(ref.kind) match {
      case Some(n) => n match {
        case Lightweight(lw) => ref.copy(config = lw.sanitizeConfig(ref.config))
        case Heavyweight(hw) => ref.copy(config = hw.sanitizeConfig(ref.config))
      }
      case _ => ref.copy(config = jEmptyObject)
    }
  }

  def initDatasource(datasourceId: I, ref: DatasourceRef[Json])
      : F[Condition[CreateError[Json]]] = {

    val init0: DatasourceModule => EitherT[F, CreateError[Json], Disposable[F, DS]] = {
      case DatasourceModule.Lightweight(lw) =>
        EitherT(handleLinkageError(
          ref.kind,
          lw.lightweightDatasource[F](ref.config)))
            .bimap(ie => ie: CreateError[Json], _.map(_.left))

      case DatasourceModule.Heavyweight(hw) =>
        EitherT(handleLinkageError(
          ref.kind,
          hw.heavyweightDatasource[T, F](ref.config)))
            .bimap(ie => ie: CreateError[Json], _.map(_.right))
    }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supportedDatasourceTypes)

      mod <-
        OptionT(modules.lookup(ref.kind).point[F])
          .toRight[CreateError[Json]](DatasourceUnsupported(ref.kind, sup))

      ds0 <- init0(mod)

      ds = ds0.map(_.bimap(
        withErrorReporting(errors, datasourceId, _),
        withErrorReporting(errors, datasourceId, _)))

      _ <- EitherT.rightT(modifyAndShutdown { r =>
        (r.insert(datasourceId, ds), r.lookupAssoc(datasourceId))
      })
    } yield ()

    inited.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean] =
    withDatasource[ExistentialError[I], Boolean](datasourceId)(_.pathIsResource(path))

  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ Stream[F, (ResourceName, ResourcePathType)]] =
    withDatasource[DiscoveryError[I], Option[Stream[F, (ResourceName, ResourcePathType)]]](
      datasourceId)(
      _.prefixedChildPaths(prefixPath))
      .map(_.flatMap(_ \/> DatasourceError.pathNotFound[DiscoveryError[I]](prefixPath)))

  def resourceSchema(
      datasourceId: I,
      path: ResourcePath,
      sstConfig: SstConfig[T[EJson], N],
      timeLimit: FiniteDuration)
      : F[DiscoveryError[I] \/ Option[sstConfig.Schema]] = {

    type S = SST[T[EJson], N]

    def sampleQuery =
      dsl.Subset(
        dsl.Unreferenced,
        freeDsl.Read(path, IdStatus.ExcludeId),
        Take,
        freeDsl.Map(
          freeDsl.Unreferenced,
          recFunc.Constant(EJson.int(sstEvalConfig.sampleSize.value))))

    def concatArrayBufs(bufs: List[ArrayBuffer[S]]): ArrayBuffer[S] = {
      val totalSize = bufs.foldLeft(0)(_ + _.length)
      bufs.foldLeft(new ArrayBuffer[S](totalSize))(_ ++= _)
    }

    @tailrec
    def sstStream(qr: QueryResult[F]): Stream[F, S] =
      qr match {
        case QueryResult.Parsed(qdd, data) =>
          data.map(QData.convert(_)(qdd, QDataEncode[S]))

        case QueryResult.Compressed(CompressionScheme.Gzip, content) =>
          sstStream(content.modifyBytes(gzip.decompress[F](DefaultDecompressionBufferSize)))

        case QueryResult.Typed(js @ ParsableType.Json(vnt, isPrecise), data) =>
          val mode: TParser.Mode = vnt match {
            case JsonVariant.ArrayWrapped => TParser.UnwrapArray
            case JsonVariant.LineDelimited => TParser.ValueStream
          }

          val parser =
            TParser(QDataPlate[F, S, ArrayBuffer[S]](isPrecise), mode)

          val parserPipe =
            StreamParser(parser)(
              Chunk.buffer,
              bufs => Chunk.buffer(concatArrayBufs(bufs)))

          data.through(parserPipe) handleErrorWith { t =>
            TectonicResourceError(path, js, t) match {
              case Some(re) =>
                Stream.eval(MonadResourceErr[F].raiseError[S](re))

              case None =>
                Stream.raiseError(t)
            }
          }
      }

    withDs[DiscoveryError[I], Option[sstConfig.Schema]](datasourceId) { ds =>

      val queryResult: F[QueryResult[F]] =
        ds.fold(_.evaluate(path), _.evaluate(sampleQuery))

      val k: N = ConvertableTo[N].fromLong(sstEvalConfig.sampleSize.value)

      Stream.eval(queryResult)
        .flatMap(sstStream)
        .take(sstEvalConfig.sampleSize.value)
        .chunkLimit(sstEvalConfig.chunkSize.value.toInt)
        .through(progressiveSstAsync(sstConfig, sstEvalConfig.parallelism.value.toInt))
        .interruptWhen(ConcurrentEffect[F].attempt(tmr.sleep(timeLimit)))
        .compile.last
        .map(_.map(SstSchema.fromSampled(k, _)))
    }
  }

  def shutdownDatasource(datasourceId: I): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(datasourceId, κ2(None)) match {
      case (v, m) => (m, v strengthL datasourceId)
    })

  val supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.keySet.point[F]


  // DatasourceErrors

  def erroredDatasources: F[IMap[I, Exception]] =
    errors.get

  def datasourceError(datasourceId: I): F[Option[Exception]] =
    errors.get.map(_.lookup(datasourceId))


  ////

  private val dsl = construction.mkGeneric[T, QScriptEducated[T, ?]]
  private val freeDsl = construction.mkFree[T, QScriptEducated[T, ?]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private def modifyAndShutdown(f: Running => (Running, Option[(I, Disposable[F, DS])])): F[Unit] =
    for {
      t <- running.modify(f)

      _  <- t.traverse_ {
        case (id, ds) => errors.update(_ - id) *> ds.dispose
      }
    } yield ()

  private def withDatasource[E >: ExistentialError[I] <: DatasourceError[I, Json], A](
      datasourceId: I)(
      f: Datasource[F, Stream[F, ?], _, _] => F[A])
      : F[E \/ A] =
    withDs[E, A](datasourceId)(ds => f(ds.merge))

  private def withDs[E >: ExistentialError[I] <: DatasourceError[I, Json], A](
      datasourceId: I)(f: DS => F[A]): F[E \/ A] =
    running.get.flatMap(_.lookup(datasourceId) match {
      case Some(ds) =>
        f(ds.unsafeValue).map(_.right[E])

      case None =>
        DatasourceError.datasourceNotFound[I, E](datasourceId)
          .left[A].point[F]
    })
}

object DatasourceManagement {
  type Modules = IMap[DatasourceType, DatasourceModule]
  type LDS[F[_]] = Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]
  type HDS[T[_[_]], F[_]] = Datasource[F, Stream[F, ?], T[QScriptEducated[T, ?]], QueryResult[F]]
  type DS[T[_[_]], F[_]] = LDS[F] \/ HDS[T, F]
  type Running[I, T[_[_]], F[_]] = IMap[I, Disposable[F, DS[T, F]]]

  type MgmtControl[T[_[_]], F[_], I, N] =
      DatasourceControl[F, Stream[F, ?], I, Json, SstConfig[T[EJson], N]]

  // 32k buffer, anything less would be uncivilized.
  val DefaultDecompressionBufferSize: Int = 32768

  final case class IncompatibleDatasourceException(kind: DatasourceType) extends java.lang.RuntimeException {
    override def getMessage = s"Loaded datasource implementation with type $kind is incompatible with quasar"
  }

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadError_[?[_], CreateError[Json]]: Timer,
      I: Order,
      N: ConvertableTo: Field: Order](
      modules: Modules,
      configured: IMap[I, DatasourceRef[Json]],
      sstEvalConfig: SstEvalConfig)(
      implicit ec: ExecutionContext)
      : F[(MgmtControl[T, F, I, N] with DatasourceErrors[F, I], Signal[F, Running[I, T, F]])] = {

    for {
      errors <- Ref.of[F, IMap[I, Exception]](IMap.empty)

      assocs <- configured.toList traverse {
        case (id, ref @ DatasourceRef(kind, _, _)) =>
          modules.lookup(kind) match {
            case None =>
              val ds = FailedDatasource[CreateError[Json], F, Stream[F, ?], ResourcePath, QueryResult[F]](
                kind,
                DatasourceUnsupported(kind, modules.keySet))

              (id, ds.left[HDS[T, F]].point[Disposable[F, ?]]).point[F]

            case Some(mod) =>
              lazyDatasource[T, F](mod, ref).strengthL(id)
          }
      }

      running = IMap.fromList(assocs) mapWithKey { (id, ds) =>
        ds.map(_.bimap(
          withErrorReporting(errors, id, _),
          withErrorReporting(errors, id, _)))
      }

      runningS <- SignallingRef[F, Running[I, T, F]](running)

      mgmt = new DatasourceManagement[T, F, I, N](modules, errors, runningS, sstEvalConfig)
    } yield (mgmt, runningS)
  }

  def withErrorReporting[F[_]: Monad: MonadError_[?[_], Exception], I: Order, G[_], Q, R](
      errors: Ref[F, IMap[I, Exception]],
      datasourceId: I,
      ds: Datasource[F, G, Q, R])
      : Datasource[F, G, Q, R] =
    ConditionReportingDatasource[Exception, F, G, Q, R](
      c => errors.update(_.alter(datasourceId, κ(Condition.optionIso.get(c)))), ds)

  ////

  private def lazyDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadError_[?[_], CreateError[Json]]: Timer](
      module: DatasourceModule,
      ref: DatasourceRef[Json])(
      implicit ec: ExecutionContext)
      : F[Disposable[F, DS[T, F]]] =
    module match {
      case DatasourceModule.Lightweight(lw) =>
        val mklw = MonadError_[F, CreateError[Json]] unattempt {
          handleLinkageError(ref.kind, lw.lightweightDatasource[F](ref.config))
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mklw)
          .map(_.map(_.left))

      case DatasourceModule.Heavyweight(hw) =>
        val mkhw = MonadError_[F, CreateError[Json]] unattempt {
          handleLinkageError(ref.kind, hw.heavyweightDatasource[T, F](ref.config))
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(ref.kind, mkhw)
          .map(_.map(_.right))
    }

  private def handleLinkageError[F[_], A](kind: DatasourceType, fa: => F[A])(implicit F: ApplicativeError[F, java.lang.Throwable])
      : F[A] =
    try {
      fa
    } catch {
      case _: java.lang.LinkageError => F.raiseError(IncompatibleDatasourceException(kind))
    }
}
