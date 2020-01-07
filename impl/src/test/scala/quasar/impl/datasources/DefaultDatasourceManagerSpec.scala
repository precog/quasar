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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.{ConditionMatchers, RateLimiter, RenderTreeT, ScalarStages}
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.connector._
import quasar.connector.DataFormat
import quasar.contrib.fs2.stream._
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.implicits._
import quasar.impl.DatasourceModule
import quasar.impl.datasource.EmptyDatasource
import quasar.qscript.{MonadPlannerErr, PlannerError, InterpretedRead, QScriptEducated}

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.{jEmptyObject, jString}

import cats.Applicative
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.applicative._
import cats.syntax.applicativeError._

import eu.timepit.refined.auto._

import fs2.Stream

import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data.Fix

import scalaz.{IMap, Show}
import scalaz.syntax.traverse._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._

import shims.{eqToScalaz => _, orderToScalaz => _, _}

object DefaultDatasourceManagerSpec extends quasar.Qspec with ConditionMatchers {

  type Mgr = DatasourceManager[Int, Json, Fix, IO, Stream[IO, ?], QueryResult[IO], ResourcePathType.Physical]
  type Disposes = Ref[IO, List[DatasourceType]]

  type R[F[_], A] = Either[InitializationError[Json], Datasource[F, Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]

  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[Int, Json]].shows(ce))

  implicit val ioPlannerErrorME: MonadError_[IO, PlannerError] =
    new MonadError_[IO, PlannerError] {
      def raiseError[A](e: PlannerError): IO[A] =
        IO.raiseError(new PlannerErrorException(e))

      def handleError[A](fa: IO[A])(f: PlannerError => IO[A]): IO[A] =
        fa.recoverWith {
          case PlannerErrorException(pe) => f(pe)
        }
    }

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))

      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)

  def mkDatasource[F[_]: Applicative, Q, P <: ResourcePathType](kind: DatasourceType)
      : Datasource[F, Stream[F, ?], Q, QueryResult[F], P] =
    EmptyDatasource[F, Stream[F, ?], Q, QueryResult[F], P](
      kind,
      QueryResult.typed(
        DataFormat.ldjson,
        Stream.empty,
        ScalarStages.Id))

  val LightT = DatasourceType("light", 1L)
  val HeavyT = DatasourceType("heavy", 2L)

  def lightMod(disposes: Disposes) = new LightweightDatasourceModule {
    val kind = LightT

    def sanitizeConfig(config: Json): Json = jString("sanitized")

    def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
        config: Json,
        rateLimiter: RateLimiter[F])(
        implicit ec: ExecutionContext)
        : Resource[F, R[F, InterpretedRead[ResourcePath]]] =
      Resource((
        Right(mkDatasource[F, InterpretedRead[ResourcePath], ResourcePathType.Physical](kind)) : R[F, InterpretedRead[ResourcePath]],
        ConcurrentEffect[F].liftIO(disposes.update(kind :: _))).pure[F])
  }

  def heavyMod(disposes: Disposes) = new HeavyweightDatasourceModule {
    val kind = HeavyT

    def sanitizeConfig(config: Json): Json = jString("sanitized")

    def heavyweightDatasource[
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: Timer](
        config: Json)(
        implicit ec: ExecutionContext)
        : Resource[F, R[F, T[QScriptEducated[T, ?]]]] =
      Resource((
        Right(mkDatasource[F, T[QScriptEducated[T, ?]], ResourcePathType.Physical](kind)) : R[F, T[QScriptEducated[T, ?]]],
        ConcurrentEffect[F].liftIO(disposes.update(kind :: _))).pure[F])
  }

  def modules(disposes: Disposes): DefaultDatasourceManager.Modules = {
    val lm = lightMod(disposes)
    val hm = heavyMod(disposes)

    IMap(
      lm.kind -> DatasourceModule.Lightweight(lm),
      hm.kind -> DatasourceModule.Heavyweight(hm))
  }

  def withInitialMgr[A](configured: IMap[Int, DatasourceRef[Json]])(f: (Mgr, Disposes) => IO[A]): A =
    RateLimiter[IO](1.0).flatMap(limiter =>
      Ref[IO].of(List.empty[DatasourceType])
        .flatMap(disps =>
          DefaultDatasourceManager.Builder[Int, Fix, IO]
            .build(modules(disps), configured, limiter)
            .use(f(_, disps))))
      .unsafeRunSync()

  def withMgr[A](f: (Mgr, Disposes) => IO[A]): A =
    withInitialMgr(IMap.empty)(f)

  "configured datasources" >> {
    "implicitly available" >> withInitialMgr(IMap(7 -> DatasourceRef(HeavyT, DatasourceName("bar"), Json.jNull))) { (mgr, _) =>
      for {
        ds <- mgr.managedDatasource(7)
        isR <- ds.traverse(_.pathIsResource(ResourcePath.root()))
      } yield {
        ds.map(_.kind) must_= Some(HeavyT)
        isR must beSome(false)
      }
    }

    "dispose properly" >> withInitialMgr(IMap(8 -> DatasourceRef(LightT, DatasourceName("foo"), Json.jNull))) { (mgr, disps) =>
      for {
        ds <- mgr.managedDatasource(8)
        // Have to access it as initialization is lazy
        _ <- ds.traverse(_.pathIsResource(ResourcePath.root()))
        _ <- mgr.shutdownDatasource(8)
        disposed <- disps.get
      } yield {
        ds.map(_.kind) must_= Some(LightT)
        disposed must_= List(LightT)
      }
    }
  }

  "init datasource" >> {
    "creates a datasource successfully" >> withMgr { (mgr, _) =>
      for {
        r0 <- mgr.managedDatasource(1)
        c <- mgr.initDatasource(1, DatasourceRef(LightT, DatasourceName("foo"), Json.jNull))
        r1 <- mgr.managedDatasource(1)
      } yield {
        r0 must beNone
        c must beNormal
        r1 must beSome
      }
    }

    "error when kind is unsupported" >> withMgr { (mgr, disps) =>
      val unkT = DatasourceType("unknown", 1L)

      mgr.initDatasource(1, DatasourceRef(unkT, DatasourceName("nope"), Json.jNull)) map { c =>
        c must beAbnormal(DatasourceUnsupported(unkT, modules(disps).keySet))
      }
    }

    "finalizes an existing datasource when replaced" >> withMgr { (mgr, disps) =>
      val a = DatasourceName("a")

      for {
        c1 <- mgr.initDatasource(1, DatasourceRef(LightT, a, Json.jNull))
        ds1 <- mgr.managedDatasource(1)
        disposed0 <- disps.get
        c2 <- mgr.initDatasource(1, DatasourceRef(HeavyT, a, Json.jNull))
        disposed1 <- disps.get
        ds2 <- mgr.managedDatasource(1)
      } yield {
        c1 must beNormal
        c2 must beNormal
        Applicative[Option].map2(ds1, ds2)(_ eq _) must beSome(false)
        disposed0 must_= Nil
        disposed1 must_= List(LightT)
      }
    }
  }

  "shutdown" >> {
    "finalizes the datasource" >> withMgr { (mgr, disps) =>
      for {
        c1 <- mgr.initDatasource(3, DatasourceRef(LightT, DatasourceName("f"), Json.jNull))
        ds1 <- mgr.managedDatasource(3)
        disposed0 <- disps.get
        _ <- mgr.shutdownDatasource(3)
        disposed1 <- disps.get
        ds2 <- mgr.managedDatasource(3)
      } yield {
        ds1 must beSome
        ds2 must beNone
        disposed0 must_= Nil
        disposed1 must_= List(LightT)
      }
    }

    "no-op for a datasource that doesn't exist" >> withMgr { (mgr, disps) =>
      for {
        c1 <- mgr.initDatasource(4, DatasourceRef(LightT, DatasourceName("a"), Json.jNull))
        r1 <- mgr.managedDatasource(4)
        disposed0 <- disps.get
        _ <- mgr.shutdownDatasource(-1)
        disposed1 <- disps.get
        r2 <- mgr.managedDatasource(4)
      } yield {
        r1 must beSome
        r2 must beSome
        disposed0 must_= Nil
        disposed1 must_= Nil
      }
    }
  }

  "managed datasource" >> {
    "returns an existing datasource" >> withMgr { (mgr, _) =>
      for {
        c <- mgr.initDatasource(1, DatasourceRef(LightT, DatasourceName("b"), Json.jNull))
        d <- mgr.managedDatasource(1)
      } yield {
        c must beNormal
        d must beSome
      }
    }

    "returns none when no datasource having id" >> withMgr { (mgr, _) =>
      mgr.managedDatasource(42) map (_ must beNone)
    }
  }

  "config sanitization" >> {
    val cleansed = jString("sanitized")

    "module-specific sanitization when known" >> withMgr { (mgr, _) =>
      for {
        l <- mgr.sanitizedRef(DatasourceRef(LightT, DatasourceName("b"), jString("config"))).pure[IO]
        h <- mgr.sanitizedRef(DatasourceRef(HeavyT, DatasourceName("b"), jString("config"))).pure[IO]
      } yield {
        l.config must_= cleansed
        h.config must_= cleansed
      }
    }

    "empty object when unknown" >> withMgr { (mgr, _) =>
      val unkT = DatasourceType("unknown", 1L)
      for {
        u <- mgr.sanitizedRef(DatasourceRef(unkT, DatasourceName("b"), jString("config"))).pure[IO]
      } yield {
        u.config must_= jEmptyObject
      }
    }
  }
}
