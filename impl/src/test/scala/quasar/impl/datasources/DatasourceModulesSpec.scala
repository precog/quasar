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

import quasar.{EffectfulQSpec, RateLimiter, RateLimiting, RenderTreeT, ScalarStages}
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.impl.{DatasourceModule, EmptyDatasource, QuasarDatasource}
import quasar.connector._
import quasar.connector.datasource._
import quasar.contrib.scalaz._
import quasar.qscript.{MonadPlannerErr, PlannerError, InterpretedRead, QScriptEducated}

import fs2.Stream

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.{jArray, jEmptyObject, jString, jNumber}

import cats.{Monad, Show}
import cats.effect.{ContextShift, ConcurrentEffect, IO, Resource, Sync, Timer}
import cats.instances.int._
import cats.kernel.Hash
import cats.kernel.instances.uuid._
import cats.syntax.applicative._
import cats.syntax.applicativeError._

import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data.Fix

import scalaz.{ISet, NonEmptyList}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import shims.{showToCats, showToScalaz}

object DatasourceModulesSpec extends EffectfulQSpec[IO] {
  implicit val tmr = IO.timer(global)

  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[Int, Json]].show(ce))

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

  type R[F[_], A] = Either[InitializationError[Json], Datasource[Resource[F, ?], Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]

  def mkDatasource[F[_]: Monad, Q](kind: DatasourceType)
      : Datasource[Resource[F, ?], Stream[F, ?], Q, QueryResult[F], ResourcePathType.Physical] = {
    EmptyDatasource[Resource[F, ?], Stream[F, ?], Q, QueryResult[F], ResourcePathType.Physical](
      kind,
      QueryResult.typed(
        DataFormat.ldjson,
        ResultData.empty,
        ScalarStages.Id))
  }

  def lightMod(k: DatasourceType, err: Option[InitializationError[Json]] = None, minV: Option[Long] = None)
      : DatasourceModule =
    DatasourceModule.Lightweight(new LightweightDatasourceModule {
      val kind = k

      override def minVersion = minV getOrElse k.version

      def sanitizeConfig(config: Json): Json = jString("sanitized")

      def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json)
          : F[Either[ConfigurationError[Json], Json]] = {
        val back: Either[ConfigurationError[Json], Json] = Right(jArray(List(jNumber(from), jNumber(to), jString("migrated"))))
        back.pure[F]
      }

      def reconfigure(original: Json, patch: Json)
          : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
        Right((Reconfiguration.Reset, jArray(List(original, patch))))

      def lightweightDatasource[
          F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer,
          A: Hash](
          config: Json,
          rateLimiting: RateLimiting[F, A],
          byteStore: ByteStore[F],
          auth: GetAuth[F])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, InterpretedRead[ResourcePath]]] = {

        Resource.pure(err match {
          case None => Right(mkDatasource(k))
          case Some(e) => Left(e)
        })
      }
    })

  def heavyMod(k: DatasourceType, err: Option[InitializationError[Json]] = None, minV: Option[Long] = None)
      : DatasourceModule =
    DatasourceModule.Heavyweight(new HeavyweightDatasourceModule {
      val kind = k

      override def minVersion = minV getOrElse k.version

      def sanitizeConfig(config: Json): Json = jString("sanitized")

      def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json)
          : F[Either[ConfigurationError[Json], Json]] = {
        val back: Either[ConfigurationError[Json], Json] = Right(jArray(List(jNumber(from), jNumber(to), jString("migrated"))))
        back.pure[F]
      }

      def reconfigure(original: Json, patch: Json)
          : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
        Right((Reconfiguration.Reset, jArray(List(original, patch))))

      def heavyweightDatasource[
          T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
          F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: Timer](
          config: Json,
          byteStore: ByteStore[F])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, T[QScriptEducated[T, ?]]]] = {

        Resource.pure(err match {
          case None => Right(mkDatasource(k))
          case Some(e) => Left(e)
        })
      }
    })

  def makeRateLimiter = RateLimiter[IO, UUID](IO.delay(UUID.randomUUID())).use(IO(_))

  "supported types" >> {
    "empty" >>* {
      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(), rl, ByteStores.void[IO, Int], x => IO(None) )
        tys <- modules.supportedTypes
      } yield {
        tys === ISet.empty
      }
    }
    "non-empty" >>* {
      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(
          lightMod(DatasourceType("a", 1)),
          lightMod(DatasourceType("b", 2)),
          heavyMod(DatasourceType("c", 3))),
          rl,
          ByteStores.void[IO, Int],
          x => IO(None))
        tys <- modules.supportedTypes
      } yield {
        tys === ISet.fromList(List(DatasourceType("a", 1), DatasourceType("b", 2), DatasourceType("c", 3)))
      }
    }
  }

  "sanitizing refs" >>* {
    val aType = DatasourceType("a", 1)
    val bType = DatasourceType("b", 2)
    val cType = DatasourceType("c", 3)
    val bType3 = DatasourceType("b", 3)

    val aRef = DatasourceRef(aType, DatasourceName("a-name"), jString("a-config"))
    val bRef = DatasourceRef(bType, DatasourceName("b-name"), jString("b-config"))
    val cRef = DatasourceRef(cType, DatasourceName("c-name"), jString("c-config"))
    val bRef3 = DatasourceRef(bType3, DatasourceName("b-name-3"), jString("b-config"))

    val aExpected = aRef.copy(config = jString("sanitized"))
    val bExpected = bRef.copy(config = jString("sanitized"))
    val cExpected = cRef.copy(config = jEmptyObject)
    val bExpected3 = bRef3.copy(config = jEmptyObject)

    for {
      rl <- makeRateLimiter
      modules = DatasourceModules[Fix, IO, Int, UUID](List(lightMod(aType), heavyMod(bType)), rl, ByteStores.void[IO, Int], x => IO(None))
      aRes <- modules.sanitizeRef(aRef)
      bRes <- modules.sanitizeRef(bRef)
      cRes <- modules.sanitizeRef(cRef)
      bRes3 <- modules.sanitizeRef(bRef3)
    } yield {
      aRes === aExpected
      bRes === bExpected
      cRes === cExpected
      bRes3 === bExpected3
    }
  }

  "reconfigure refs" >>* {
    val aType = DatasourceType("a", 1)
    val aType2 = DatasourceType("a", 2)
    val bType = DatasourceType("b", 1)
    val bType2 = DatasourceType("b", 2)
    val cType = DatasourceType("c", 3)

    val aRef = DatasourceRef(aType, DatasourceName("a-name"), jString("a-config"))
    val aRef2 = DatasourceRef(aType2, DatasourceName("a-name"), jString("a-config"))
    val bRef = DatasourceRef(bType, DatasourceName("b-name"), jString("b-config"))
    val bRef2 = DatasourceRef(bType2, DatasourceName("b-name"), jString("b-config"))
    val cRef = DatasourceRef(cType, DatasourceName("c-name"), jString("c-config"))

    val aPatch = jString("a-patch")
    val bPatch = jString("b-patch")
    val cPatch = jString("c-patch")

    val aExpected = aRef.copy(config = jArray(List(jString("a-config"), jString("a-patch"))))
    val bExpected = bRef2.copy(config = jArray(List(jString("b-config"), jString("b-patch"))))
    val migratedExpected = bRef2.copy(config = jArray(List(jArray(List(jNumber(1), jNumber(2), jString("migrated"))), jString("b-patch"))))
    val cExpected = cRef.copy(config = jEmptyObject)

    for {
      rl <- makeRateLimiter
      modules = DatasourceModules[Fix, IO, Int, UUID](
        List(lightMod(aType), heavyMod(bType2, None, Some(1))),
        rl,
        ByteStores.void[IO, Int],
        x => IO(None))
      aRes <- modules.reconfigureRef(aRef, aPatch).value
      aRes2 <- modules.reconfigureRef(aRef2, aPatch).value
      bRes <- modules.reconfigureRef(bRef, bPatch).value
      bRes2 <- modules.reconfigureRef(bRef2, bPatch).value
      cRes <- modules.reconfigureRef(cRef, cPatch).value
    } yield {
      aRes must beRight((Reconfiguration.Reset, aExpected))
      bRes must beRight((Reconfiguration.Reset, migratedExpected))
      bRes2 must beRight((Reconfiguration.Reset, bExpected))
      aRes2 must beLike {
        case Left(DatasourceError.DatasourceUnsupported(kind, _)) =>
          kind mustEqual aRef2.kind
      }
      cRes must beLike {
        case Left(DatasourceError.DatasourceUnsupported(kind, _)) =>
          kind mustEqual cRef.kind
      }
    }
  }

  "create" >> {
    val lightType = DatasourceType("light", 1)
    val heavyType = DatasourceType("heavy", 2)
    val incompatType = DatasourceType("incompat", 3)
    val tooNewType = DatasourceType("light", 2)
    val tooOldType = DatasourceType("light", 0)
    val migrationType = DatasourceType("heavy", 1)

    val lightRef = DatasourceRef(lightType, DatasourceName("light-name"), jString("light-config"))
    val heavyRef = DatasourceRef(heavyType, DatasourceName("heavy-name"), jString("heavy-config"))
    val incompatRef = DatasourceRef(incompatType, DatasourceName("incompat-name"), jString("incompat-config"))
    val tooNew = DatasourceRef(tooNewType, DatasourceName("too-new"), jString("too-new"))
    val tooOld = DatasourceRef(tooOldType, DatasourceName("too-old"), jString("too-old"))
    val migrationRef = DatasourceRef(migrationType, DatasourceName("migration-name"), jString("migration-config"))


    "works with provided modules" >>* {
      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(lightMod(lightType), heavyMod(heavyType)), rl, ByteStores.void[IO, Int], x => IO(None))
        (lightRes, finalizer1) <- modules.create(0, lightRef).value.allocated
        (heavyRes, finalizer2) <- modules.create(1, heavyRef).value.allocated
        _ <- finalizer1
        _ <- finalizer2
      } yield {
        lightRes must beLike { case Right(QuasarDatasource.Lightweight(lw)) => lw.kind === lightType }
        heavyRes must beLike { case Right(QuasarDatasource.Heavyweight(hw)) => hw.kind === heavyType }
      }
    }

    "works with modules supported by version range" >>* {
      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(heavyMod(heavyType, None, Some(1))), rl, ByteStores.void[IO, Int], x => IO(None))
        (res, fin) <- modules.create(0, migrationRef).value.allocated
        _ <- fin
      } yield {
        res must beLike { case Right(QuasarDatasource.Heavyweight(hw)) => hw.kind === heavyType }
      }
    }

    "errors with incompatible refs" >>* {
      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(lightMod(lightType), heavyMod(heavyType)), rl, ByteStores.void[IO, Int], x => IO(None))
        (res, fin0) <- modules.create(0, incompatRef).value.allocated
        (tooRes, fin1) <- modules.create(1, tooNew).value.allocated
        (oldRes, fin2) <- modules.create(2, tooOld).value.allocated
        _ <- fin2
        _ <- fin1
        _ <- fin0
      } yield {
        res must beLeft(DatasourceUnsupported(incompatType, ISet.singleton(lightType).insert(heavyType)))
        tooRes must beLeft(DatasourceUnsupported(tooNewType, ISet.singleton(lightType).insert(heavyType)))
        oldRes must beLeft(DatasourceUnsupported(tooOldType, ISet.singleton(lightType).insert(heavyType)))
      }
    }
    "errors with initialization error" >>* {
      val malformed =
        DatasourceType("malformed", 1)
      val malformedRef =
        DatasourceRef(malformed, DatasourceName("doesn't matter"), jString("malformed-config"))
      val invalid =
        DatasourceType("invalid", 1)
      val invalidRef =
        DatasourceRef(invalid, DatasourceName("doesn't matter"), jString("invalid-config"))
      val connFailed =
        DatasourceType("conn-failed", 1)
      val connFailedRef =
        DatasourceRef(connFailed, DatasourceName("doesn't matter"), jString("conn-failed-config"))
      val accessDenied =
        DatasourceType("access-denied", 1)
      val accessDeniedRef =
        DatasourceRef(accessDenied, DatasourceName("doesn't matter"), jString("access-denied-config"))

      for {
        rl <- makeRateLimiter
        modules = DatasourceModules[Fix, IO, Int, UUID](List(
          lightMod(malformed, Some(MalformedConfiguration(malformed, jString("a"), "malformed configuration"))),
          heavyMod(invalid, Some(InvalidConfiguration(invalid, jString("b"), NonEmptyList("invalid configuration")))),
          lightMod(connFailed, Some(ConnectionFailed(connFailed, jString("c"), new Exception("conn failed")))),
          heavyMod(accessDenied, Some(AccessDenied(accessDenied, jString("d"), "access denied")))),
          rl,
          ByteStores.void[IO, Int],
          x => IO(None)
        )

        (malformedDs, finM) <- modules.create(0, malformedRef).value.allocated
        (invalidDs, finI) <- modules.create(1, invalidRef).value.allocated
        (connFailedDs, finC) <- modules.create(2, connFailedRef).value.allocated
        (accessDeniedDs, finA) <- modules.create(3, accessDeniedRef).value.allocated
        _ <- finM
        _ <- finI
        _ <- finC
        _ <- finA
      } yield {
        malformedDs must beLeft(MalformedConfiguration(malformed, jString("a"), "malformed configuration"))
        invalidDs must beLeft(InvalidConfiguration(invalid, jString("b"), NonEmptyList("invalid configuration")))
        accessDeniedDs must beLeft(AccessDenied(accessDenied, jString("d"), "access denied"))
        connFailedDs must beLike {
          case Left(ConnectionFailed(kind, config, cause)) =>
            kind === connFailed
            config === jString("c")
            cause.getMessage === "conn failed"
        }
      }
    }
  }
}
