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

import quasar.{RateLimiter, RateLimiting, ScalarStages, ConditionMatchers, Condition, NoopRateLimitUpdater}
import quasar.api.auth.ExternalCredentials
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.{concurrent => qc}
import quasar.connector._
import quasar.connector.datasource._
import quasar.contrib.scalaz._
import quasar.impl.{DatasourceModule, EmptyDatasource, QuasarDatasource, ResourceManager}
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}
import quasar.qscript.{PlannerError, InterpretedRead}

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.{jArray, jString}

import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Resource, Sync, Timer}
import cats.effect.concurrent.Ref
import cats.instances.string._
import cats.instances.option._
import cats.kernel.Hash
import cats.kernel.instances.uuid._
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._
import cats.syntax.traverse._

import fs2.Stream

import matryoshka.data.Fix

import scalaz.{IMap, NonEmptyList, \/-, -\/}

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import shims.{orderToScalaz, showToScalaz, applicativeToScalaz, showToCats, monadToScalaz}

object DefaultDatasourcesSpec extends DatasourcesSpec[IO, Stream[IO, ?], String, Json] with ConditionMatchers {

  sequential

  implicit val tmr = IO.timer(global)

  type PathType = ResourcePathType
  type Self = Datasources[IO, Stream[IO, ?], String, Json]
  type R[F[_], A] = Either[InitializationError[Json], Datasource[Resource[F, ?], Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]
  type QDS = QuasarDatasource[Fix, Resource[IO, ?], Stream[IO, ?], QueryResult[IO], PathType]

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[String, Json]].show(ce))

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))

      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  implicit val ioPlannerErrorME: MonadError_[IO, PlannerError] =
    new MonadError_[IO, PlannerError] {
      def raiseError[A](e: PlannerError): IO[A] =
        IO.raiseError(new PlannerErrorException(e))

      def handleError[A](fa: IO[A])(f: PlannerError => IO[A]): IO[A] =
        fa.recoverWith {
          case PlannerErrorException(pe) => f(pe)
        }
    }

  def lightMod(
      mp: Map[Json, InitializationError[Json]],
      sanitize: Option[Json => Json] = None,
      reconfig: Option[(Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)]] = None)
      : DatasourceModule = DatasourceModule.Lightweight {
    new LightweightDatasourceModule {
      val kind = supportedType

      def sanitizeConfig(config: Json): Json =
        sanitize match {
          case None => config
          case Some(f) => f(config)
        }

      def migrateConfig[F[_]: Sync](config: Json)
          : F[Either[ConfigurationError[Json], Json]] = {
        val back: Either[ConfigurationError[Json], Json] = Right(config)
        back.pure[F]
      }

      def reconfigure(orig: Json, patch: Json)
          : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
        reconfig match {
          case None => Right((Reconfiguration.Preserve, orig))
          case Some(f) => f(orig, patch)
        }

      def lightweightDatasource[
          F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer,
          A: Hash](
          config: Json,
          rateLimiting: RateLimiting[F, A],
          byteStore: ByteStore[F],
          auth: UUID => F[Option[ExternalCredentials[F]]])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, InterpretedRead[ResourcePath]]] = {

        lazy val ds: R[F, InterpretedRead[ResourcePath]] =
          Right {
            EmptyDatasource[Resource[F, ?], Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F], ResourcePathType.Physical](
              supportedType,
              QueryResult.typed(
                DataFormat.ldjson,
                Stream.empty,
                ScalarStages.Id))
          }

        Resource.pure[F, R[F, InterpretedRead[ResourcePath]]](
          mp.get(config) match {
            case None => ds
            case Some(e) => Left(e)
          })
      }
    }
  }

  val blocker: Blocker = qc.Blocker.cached("rdatasources-spec")

  def datasources: Resource[IO, Self] = prepare(Map()).map(_._1)

  def prepare(
      mp: Map[Json, InitializationError[Json]],
      errorMap: Option[Ref[IO, IMap[String, Exception]]] = None,
      sanitize: Option[Json => Json] = None,
      reconfigure: Option[(Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)]] = None) = {

    val freshId = IO(java.util.UUID.randomUUID.toString())

    val fRefs: IO[IndexedStore[IO, String, DatasourceRef[Json]]] =
      IO(new ConcurrentHashMap[String, DatasourceRef[Json]]()).map { (mp: ConcurrentHashMap[String, DatasourceRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, DatasourceRef[Json]](mp, blocker)
      }

    val rCache = ResourceManager[IO, String, QDS]

    val errors = DatasourceErrors fromMap {
      errorMap match {
        case None => IMap.empty[String, Exception].pure[IO]
        case Some(e) => e.get
      }
    }

    for {
      rateLimiting <- Resource.liftF(RateLimiter[IO, UUID](1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID]))
      starts <- Resource.liftF(Ref.of[IO, List[String]](List()))
      shuts <- Resource.liftF(Ref.of[IO, List[String]](List()))

      byteStores <- Resource.liftF(ByteStores.ephemeral[IO, String])

      modules =
        DatasourceModules[Fix, IO, String, UUID](List(lightMod(mp, sanitize, reconfigure)), rateLimiting, byteStores, x => IO(None))
          .widenPathType[PathType]
          .withMiddleware((i: String, mds: QDS) => starts.update(i :: _) as mds)
          .withFinalizer((i: String, mds: QDS) => shuts.update(i :: _))

      refs <- Resource.liftF(fRefs)
      cache <- rCache
      result <- Resource.liftF {
        DefaultDatasources[Fix, IO, Resource[IO, ?], Stream[IO, ?], String, Json, QueryResult[IO]](freshId, refs, modules, cache, errors, byteStores)
      }
    } yield (result, byteStores, refs, starts, shuts)
  }

  def supportedType = DatasourceType("test-type", 3)
  def validConfigs = (jString("one"), jString("two"))
  def gatherMultiple[A](as: Stream[IO, A]) = as.compile.toList

  "implementation specific" >> {
    "add datasource" >> {
      "initializes datasource" >>* {
        for {
          ((dses, _, _, starts, _), finalize) <- prepare(Map()).allocated
          b <- refB
          i0 <- dses.addDatasource(b)
          _ <- finalize
          started <- starts.get
          _ <- finalize
        } yield {
          i0 must beLike {
            case \/-(i) => List(i) === started
          }
        }
      }
      "returns created datasources" >>* {
        for {
          ((dses, _, _, starts, shuts), finalize) <- prepare(Map()).allocated
          md0 <- dses.allDatasourceMetadata.flatMap(_.compile.toList)
          b <- refB
          addStatus <- dses.addDatasource(b)
          md1 <- dses.allDatasourceMetadata.flatMap(_.compile.toList)
          d <- addStatus match {
            case -\/(_) => None.pure[IO]
            case \/-(s) => dses.quasarDatasourceOf(s)
          }
          ended0 <- shuts.get
          started <- starts.get
          _ <- finalize
          ended1 <- shuts.get
        } yield {
          md0 must_== List[(String, DatasourceMeta)]()
          addStatus must be_\/-
          md1.map(_._2) must_== List(
            DatasourceMeta(supportedType, b.name, Condition.Normal[Exception])
          )
          d must beSome
          started.size === 1
          ended0.size === 0
          ended1.size === 1
        }
      }
      "doesn't store config when initialization fails" >>* {
        val err3: InitializationError[Json] =
          MalformedConfiguration(supportedType, jString("three"), "3 isn't a config!")
        for {
          ((dses, _, refs, _, _), finalize) <- prepare(Map(jString("three") -> err3)).allocated
          a <- refA
          _ <- dses.addDatasource(DatasourceRef.config.set(jString("three"))(a)).attempt
          _ <- finalize
          entries <- refs.entries.compile.toList
        } yield {
          entries === List()
        }
      }
    }
    "remove datasource" >> {
      "shutdown existing" >>* {
        for {
          ((dses, _, _, starts, shuts), finalize) <- prepare(Map()).allocated
          a <- refA
          added <- dses.addDatasource(a)
          mbi = added.toOption
          cond <- mbi.traverse(dses.removeDatasource(_))
          started <- starts.get
          ended <- shuts.get
          _ <- finalize
        } yield {
          cond must beLike {
            case Some(c) =>
              c must beNormal
          }
          mbi must beLike {
            case Some(x) =>
              started === List(x)
              ended === List(x)
          }
        }
      }
    }
    "replace datasource" >> {
      "updates manager" >>* {
        for {
          a <- refA
          b <- refB
          ((dses, _, _, starts, shuts), finalize) <- prepare(Map()).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          c <- dses.replaceDatasource(i, b)
          started <- starts.get
          ended <- shuts.get
          _ <- finalize
        } yield {
          started === List(i, i)
          ended === List(i)
          c must beNormal
        }
      }
      "doesn't update manager when only name changed" >>* {
        for {
          a <- refA
          n <- randomName
          b = DatasourceRef.name.set(n)(a)
          ((dses, _, refs, starts, shuts), finalize) <- prepare(Map()).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          c <- dses.replaceDatasource(i, b)
          started <- starts.get
          ended <- shuts.get
          mbB <- refs.lookup(i)
          _ <- finalize
        } yield {
          started === List(i)
          ended === List()
          c must beNormal
          mbB must beSome(b)
        }
      }

      "doesn't replace when config invalid" >>* {
        val err3: InitializationError[Json] =
          MalformedConfiguration(supportedType, jString("three"), "3 isn't a config!")

        for {
          ((dses, _, refs, starts, shuts), finalize) <- prepare(Map(jString("invalid") -> err3)).allocated
          a <- refA
          a2 = DatasourceRef.config.set(jString("invalid"))(a)
          r <- dses.addDatasource(a)
          i = r.toOption.get
          c <- dses.replaceDatasource(i, a2)
          r2 <- refs.lookup(i)
          d <- dses.quasarDatasourceOf(i)
          started <- starts.get
          ended <- shuts.get
          _ <- finalize
        } yield {
          // once for initial and another for restart after failed replace
          started === List(i, i)
          ended === List(i)
          c must beAbnormal(err3)
          r2 must beSome(a)
          d must beSome
        }
      }
    }
    "sanitize config" >> {
      "ref is sanitized" >>* {
        val sanitize: Json => Json = x => jString("sanitized")
        for {
          a <- refA
          ((dses, _, _, _, _), finalize) <- prepare(Map(), None, Some(sanitize), None).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          l <- dses.datasourceRef(i)
          _ <- finalize
        } yield {
          l must be_\/-(a.copy(config = sanitize(a.config)))
        }
      }
    }
    "reconfigure config" >> {
      "ref is reconfigured resetting state" >>* {
        val reconfigure: (Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)] = {
          case (j1, j2) => Right((Reconfiguration.Reset, jArray(List(j1, j2))))
        }
        val patchConfig = jString("patchconfig")
        for {
          a <- refA
          ((dses, bs, _, _, _), finalize) <- prepare(Map(), None, None, Some(reconfigure)).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          s <- bs.get(i)
          _ <- s.insert("this-is-a-string", Array(24))
          _ <- dses.reconfigureDatasource(i, patchConfig)
          bsLookup <- bs.get(i).flatMap(_.lookup("this-is-a-string"))
          l <- dses.datasourceRef(i)
          _ <- finalize
        } yield {
          bsLookup must beNone
          l must be_\/-(a.copy(config = jArray(List(a.config, patchConfig))))
        }
      }
      "ref is reconfigured preserving state" >>* {
        val reconfigure: (Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)] = {
          case (j1, j2) => Right((Reconfiguration.Preserve, jArray(List(j1, j2))))
        }
        val patchConfig = jString("patchconfig")
        for {
          a <- refA
          ((dses, bs, _, _, _), finalize) <- prepare(Map(), None, None, Some(reconfigure)).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          s <- bs.get(i)
          _ <- s.insert("this-is-a-string", Array(24))
          _ <- dses.reconfigureDatasource(i, patchConfig)
          bsLookup <- bs.get(i).flatMap(_.lookup("this-is-a-string"))
          l <- dses.datasourceRef(i)
          _ <- finalize
        } yield {
          bsLookup must beSome(Array(24))
          l must be_\/-(a.copy(config = jArray(List(a.config, patchConfig))))
        }
      }
      "reconfiguration errors given a nonexisting datasource id" >>* {
        val reconfigure: (Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)] = {
          case (j1, j2) => Right((Reconfiguration.Reset, jArray(List(j1, j2))))
        }
        val patchConfig = jString("patchconfig")
        for {
          ((dses, _, _, _, _), finalize) <- prepare(Map(), None, None, Some(reconfigure)).allocated
          reconf <- dses.reconfigureDatasource("not a datasource", patchConfig)
          _ <- finalize
        } yield {
          reconf must beLike {
            case Condition.Abnormal(ex) => ex must_=== DatasourceNotFound("not a datasource")
          }
        }
      }
      "ref is not reconfigured when reconfigure function errors" >>* {
        val patchConfig = jString("sensitive")
        val error = DatasourceError.InvalidConfiguration(supportedType, patchConfig, NonEmptyList("oops"))
        val reconfigure: (Json, Json) => Either[ConfigurationError[Json], (Reconfiguration, Json)] = {
          case (j1, j2) => Left(error)
        }
        for {
          a <- refA
          ((dses, _, _, _, _), finalize) <- prepare(Map(), None, None, Some(reconfigure)).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          p <- dses.reconfigureDatasource(i, patchConfig)
          l <- dses.datasourceRef(i)
          _ <- finalize
        } yield {
          p must beAbnormal(error)
          l must be_\/-(a)
        }
      }
    }
    "lookup status" >> {
      "include errors" >>* {
        for {
          ref <- Ref.of[IO, IMap[String, Exception]](IMap.empty)
          ((dses, _, _, _, _), finalize) <- prepare(Map(), Some(ref), None, None).allocated
          a <- refA
          r <- dses.addDatasource(a)
          i = r.toOption.get
          _ <- ref.update(_.insert(i, new IOException()))
          r <- dses.datasourceStatus(i)
          _ <- finalize
        } yield {
          r must beLike {
            case \/-(Condition.Abnormal(ex)) => ex must beAnInstanceOf[IOException]
          }
        }

      }
    }
    "clustering" >> {
      "new ref appeared" >>* {
        for {
          ((dses, _, refs, starts, _), finalize) <- prepare(Map()).allocated
          a <- refA
          _ <- refs.insert("foo", a)
          res <- dses.quasarDatasourceOf("foo")
          started <- starts.get
          _ <- finalize
        } yield {
          res must beSome
          started === List("foo")
        }
      }
      "ref disappeared" >>* {
        for {
          ((dses, _, refs, starts, shuts), finalize) <- prepare(Map()).allocated
          a <- refA
          r <- dses.addDatasource(a)
          i = r.toOption.get
          res0 <- dses.quasarDatasourceOf(i)
          _ <- refs.delete(i)
          res1 <- dses.quasarDatasourceOf(i)
          ended <- shuts.get
          started <- starts.get
          _ <- finalize
        } yield {
          // Note, that we count resource init and finalize by datasource modules, I couldn't come up
          // with any way of unifying inner `F[_]` in lightweightdatasource :(
          res0 must beSome
          res1 must beNone
          started === List(i)
          ended === List(i)
        }
      }
      "ref updated" >>* {
        for {
          ((dses, _, refs, starts, shuts), finalize) <- prepare(Map()).allocated
          a <- refA
          _ <- refs.insert("foo", a)
          res0 <- dses.quasarDatasourceOf("foo")
          b <- refB
          _ <- refs.insert("foo", b)
          res1 <- dses.quasarDatasourceOf("foo")
          ended <- shuts.get
          started <- starts.get
          _ <- finalize
        } yield {
          res0 must beSome
          res1 must beSome
          started === List("foo", "foo")
          ended === List("foo")
        }
      }
    }
  }
}
