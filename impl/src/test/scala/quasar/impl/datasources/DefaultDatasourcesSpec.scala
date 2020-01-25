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

import quasar.{RateLimiter, RateLimiting, ScalarStages, ConditionMatchers, Condition, NoopRateLimitUpdater}
import quasar.api.MockSchemaConfig
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.{concurrent => qc}
import quasar.connector._
import quasar.connector.DataFormat
import quasar.contrib.scalaz._
import quasar.impl.{DatasourceModule, ResourceManager}
import quasar.impl.datasource.EmptyDatasource
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}
import quasar.qscript.{PlannerError, InterpretedRead}

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.jString

import cats.Show
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.instances.string._
import cats.instances.option._
import cats.kernel.Hash
import cats.kernel.instances.uuid._
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._
import cats.syntax.traverse._

import eu.timepit.refined.auto._

import fs2.Stream

import matryoshka.data.Fix

import scalaz.{IMap, \/-}

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import shims.{orderToScalaz, showToScalaz, applicativeToScalaz, monoidKToScalaz, showToCats, monadToScalaz}

object DefaultDatasourcesSpec extends DatasourcesSpec[IO, Stream[IO, ?], String, Json, MockSchemaConfig.type] with ConditionMatchers {

  sequential

  implicit val tmr = IO.timer(global)

  type PathType = ResourcePathType
  type Self = Datasources[IO, Stream[IO, ?], String, Json, MockSchemaConfig.type]
  type R[F[_], A] = Either[InitializationError[Json], Datasource[F, Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]
  type MDS = ManagedDatasource[Fix, IO, Stream[IO, ?], QueryResult[IO], PathType]

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
      sanitize: Option[Json => Json] = None)
      : DatasourceModule = DatasourceModule.Lightweight {
    new LightweightDatasourceModule {
      val kind = supportedType

      def sanitizeConfig(config: Json): Json = sanitize match {
        case None => config
        case Some(f) => f(config)
      }

      def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
          config: Json,
          rateLimiting: RateLimiting[F, A],
          byteStore: ByteStore[F])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, InterpretedRead[ResourcePath]]] = {
        lazy val ds: R[F, InterpretedRead[ResourcePath]] =
          Right {
            EmptyDatasource[F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F], ResourcePathType.Physical](
              supportedType,
              QueryResult.typed(
                DataFormat.ldjson,
                Stream.empty,
                ScalarStages.Id))
          }
        val result = mp.get(config) match {
          case None => ds
          case Some(e) => Left(e): R[F, InterpretedRead[ResourcePath]]
        }
        Resource.make(result.pure[F])(x => ().pure[F])
      }
    }
  }

  val blocker: Blocker = qc.Blocker.cached("rdatasources-spec")

  def datasources: Resource[IO, Self] = prepare(Map()).map(_._1)

  def prepare(
      mp: Map[Json, InitializationError[Json]],
      errorMap: Option[Ref[IO, IMap[String, Exception]]] = None,
      sanitize: Option[Json => Json] = None) = {

    val freshId = IO(java.util.UUID.randomUUID.toString())

    val fRefs: IO[IndexedStore[IO, String, DatasourceRef[Json]]] =
      IO(new ConcurrentHashMap[String, DatasourceRef[Json]]()).map { (mp: ConcurrentHashMap[String, DatasourceRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, DatasourceRef[Json]](mp, blocker)
      }

    val rCache = ResourceManager[IO, String, ManagedDatasource[Fix, IO, Stream[IO, ?], QueryResult[IO], PathType]]

    val schema =
      new ResourceSchema[IO, MockSchemaConfig.type, (ResourcePath, QueryResult[IO])] {
        def apply(c: MockSchemaConfig.type, r: (ResourcePath, QueryResult[IO])) =
          MockSchemaConfig.MockSchema.pure[IO]
      }

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

      byteStores = ByteStores.void[IO, String]

      modules = {
        DatasourceModules[Fix, IO, String, UUID](List(lightMod(mp, sanitize)), rateLimiting, byteStores)
          .widenPathType[PathType]
          .withMiddleware((i: String, mds: MDS) => starts.update(i :: _) as mds)
          .withFinalizer((i: String, mds: MDS) => shuts.update(i :: _))
      }
      refs <- Resource.liftF(fRefs)
      cache <- rCache
      result <- Resource.liftF {
        DefaultDatasources[Fix, IO, String, Json, MockSchemaConfig.type, QueryResult[IO]](freshId, refs, modules, cache, errors, schema, byteStores)
      }
    } yield (result, refs, starts, shuts)
  }

  def supportedType = DatasourceType("test-type", 3L)
  def validConfigs = (jString("one"), jString("two"))
  val schemaConfig = MockSchemaConfig
  def gatherMultiple[A](as: Stream[IO, A]) = as.compile.toList

  "implementation specific" >> {
    "add datasource" >> {
      "initializes datasource" >>* {
        for {
          ((dses, _, starts, _), finalize) <- prepare(Map()).allocated
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
      "doesn't store config when initialization fails" >>* {
        val err3: InitializationError[Json] =
          MalformedConfiguration(supportedType, jString("three"), "3 isn't a config!")
        for {
          ((dses, refs, _, _), finalize) <- prepare(Map(jString("three") -> err3)).allocated
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
          ((dses, _, starts, shuts), finalize) <- prepare(Map()).allocated
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
          ((dses, _, starts, shuts), finalize) <- prepare(Map()).allocated
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
          ((dses, refs, starts, shuts), finalize) <- prepare(Map()).allocated
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
    }
    "sanitize config" >> {
      "ref is sanitized" >>* {
        val sanitize: Json => Json = x => jString("sanitized")
        for {
          a <- refA
          ((dses, _, _, _), finalize) <- prepare(Map(), None, Some(sanitize)).allocated
          r <- dses.addDatasource(a)
          i = r.toOption.get
          l <- dses.datasourceRef(i)
          _ <- finalize
        } yield {
          l must be_\/-(a.copy(config = sanitize(a.config)))
        }
      }
    }
    "lookup status" >> {
      "include errors" >>* {
        for {
          ref <- Ref.of[IO, IMap[String, Exception]](IMap.empty)
          ((dses, _, _, _), finalize) <- prepare(Map(), Some(ref), None).allocated
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
          ((dses, refs, starts, _), finalize) <- prepare(Map()).allocated
          a <- refA
          _ <- refs.insert("foo", a)
          res <- dses.pathIsResource("foo", ResourcePath.root())
          started <- starts.get
          _ <- finalize
        } yield {
          res.toOption must beSome(false)
          started === List("foo")
        }
      }
      "ref disappered" >>* {
        for {
          ((dses, refs, starts, shuts), finalize) <- prepare(Map()).allocated
          a <- refA
          r <- dses.addDatasource(a)
          i = r.toOption.get
          res0 <- dses.pathIsResource(i, ResourcePath.root())
          _ <- refs.delete(i)
          res1 <- dses.pathIsResource(i, ResourcePath.root())
          ended <- shuts.get
          started <- starts.get
          _ <- finalize
        } yield {
          // Note, that we count resource init and finalize by datasource modules, I couldn't come up
          // with any way of unifying inner `F[_]` in lightweightdatasource :(
          res1 must be_-\/
          started === List(i, i)
          ended === List(i, i)
        }
      }
      "ref updated" >>* {
        for {
          ((dses, refs, starts, shuts), finalize) <- prepare(Map()).allocated
          a <- refA
          _ <- refs.insert("foo", a)
          res0 <- dses.pathIsResource("foo", ResourcePath.root())
          b <- refB
          _ <- refs.insert("foo", b)
          res1 <- dses.pathIsResource("foo", ResourcePath.root())
          ended <- shuts.get
          started <- starts.get
          _ <- finalize
        } yield {
          res0.toOption must beSome(false)
          res1.toOption must beSome(false)
          started === List("foo", "foo")
          ended === List("foo")
        }
      }
    }
  }
}
