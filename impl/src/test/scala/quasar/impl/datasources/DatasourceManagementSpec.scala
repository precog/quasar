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
import quasar.{ConditionMatchers, Disposable, RenderTreeT}
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.common.data.Data
import quasar.common.resource._
import quasar.connector.{Datasource, HeavyweightDatasourceModule, LightweightDatasourceModule}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.DatasourceModule
import quasar.qscript.{MonadPlannerErr, PlannerError, QScriptEducated}

import java.lang.IllegalArgumentException
import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import argonaut.JsonScalaz._
import cats.{Applicative, ApplicativeError, MonadError}
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import eu.timepit.refined.auto._
import fs2.{Scheduler, Stream}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data.Fix
import scalaz.{\/, -\/, IMap, Show}
import scalaz.syntax.either._
import scalaz.syntax.show._
import scalaz.std.anyVal._
import scalaz.std.option._
import shims._

final class DatasourceManagementSpec extends quasar.Qspec with ConditionMatchers {
  import DatasourceManagementSpec._

  type Mgmt = DatasourceControl[IO, Stream[IO, ?], Int, Json] with DatasourceErrors[IO, Int]
  type Running = DatasourceManagement.Running[Int, Fix, IO]

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

  val LightT = DatasourceType("light", 1L)
  val HeavyT = DatasourceType("heavy", 2L)

  val lightMod = new LightweightDatasourceModule {
    val kind = LightT

    def lightweightDatasource[F[_]: ConcurrentEffect: MonadResourceErr: Timer](config: Json)
        : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, Stream[F, Data]]]] =
      mkDatasource[ResourcePath, F](kind).right.pure[F]
  }

  val heavyMod = new HeavyweightDatasourceModule {
    val kind = HeavyT

    def heavyweightDatasource[
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: MonadPlannerErr: Timer](
        config: Json)
        : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], T[QScriptEducated[T, ?]], Stream[F, Data]]]] =
      mkDatasource[T[QScriptEducated[T, ?]], F](kind).right.pure[F]
  }

  val modules: DatasourceManagement.Modules =
    IMap(
      lightMod.kind -> DatasourceModule.Lightweight(lightMod),
      heavyMod.kind -> DatasourceModule.Heavyweight(heavyMod))

  def withInitialMgmt[A](configured: IMap[Int, DatasourceRef[Json]])(f: (Mgmt, IO[Running]) => IO[A]): A =
    (for {
      s <- Scheduler.allocate[IO](1)
      t <- DatasourceManagement[Fix, IO, Int](modules, configured, global, s._1)
      (mgmt, run) = t
      a <- f(mgmt, run.get)
      _ <- s._2
    } yield a).unsafeRunSync()

  def withMgmt[A](f: (Mgmt, IO[Running]) => IO[A]): A =
    withInitialMgmt(IMap.empty)(f)

  "control" >> {
    "init datasource" >> {
      "creates a datasource successfully" >> withMgmt { (mgmt, running) =>
        for {
          r0 <- running
          c <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("foo"), Json.jNull))
          r1 <- running
        } yield {
          r0.size must_= 0
          c must beNormal
          r1.member(1) must beTrue
        }
      }

      "error when kind is unsupported" >> withMgmt { (mgmt, running) =>
        val unkT = DatasourceType("unknown", 1L)

        mgmt.initDatasource(1, DatasourceRef(unkT, DatasourceName("nope"), Json.jNull)) map { c =>
          c must beAbnormal(DatasourceUnsupported(unkT, modules.keySet))
        }
      }

      "finalizes an existing datasource when replaced" >> withMgmt { (mgmt, running) =>
        val a = DatasourceName("a")

        for {
          c1 <- mgmt.initDatasource(1, DatasourceRef(LightT, a, Json.jNull))
          ds1 <- running.map(_.lookup(1))
          c2 <- mgmt.initDatasource(1, DatasourceRef(HeavyT, a, Json.jNull))
          ds2 <- running.map(_.lookup(1))
        } yield {
          c1 must beNormal
          c2 must beNormal
          Applicative[Option].map2(ds1, ds2)(_ eq _) must beSome(false)
        }
      }

      "clears errors from an existing datasource when replaced" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.initDatasource(2, DatasourceRef(LightT, DatasourceName("a"), Json.jNull))
          _  <- mgmt.prefixedChildPaths(2, ResourcePath.root()).attempt
          e1 <- mgmt.datasourceError(2)
          c2 <- mgmt.initDatasource(2, DatasourceRef(HeavyT, DatasourceName("a"), Json.jNull))
          r  <- running.map(_.lookup(2))
          e2 <- mgmt.datasourceError(2)
        } yield {
          c1 must beNormal
          e1 must beSome(beAnInstanceOf[IllegalArgumentException])
          c2 must beNormal
          r must beSome
          e2 must beNone
        }
      }
    }

    "shutdown" >> {
      "finalizes the datasource and clears errors" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.initDatasource(3, DatasourceRef(LightT, DatasourceName("f"), Json.jNull))
          _  <- mgmt.prefixedChildPaths(3, ResourcePath.root()).attempt
          e1 <- mgmt.datasourceError(3)
          _ <- mgmt.shutdownDatasource(3)
          e2 <- mgmt.datasourceError(3)
          ds2 <- running.map(_.lookup(3))
        } yield {
          e1 must beSome
          e2 must beNone
          ds2 must beNone
        }
      }

      "no-op for a datasource that doesn't exist" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.initDatasource(4, DatasourceRef(LightT, DatasourceName("a"), Json.jNull))
          r1 <- running
          _ <- mgmt.shutdownDatasource(-1)
          r2 <- running
        } yield {
          r1.member(4) must beTrue
          r2.member(4) must beTrue
        }
      }
    }

    "path is resource" >> {
      "pass through to underlying datasource" >> withMgmt { (mgmt, _) =>
        for {
          c <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("b"), Json.jNull))
          b <- mgmt.pathIsResource(1, ResourcePath.root())
        } yield {
          c must beNormal
          b must be_\/-(false)
        }
      }

      "datasource not found when no datasource having id" >> withMgmt { (mgmt, _) =>
        mgmt.pathIsResource(-1, ResourcePath.root()).map(_ must beLike {
          case -\/(DatasourceNotFound(-1)) => ok
        })
      }
    }

    "prefixed child paths" >> {
      "delegate to found datasource" >> withMgmt { (mgmt, _) =>
        for {
          c <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("b"), Json.jNull))
          b <- mgmt.prefixedChildPaths(1, ResourcePath.root() / ResourceName("data"))
        } yield {
          c must beNormal
          b must be_\/-
        }
      }

      "path not found when underlying returns none" >> withMgmt { (mgmt, _) =>
        val dne = ResourcePath.root() / ResourceName("dne")

        for {
          c <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("b"), Json.jNull))
          b <- mgmt.prefixedChildPaths(1, dne)
        } yield {
          c must beNormal
          b must beLike {
            case -\/(PathNotFound(p)) => p must_= dne
          }
        }
      }


      "datasource not found when no datasource having id" >> withMgmt { (mgmt, _) =>
        mgmt.prefixedChildPaths(-1, ResourcePath.root() / ResourceName("data")).map(_ must beLike {
          case -\/(DatasourceNotFound(-1)) => ok
        })
      }
    }
  }

  "errors" >> {
    "datasource error returns latest error" >> withMgmt { (mgmt, running) =>
      val foo = ResourcePath.root() / ResourceName("error") / ResourceName("foo")
      val bar = ResourcePath.root() / ResourceName("error") / ResourceName("bar")

      for {
        ca <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("c"), Json.jNull))
        _  <- mgmt.prefixedChildPaths(1, foo).attempt
        e1 <- mgmt.datasourceError(1)
        _  <- mgmt.prefixedChildPaths(1, bar).attempt
        e2 <- mgmt.datasourceError(1)
      } yield {
        ca must beNormal
        e1.exists(_.getMessage.contains("foo")) must beTrue
        e2.exists(_.getMessage.contains("bar")) must beTrue
      }
    }

    "lookup returns none once a previously erroring datasource succeeds" >> withMgmt { (mgmt, running) =>
      for {
        ca <- mgmt.initDatasource(1, DatasourceRef(LightT, DatasourceName("d"), Json.jNull))
        _  <- mgmt.prefixedChildPaths(1, ResourcePath.root() / ResourceName("error") / ResourceName("quux")).attempt
        e1 <- mgmt.datasourceError(1)
        _  <- mgmt.prefixedChildPaths(1, ResourcePath.root() / ResourceName("data")).attempt
        e2 <- mgmt.datasourceError(1)
      } yield {
        ca must beNormal
        e1.exists(_.getMessage.contains("quux")) must beTrue
        e2 must beNone
      }
    }

    val initCfg: IMap[Int, DatasourceRef[Json]] =
      IMap(
        1 -> DatasourceRef(LightT, DatasourceName("initlw"), Json.jNull),
        -1 -> DatasourceRef(DatasourceType("nope", 1L), DatasourceName("nope"), Json.jNull))

    "initial configured datasources report errors" >> withInitialMgmt(initCfg) { (mgmt, running) =>
      for {
        _  <- mgmt.prefixedChildPaths(1, ResourcePath.root() / ResourceName("error") / ResourceName("foo")).attempt
        e1 <- mgmt.datasourceError(1)
      } yield e1.exists(_.getMessage.contains("foo")) must beTrue
    }

    "initial configured datasources report config errors" >> withInitialMgmt(initCfg) { (mgmt, running) =>
      for {
        _  <- mgmt.pathIsResource(-1, ResourcePath.root() / ResourceName("foo")).attempt
        e1 <- mgmt.datasourceError(-1)
      } yield e1.exists(_.getMessage.contains("Unsupported")) must beTrue
    }
  }
}

object DatasourceManagementSpec {
  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[Int, Json]].shows(ce))

  def mkDatasource[Q, F[_]: MonadError[?[_], Throwable]](kind0: DatasourceType)
      : Disposable[F, Datasource[F, Stream[F, ?], Q, Stream[F, Data]]] =
    new Datasource[F, Stream[F, ?], Q, Stream[F, Data]] {
      def kind = kind0

      def evaluate(query: Q): F[Stream[F, Data]] =
        Stream.empty.covaryAll[F, Data].pure[F]

      def pathIsResource(path: ResourcePath): F[Boolean] =
        false.pure[F]

      def prefixedChildPaths(path: ResourcePath): F[Option[Stream[F, (ResourceName, ResourcePathType)]]] =
        path.uncons match {
          case None =>
            ApplicativeError[F, Throwable]
              .raiseError(new IllegalArgumentException(s"InvalidPath: /"))

          case Some((ResourceName("error"), _)) =>
            ApplicativeError[F, Throwable]
              .raiseError(new IllegalArgumentException(s"InvalidPath: ${path.shows}"))

          case Some((ResourceName("dne"), _)) =>
            none[Stream[F, (ResourceName, ResourcePathType)]].pure[F]

          case _ =>
            some(Stream.empty.covaryAll[F, (ResourceName, ResourcePathType)]).pure[F]
        }
    }.pure[Disposable[F, ?]]
}
