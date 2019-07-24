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

package quasar.impl.datasources.middleware

import slamdata.Predef.{Boolean, List, None, Option, Unit}

import quasar.{Condition, ConditionMatchers, ScalarStages}
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.InterpretedRead

import java.lang.{Exception, IllegalArgumentException}

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO
import cats.effect.concurrent.Ref

import eu.timepit.refined.auto._

import shims._

object ConditionReportingMiddlewareSpec extends quasar.EffectfulQSpec[IO] with ConditionMatchers {

  type T[_[_]] = Unit

  val thatsRoot = new IllegalArgumentException("THAT'S ROOT!")

  object TestDs extends Datasource[IO, List, InterpretedRead[ResourcePath], Unit, ResourcePathType] {
    val kind: DatasourceType = DatasourceType("tester", 7L)

    def evaluate(query: InterpretedRead[ResourcePath]): IO[Unit] =
      if (ResourcePath.root.nonEmpty(query.path))
        IO.raiseError(thatsRoot)
      else
        IO.pure(())

    def pathIsResource(path: ResourcePath): IO[Boolean] =
      IO.pure(false)

    def prefixedChildPaths(path: ResourcePath)
        : IO[Option[List[(ResourceName, ResourcePathType)]]] =
      IO.pure(None)
  }

  val managedTester = ManagedDatasource.lightweight[T](TestDs)

  "initial condition is normal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Exception]]())
      ds <- ConditionReportingMiddleware[IO, Unit]((_, c) => r.update(c :: _))((), managedTester)
      cs <- r.get
    } yield {
      cs must_=== List(Condition.normal())
    }
  }

  "operations that succeed emit normal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Exception]]())
      ds <- ConditionReportingMiddleware[IO, Unit]((_, c) => r.update(c :: _))((), managedTester)
      _ <- ds.pathIsResource(ResourcePath.root())
      cs <- r.get
    } yield {
      cs must_=== List(Condition.normal(), Condition.normal())
    }
  }

  "operations that throw emit abnormal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Exception]]())
      ds <- ConditionReportingMiddleware[IO, Unit]((_, c) => r.update(c :: _))((), managedTester)
      res = ds match {
        case ManagedDatasource.ManagedLightweight(lw) => lw.evaluate(InterpretedRead(ResourcePath.root(), ScalarStages.Id))
        case _ => IO.pure(())
      }
      _ <- res.attempt
      cs <- r.get
    } yield {
      cs must_=== List(Condition.abnormal(thatsRoot), Condition.normal())
    }
  }
}
