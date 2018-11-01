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

package quasar.connector.datasource

import slamdata.Predef.{Left, List, String}

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{DatasourceSpec, ResourceError}
import quasar.contrib.scalaz.MonadError_

import cats.effect.IO
import eu.timepit.refined.auto._
import scalaz.std.list._
import shims._

object SingletonDatasourceSpec extends DatasourceSpec[IO, List] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val tpe = DatasourceType("singleton", 1L)
  val path = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

  val datasource = SingletonDatasource[IO, List, String](tpe, path, IO.pure("data"))
  val nonExistentPath = path / ResourceName("baz")
  def gatherMultiple[A](xs: List[A]) = IO.pure(xs)


  "evaluating resource path returns resource" >>* {
    datasource.evaluate(path).map(_ must_=== "data")
  }

  "evaluating prefix of resource path returns not a resource" >>* {
    val p = ResourcePath.root() / ResourceName("foo")

    datasource.evaluate(p).attempt.map(_ must beLike {
      case Left(ResourceError.throwableP(err)) =>
        err must_= ResourceError.notAResource(p)
    })
  }

  "evaluating unrelated path results in resource not found" >>* {
    datasource.evaluate(nonExistentPath).attempt.map(_ must beLike {
      case Left(ResourceError.throwableP(err)) =>
        err must_= ResourceError.pathNotFound(nonExistentPath)
    })
  }
}
