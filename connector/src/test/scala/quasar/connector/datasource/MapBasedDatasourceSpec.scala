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

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{DatasourceSpec, MonadResourceErr, ResourceError}
import quasar.contrib.scalaz.MonadError_

import cats.data.OptionT
import cats.effect.IO
import eu.timepit.refined.auto._
import scalaz.IMap
import scalaz.std.list._
import shims._

object MapBasedDatasourceSpec extends DatasourceSpec[IO, List, ResourcePathType.Physical] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val datasource =
    MapBasedDatasource.pure[IO, List](
      DatasourceType("pure-test", 1L),
      IMap(
        ResourcePath.root() / ResourceName("a") -> 0,
        ResourcePath.root() / ResourceName("a") / ResourceName("b") -> 1,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") -> 2,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") / ResourceName("d") -> 3,
        ResourcePath.root() / ResourceName("d") / ResourceName("e") -> 4,
        ResourcePath.root() / ResourceName("f") -> 5))

  def nonExistentPath: ResourcePath =
    ResourcePath.root() / ResourceName("x") / ResourceName("y")

  def gatherMultiple[A](fga: List[A]): IO[List[A]] =
    IO.pure(fga)

  def assertPrefixedChildPaths(path: ResourcePath, expected: Set[(ResourceName, ResourcePathType)]) =
    for {
      res <- OptionT(datasource.prefixedChildPaths(path))
        .getOrElseF(IO.raiseError(new Exception(s"Failed to list resources under $path")))
        .map(_.toSet).map(_ must_== expected)
    } yield res

  "evaluation" >> {
    "known resource returns result" >>* {
      datasource
        .evaluate(ResourcePath.root() / ResourceName("d") / ResourceName("e"))
        .map(_ must_=== 4)
    }

    "known prefix errors with 'not a resource'" >>* {
      val pfx = ResourcePath.root() / ResourceName("d")

      MonadResourceErr[IO].attempt(datasource.evaluate(pfx)).map(_ must be_-\/.like {
        case ResourceError.NotAResource(p) => p must equal(pfx)
      })
    }

    "unknown path errors with 'not found'" >>* {
      MonadResourceErr[IO].attempt(datasource.evaluate(nonExistentPath)).map(_ must be_-\/.like {
        case ResourceError.PathNotFound(p) => p must equal(nonExistentPath)
      })
    }

    "children of root is correct" >>* {
      assertPrefixedChildPaths(
        ResourcePath.root(),
        Set(
          ResourceName("a") -> ResourcePathType.prefixResource,
          ResourceName("d") -> ResourcePathType.prefix,
          ResourceName("f") -> ResourcePathType.leafResource))
    }

    "children of non-root is correct" >>* {
      assertPrefixedChildPaths(
        ResourcePath.root() / ResourceName("a"),
        Set(
          ResourceName("b") -> ResourcePathType.leafResource,
          ResourceName("c") -> ResourcePathType.prefixResource))
    }
  }
}
