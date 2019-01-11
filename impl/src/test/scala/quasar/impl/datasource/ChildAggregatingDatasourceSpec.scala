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

package quasar.impl.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector._
import quasar.contrib.fs2.stream._
import quasar.contrib.scalaz.MonadError_

import cats.effect.IO

import eu.timepit.refined.auto._

import fs2.Stream

import monocle.Lens

import scalaz.IMap
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

import shims._

object ChildAggregatingDatasourceSpec extends DatasourceSpec[IO, Stream[IO, ?]] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val underlying =
    PureDatasource[IO, Stream[IO, ?]](
      DatasourceType("pure-test", 1L),
      IMap(
        ResourcePath.root() / ResourceName("a") / ResourceName("b") -> 1,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") -> 2,
        ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("r") -> 3,
        ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("s") -> 4,
        ResourcePath.root() / ResourceName("d") -> 5))

  val datasource =
    ChildAggregatingDatasource.composite(underlying, Lens.id)

  def nonExistentPath: ResourcePath =
    ResourcePath.root() / ResourceName("x") / ResourceName("y")

  def gatherMultiple[A](fga: Stream[IO, A]): IO[List[A]] =
    fga.compile.to[List]

  "aggregate discovery" >> {
    "underlying prefix resources are preserved" >>* {
      val z = ResourcePath.root() / ResourceName("z")
      val x = ResourceName("x")
      val y = ResourceName("y")

      val paths = IMap(z -> 1, (z / x) -> 2, (z / y) -> 3)

      val uds =
        new Datasource[IO, Stream[IO, ?], ResourcePath, Int] {
          val kind = DatasourceType("prefixed", 6L)

          def evaluate(rp: ResourcePath): IO[Int] =
            IO.pure(paths.lookup(rp) getOrElse -1)

          def pathIsResource(rp: ResourcePath): IO[Boolean] =
            IO.pure(paths.member(rp))

          def prefixedChildPaths(rp: ResourcePath)
              : IO[Option[Stream[IO, (ResourceName, ResourcePathType)]]] =
            IO pure {
              if (rp ≟ ResourcePath.root())
                Some(Stream(ResourceName("z") -> ResourcePathType.prefixResource))
              else if (rp ≟ z)
                Some(Stream(
                  ResourceName("x") -> ResourcePathType.leafResource,
                  ResourceName("y") -> ResourcePathType.leafResource))
              else if (paths.member(rp))
                Some(Stream.empty)
              else
                None
            }
        }

      val ds = ChildAggregatingDatasource.composite(uds, Lens.id)

      for {
        dres <- ds.prefixedChildPaths(ResourcePath.root())
        meta <- dres.traverse(_.compile.to[List])
        qres <- ds.evaluate(z)
      } yield {
        meta must beSome(equal(List(ResourceName("z") -> ResourcePathType.prefixResource)))
        qres must beLeft(1)
      }
    }

    "underlying prefix paths are resources" >>* {
      datasource
        .pathIsResource(ResourcePath.root() / ResourceName("a"))
        .map(_ must beTrue)
    }

    "underlying prefix paths are typed as prefix-resource" >>* {
      datasource
        .prefixedChildPaths(ResourcePath.root() / ResourceName("a"))
        .flatMap(_.cata(_.compile.to[List], IO.pure(Nil)))
        .map(_ must contain(ResourceName("q") -> ResourcePathType.prefixResource))
    }
  }

  "evaluation" >> {
    "querying a non-existent path is not found" >>* {
      MonadResourceErr[IO].attempt(datasource.evaluate(nonExistentPath)).map(_ must be_-\/.like {
        case ResourceError.PathNotFound(p) => p must equal(nonExistentPath)
      })
    }

    "querying an underlying resource is unaffected" >>* {
      datasource
        .evaluate(ResourcePath.root() / ResourceName("d"))
        .map(_ must beLeft(5))
    }

    "querying an underlying prefix aggregates sibling leafs" >>* {
      val b = ResourcePath.root() / ResourceName("a") / ResourceName("b")
      val c = ResourcePath.root() / ResourceName("a") / ResourceName("c")

      datasource
        .evaluate(ResourcePath.root() / ResourceName("a"))
        .flatMap(_.traverse(_.compile.to[List]))
        .map(_ must beRight(contain(exactly((b, 1), (c, 2)))))
    }
  }
}
