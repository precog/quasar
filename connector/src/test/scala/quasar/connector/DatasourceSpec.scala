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

package quasar.connector

import slamdata.Predef.{Any, List, StringContext}
import quasar.EffectfulQSpec
import quasar.api.resource._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats.effect.Effect

import org.specs2.matcher.MatchResult

import scalaz.Scalaz._

import shims.monadToScalaz

abstract class DatasourceSpec[F[_]: Effect, G[_], P <: ResourcePathType]
    extends EffectfulQSpec[F] {

  def datasource: Datasource[F, G, _, _, P]

  def nonExistentPath: ResourcePath

  def nonExistentPathWithAsterisk: ResourcePath = nonExistentPath / ResourceName("*")

  def gatherMultiple[A](fga: G[A]): F[List[A]]

  private val widthLimit = 10

  /** Confirms that all discovered paths are reported to exist and any
    * resource paths are confirmed as such by `pathIsResource`.
    */
  private def checkAgreementUnder(pfx: ResourcePath): F[MatchResult[Any]] =
    for {
      r <- datasource.prefixedChildPaths(pfx)

      children <- r.traverse(gatherMultiple)

      // Try and keep the test reasonably fast if executed on a dense datasource
      limited <- children traverse { xs =>
        Effect[F].delay(Random.shuffle(xs).take(widthLimit))
      }

      agree <- limited.fold(ko(s"Expected ${pfx.shows} to exist.").point[F]) { xs =>
        xs.foldLeftM(ok) { case (mr, (n, tpe)) =>
          val path = pfx / n

          val tpeResult =
            if (tpe.isResource)
              datasource.pathIsResource(path).ifM(
                checkAgreementUnder(path),
                ko(s"Expected ${path.shows} to be a resource.").point[F])
            else
              datasource.pathIsResource(path).ifM(
                ko(s"Expected ${path.shows} not to be a resource.").point[F],
                checkAgreementUnder(path))

          tpeResult.map(mr and _)
        }
      }
    } yield agree

  "resource discovery" >> {
    "must not be empty" >>* {
      datasource
        .prefixedChildPaths(ResourcePath.root())
        .flatMap(_.traverse(gatherMultiple))
        .map(_.any(g => !g.empty))
    }

    "prefixed child status agrees with pathIsResource" >>* {
      checkAgreementUnder(ResourcePath.root())
    }

    "children of non-existent is not found" >>* {
      datasource
        .prefixedChildPaths(nonExistentPath)
        .map(_ must beNone)
    }

    "non-existent is not a resource" >>* {
      datasource
        .pathIsResource(nonExistentPath)
        .map(_ must beFalse)
    }

    "non-existent * is not a resource" >>* {
      datasource
        .pathIsResource(nonExistentPathWithAsterisk)
        .map(_ must beFalse)
    }
  }
}
