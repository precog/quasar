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

package quasar.api

import slamdata.Predef.{Product, Serializable, Unit}
import quasar.contrib.pathy.{AFile, APath}
import quasar.fp.ski.{ι, κ}

import monocle.Prism
import pathy.Path._
import scalaz.{Cord, Order, Show}
import scalaz.syntax.show._

/** Identifies a resource in a datasource. */
sealed trait ResourcePath extends Product with Serializable {
  def fold[A](leaf: AFile => A, root: => A): A =
    this match {
      case ResourcePath.Leaf(f) => leaf(f)
      case ResourcePath.Root    => root
    }

  def / (name: ResourceName): ResourcePath =
    this match {
      case ResourcePath.Leaf(f) =>
        val d = fileParent(f)
        val n = fileName(f)
        ResourcePath.leaf(d </> dir(n.value) </> file(name.value))

      case ResourcePath.Root =>
        ResourcePath.leaf(rootDir </> file(name.value))
    }

  def toPath: APath =
    fold(ι, rootDir)
}

object ResourcePath extends ResourcePathInstances {
  final case class Leaf(file: AFile) extends ResourcePath
  case object Root extends ResourcePath

  val leaf: Prism[ResourcePath, AFile] =
    Prism.partial[ResourcePath, AFile] {
      case Leaf(f) => f
    } (Leaf)

  val root: Prism[ResourcePath, Unit] =
    Prism.partial[ResourcePath, Unit] {
      case Root => ()
    } (κ(Root))

  def fromPath(path: APath): ResourcePath =
    peel(path).fold(root()) {
      case (parent, name) =>
        leaf(parent </> file1(name.valueOr(d => FileName(d.value))))
    }
}

sealed abstract class ResourcePathInstances {
  implicit val order: Order[ResourcePath] =
    Order.orderBy(_.toPath)

  implicit val show: Show[ResourcePath] =
    Show.show { rp =>
      Cord("ResourcePath(") ++ rp.toPath.show ++ Cord(")")
    }
}
