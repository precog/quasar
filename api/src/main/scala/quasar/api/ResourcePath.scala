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

import slamdata.Predef._
import quasar.contrib.pathy.{firstSegmentName, rebaseA, stripPrefixA, AFile, APath}
import quasar.fp.ski.{ι, κ}

import monocle.{Iso, Prism}
import pathy.Path._
import scalaz.{ICons, IList, INil, Order, Show}

/** Identifies a resource in a datasource. */
sealed trait ResourcePath extends Product with Serializable {
  def fold[A](leaf: AFile => A, root: => A): A =
    this match {
      case ResourcePath.Leaf(f) => leaf(f)
      case ResourcePath.Root    => root
    }

  def /(name: ResourceName): ResourcePath =
    this match {
      case ResourcePath.Leaf(f) =>
        val d = fileParent(f)
        val n = fileName(f)
        ResourcePath.leaf(d </> dir(n.value) </> file(name.value))

      case ResourcePath.Root =>
        ResourcePath.leaf(rootDir </> file(name.value))
    }

  def /:(name: ResourceName): ResourcePath =
    this match {
      case ResourcePath.Leaf(f) =>
        ResourcePath.leaf(rebaseA(rootDir </> dir(name.value))(f))

      case ResourcePath.Root =>
        ResourcePath.leaf(rootDir </> file(name.value))
    }

  def ++(path: ResourcePath): ResourcePath =
    ResourcePath.resourceNamesIso(
      ResourcePath.resourceNamesIso.get(this) ++
        ResourcePath.resourceNamesIso.get(path))

  def toPath: APath =
    fold(ι, rootDir)

  def uncons: Option[(ResourceName, ResourcePath)] =
    ResourcePath.leaf.getOption(this).map(ResourcePath.unconsLeaf)
}

object ResourcePath extends ResourcePathInstances {
  final case class Leaf(file: AFile) extends ResourcePath
  case object Root extends ResourcePath

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  val resourceNamesIso: Iso[ResourcePath, IList[ResourceName]] =
    Iso[ResourcePath, IList[ResourceName]] {
      _.uncons match {
        case None => INil()
        case Some((name, path)) => name :: resourceNamesIso.get(path)
      }
    } {
      case INil() => Root
      case ICons(h, t) => h /: resourceNamesIso(t)
    }

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

  def unconsLeaf(file: AFile): (ResourceName, ResourcePath) = {
    val (n, p) = firstSegmentName(fileParent(file)) match {
      case None =>
        (fileName(file).value, ResourcePath.root())

      case Some(seg) =>
        val str = seg.fold(_.value, _.value)
        val fileAsDir = fileParent(file) </> dir(fileName(file).value)
        (str, ResourcePath.fromPath(stripPrefixA(rootDir </> dir(str))(fileAsDir)))
    }

    (ResourceName(n), p)
  }
}

sealed abstract class ResourcePathInstances {
  implicit val order: Order[ResourcePath] =
    Order.orderBy(_.toPath)

  implicit val show: Show[ResourcePath] =
    Show.shows { rp =>
      s"ResourcePath(${posixCodec.printPath(rp.toPath)})"
    }
}
