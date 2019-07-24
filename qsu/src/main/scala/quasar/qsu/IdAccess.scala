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

package quasar.qsu

import slamdata.Predef._
import quasar.RenderTree
import quasar.fp.symbolOrder

import monocle.{Prism, Traversal}
import scalaz.{Applicative, Equal, Order, Show}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._

/** Describes access to the various forms of ids. */
sealed abstract class IdAccess

object IdAccess extends IdAccessInstances {
  final case class Bucket(of: Symbol, idx: Int) extends IdAccess
  final case class GroupKey(of: Symbol, idx: Int) extends IdAccess
  final case class Identity(of: Symbol) extends IdAccess

  val bucket: Prism[IdAccess, (Symbol, Int)] =
    Prism.partial[IdAccess, (Symbol, Int)] {
      case Bucket(s, i) => (s, i)
    } { case (s, i) => Bucket(s, i) }

  val groupKey: Prism[IdAccess, (Symbol, Int)] =
    Prism.partial[IdAccess, (Symbol, Int)] {
      case GroupKey(s, i) => (s, i)
    } { case (s, i) => GroupKey(s, i) }

  val identity: Prism[IdAccess, Symbol] =
    Prism.partial[IdAccess, Symbol] {
      case Identity(s) => s
    } (Identity(_))

  val symbols: Traversal[IdAccess, Symbol] =
    new Traversal[IdAccess, Symbol] {
      def modifyF[F[_]: Applicative](f: Symbol => F[Symbol])(s: IdAccess) =
        s match {
          case Bucket(s, i)   => f(s) map (bucket(_, i))
          case GroupKey(s, i) => f(s) map (groupKey(_, i))
          case Identity(s)    => f(s) map (identity(_))
        }
    }
}

sealed abstract class IdAccessInstances extends IdAccessInstances0 {
  import IdAccess._

  implicit val order: Order[IdAccess] =
    Order.orderBy(generic(_))

  implicit val show: Show[IdAccess] =
    Show.shows {
      case Bucket(s, i)   => s"Bucket($s[$i])"
      case GroupKey(s, i) => s"GroupKey($s[$i])"
      case Identity(s)    => s"Identity($s)"
    }

  implicit val renderTree: RenderTree[IdAccess] =
    RenderTree.fromShowAsType("IdAccess")
}

sealed abstract class IdAccessInstances0 {
  import IdAccess.{bucket, groupKey, identity}

  implicit val equal: Equal[IdAccess] =
    Equal.equalBy(generic(_))

  protected def generic(a: IdAccess) =
    (bucket.getOption(a), groupKey.getOption(a), identity.getOption(a))
}
