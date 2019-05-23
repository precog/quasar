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

package quasar.api.resource

import slamdata.Predef.{Boolean, Int, Product, Serializable, Some}

import scalaz.{Enum, Order,  Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

/** The sorts of paths within a Datasource. */
sealed trait ResourcePathType extends Product with Serializable {
  def isPrefix: Boolean =
    this match {
      case ResourcePathType.Prefix => true
      case ResourcePathType.PrefixResource => true
      case ResourcePathType.LeafResource => false
      case ResourcePathType.AggregateResource => false
    }

  def isResource: Boolean =
    this match {
      case ResourcePathType.Prefix => false
      case ResourcePathType.PrefixResource => true
      case ResourcePathType.LeafResource => true
      case ResourcePathType.AggregateResource => true
    }
}

object ResourcePathType extends ResourcePathTypeInstances {
  sealed trait Physical extends ResourcePathType
  /** The path does not refer to a resource, but is a prefix of one or more paths. */
  case object Prefix extends Physical
  /** The path refers to a resource and is a prefix of one or more paths. */
  case object PrefixResource extends Physical
  /** The path refers to a resource and is not a prefix of any other paths. */
  case object LeafResource extends Physical

  /** The path refers to a resource that is aggregating other resources */
  case object AggregateResource extends ResourcePathType

  val leafResource: Physical =
    LeafResource

  val prefixResource: Physical =
    PrefixResource

  val prefix: Physical =
    Prefix
}

sealed abstract class ResourcePathTypeInstances {
  implicit val enum: Enum[ResourcePathType] =
    new Enum[ResourcePathType] {
      def order(x: ResourcePathType, y: ResourcePathType) =
        toInt(x) ?|? toInt(y)

      def pred(t: ResourcePathType) =
        t match {
          case ResourcePathType.Prefix => ResourcePathType.AggregateResource
          case ResourcePathType.PrefixResource => ResourcePathType.Prefix
          case ResourcePathType.LeafResource => ResourcePathType.PrefixResource
          case ResourcePathType.AggregateResource => ResourcePathType.LeafResource
        }

      def succ(t: ResourcePathType) =
        t match {
          case ResourcePathType.Prefix => ResourcePathType.PrefixResource
          case ResourcePathType.PrefixResource => ResourcePathType.LeafResource
          case ResourcePathType.LeafResource => ResourcePathType.AggregateResource
          case ResourcePathType.AggregateResource => ResourcePathType.Prefix
        }

      override val min = Some(ResourcePathType.Prefix)

      override val max = Some(ResourcePathType.AggregateResource)

      private def toInt(t: ResourcePathType): Int =
        t match {
          case ResourcePathType.Prefix => 0
          case ResourcePathType.PrefixResource => 1
          case ResourcePathType.LeafResource => 2
          case ResourcePathType.AggregateResource => 3
        }
    }

  implicit val orderPhysical: Order[ResourcePathType.Physical] = enum.contramap(x => x)

  implicit val show: Show[ResourcePathType] =
    Show.showFromToString
}
