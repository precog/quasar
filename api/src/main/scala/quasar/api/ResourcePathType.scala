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

import slamdata.Predef.{Int, Product, Serializable, Some}

import scalaz.{Enum, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

/** Describes what a `ResourcePath` refers to. */
sealed trait ResourcePathType extends Product with Serializable

object ResourcePathType extends ResourcePathTypeInstances {
  case object Resource extends ResourcePathType
  case object ResourcePrefix extends ResourcePathType

  val resource: ResourcePathType =
    Resource

  val resourcePrefix: ResourcePathType =
    ResourcePrefix
}

sealed abstract class ResourcePathTypeInstances {
  implicit val enum: Enum[ResourcePathType] =
    new Enum[ResourcePathType] {
      def order(x: ResourcePathType, y: ResourcePathType) =
        toInt(x) ?|? toInt(y)

      def pred(t: ResourcePathType) =
        t match {
          case ResourcePathType.Resource => ResourcePathType.ResourcePrefix
          case ResourcePathType.ResourcePrefix  => ResourcePathType.Resource
        }

      def succ(t: ResourcePathType) =
        t match {
          case ResourcePathType.Resource => ResourcePathType.ResourcePrefix
          case ResourcePathType.ResourcePrefix  => ResourcePathType.Resource
        }

      override val min = Some(ResourcePathType.ResourcePrefix)

      override val max = Some(ResourcePathType.Resource)

      private def toInt(t: ResourcePathType): Int =
        t match {
          case ResourcePathType.ResourcePrefix => 0
          case ResourcePathType.Resource => 1
        }
    }

  implicit val show: Show[ResourcePathType] =
    Show.showFromToString
}
