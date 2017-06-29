/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil

import quasar.blueeyes._, json._
import scalaz._, Scalaz._

/**
 * This provides an ordering on JValue that mimics how we'd order them as
 * columns in a table, rather than using JValue's default ordering which
 * behaves differently.
 */
trait PrecogJValueOrder extends scalaz.Order[JValue] {
  def order(a: JValue, b: JValue): Ordering = {
    val prims0 = a.flattenWithPath.toMap
    val prims1 = b.flattenWithPath.toMap
    val cols0  = (prims1.mapValues { _ => JUndefined } ++ prims0).toList.sortMe
    val cols1  = (prims0.mapValues { _ => JUndefined } ++ prims1).toList.sortMe

    scalaz.Order[Vector[(JPath, JValue)]].order(cols0, cols1)
  }
}

object PrecogJValueOrder {
  implicit object order extends PrecogJValueOrder
  implicit def ordering: scala.math.Ordering[JValue] = order.toScalaOrdering
}
