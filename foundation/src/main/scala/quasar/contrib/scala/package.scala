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

package quasar.contrib

import slamdata.Predef._
import quasar.fp.ski._

import _root_.scalaz._, Scalaz._

package object std {
  implicit class AugmentedList[A](val a: List[A]) extends scala.AnyVal {
    def duplicates: List[NonEmptyList[A]] = {
      a.groupBy1(ι).values.filter(_.size > 1).toList
    }
  }

  implicit class AugmentedLong(val l: Long) extends scala.AnyVal {
    /** A safe version of `Long.toInt` which returns a `None` if the Long value is too large for an Int */
    // TODO: Find and remove instances of `toInt` which have crept their way into the code base
    // TODO: Enable some code linting tool to catch usages of `Long.toInt`
    def toIntSafe: Option[Int] =
      if (l > Int.MaxValue || l < Int.MinValue) None else Some(l.toInt)
  }
}