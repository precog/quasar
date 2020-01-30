/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.api.destination

import scala.AnyVal

import java.lang.String

/** A semantic, human consumable description.
  *
  * In contrast to `Show`, which is more representation focused, `Label`
  * focuses on semantics and aims to be suitable for display to an end-user
  * of the system.
  */
trait Label[A] {
  def label(a: A): String
}

object Label {

  def apply[A](implicit A: Label[A]): Label[A] = A

  def label[A](f: A => String): Label[A] =
    new Label[A] { def label(a: A) = f(a) }

  object Syntax {
    implicit final class EnrichedA[A](val self: A) extends AnyVal {
      def label(implicit A: Label[A]): String = A.label(self)
    }
  }
}
