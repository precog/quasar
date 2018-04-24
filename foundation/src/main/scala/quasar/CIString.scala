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

package quasar

import slamdata.Predef._

import java.lang.Character

import argonaut.CodecJson
import monocle.Iso
import scalaz.{Order, Ordering, Show}

/** A case-insensitive string. */
final class CIString private (val value: String) {
  val length: Int =
    value.length

  override def equals(other: Any) = other match {
    case o @ CIString(_) => CIString.order.equal(this, o)
    case _               => false
  }

  /** Lazily cache the hash code. This is nearly identical to the
    * hashCode of java.lang.String, but converting to lower case on
    * the fly to avoid copying `value`'s character storage.
    *
    * Shamelessly cribbed from http4s, thank you to the authors
    * https://github.com/http4s/http4s/blob/v0.16.6/core/src/main/scala/org/http4s/util/CaseInsensitiveString.scala#L19
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[this] var hash = 0

  @SuppressWarnings(Array(
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.While"))
  override def hashCode: Int = {
    if (hash == 0) {
      var h = 0
      var i = 0
      val len = value.length
      while (i < len) {
        // Strings are equal igoring case if either their uppercase or lowercase
        // forms are equal. Equality of one does not imply the other, so we need
        // to go in both directions. A character is not guaranteed to make this
        // round trip, but it doesn't matter as long as all equal characters
        // hash the same.
        h = h * 31 + Character.toLowerCase(Character.toUpperCase(value.charAt(i)))
        i += 1
      }
      hash = h
    }
    hash
  }

  override def toString: String =
    value
}

object CIString extends CIStringInstances {
  def apply(s: String): CIString =
    new CIString(s)

  /** Always use lower-case view of string when pattern matching. */
  def unapply(cis: CIString): Option[String] =
    Some(cis.value.toLowerCase)

  val stringIso: Iso[CIString, String] =
    Iso((_: CIString).value)(CIString(_))
}

sealed abstract class CIStringInstances {
  implicit val order: Order[CIString] =
    Order.order { (x, y) =>
      Ordering.fromInt(x.value.compareToIgnoreCase(y.value))
    }

  implicit val show: Show[CIString] =
    Show.shows(_.value)

  implicit val codecJson: CodecJson[CIString] =
    CodecJson.derived[String].xmap(CIString(_))(_.value)
}
