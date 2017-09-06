/* MIT License
 * Copyright (c)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

// From typelevel/specs2-scalaz which is released infrequently

package quasar.contrib.specs2.scalaz

import slamdata.Predef._

import scala.language.implicitConversions

import org.specs2._
import org.specs2.matcher._
import org.specs2.specification.core._
import org.specs2.scalacheck._
import org.specs2.main.{ArgumentsShortcuts, ArgumentsArgs}
import org.scalacheck.{Prop, Properties}
import org.scalacheck.util.{FreqMap, Pretty}

/** A minimal version of the Specs2 mutable base class */
trait Spec extends org.specs2.mutable.Spec with ScalaCheck
  with MatchersImplicits with StandardMatchResults
  with ArgumentsShortcuts with ArgumentsArgs {

  val ff = fragmentFactory; import ff._

  setArguments(fullStackTrace)

  def checkAll(name: String, props: Properties)(implicit p: Parameters, f: FreqMap[Set[Any]] => Pretty) = {
    addFragment(text(s"$name  ${props.name} must satisfy"))
    addFragments(Fragments.foreach(props.properties) { case (name, prop) => Fragments(name in check(prop, p, f)) })
  }

  def checkAll(props: Properties)(implicit p: Parameters, f: FreqMap[Set[Any]] => Pretty) = {
    addFragment(text(s"${props.name} must satisfy"))
    addFragments(Fragments.foreach(props.properties) { case (name, prop) => Fragments(name in check(prop, p, f)) })
  }

  implicit def enrichProperties(props: Properties) = new {
    def withProp(propName: String, prop: Prop) = new Properties(props.name) {
      for {(name, p) <- props.properties} property(name) = p
      property(propName) = prop
    }
  }

}

