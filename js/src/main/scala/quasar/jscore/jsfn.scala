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

package quasar.jscore

import quasar.RenderTree, RenderTree.ops._

import scalaz._, Scalaz._

/** Arbitrary javascript expression which is applied inline at compile time
  * (kinda like a macro)
  * @param param The free parameter to the expression
  */
final case class JsFn(param: Name, expr: JsCore) {
  def apply(x: JsCore): JsCore = expr.substitute(Ident(param), x)

  def >>>(that: JsFn): JsFn =
    if (this ≟ JsFn.identity) that
    else if (that ≟ JsFn.identity) this
    else JsFn(this.param, Let(that.param, this.expr, that.expr).simplify)

  override def toString = apply(ident("_")).toJs.pprint(0)

  // NB: Hanging around because other types lack an explicit equality.
  override def equals(obj: scala.Any) = obj match {
    case that @ JsFn(_, _) => this ≟ that
    case _                 => false
  }
}
object JsFn {
  val defaultName = Name("__val")

  val identity = JsFn(defaultName, Ident(defaultName))

  val const: JsCore => JsFn = JsFn(Name("__unused"), _)

  implicit val equal: Equal[JsFn] =
    Equal.equal(_(ident("_")).simplify ≟ _(ident("_")).simplify)

  implicit val renderTree: RenderTree[JsFn] = new RenderTree[JsFn] {
    def render(v: JsFn) = v(ident("_")).render
  }
}
