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

package quasar.blueeyes.json

/** A difference between two JSONs (j1 diff j2).
  */
case class Diff(changed: JValue, added: JValue, deleted: JValue) {
  def map(f: JValue => JValue): Diff = {
    def applyTo(x: JValue) = x match {
      case JUndefined => JUndefined
      case _          => f(x)
    }
    Diff(applyTo(changed), applyTo(added), applyTo(deleted))
  }
  def merge(that: Diff) = Diff(changed merge that.changed, added merge that.added, deleted merge that.deleted)
}

/** Computes a diff between two JSONs.
  */
object Diff {

  /** Return a diff.
    */
  def diff(val1: JValue, val2: JValue): Diff = (val1, val2) match {
    case (x, y) if x == y                     => Diff(JUndefined, JUndefined, JUndefined)
    case (JObject(xs), JObject(ys))           => diffFields(xs, ys)
    case (JArray(xs), JArray(ys))             => diffVals(xs, ys)
    case (JNum(x), JNum(y)) if (x != y)       => Diff(JNum(y), JUndefined, JUndefined)
    case (JString(x), JString(y)) if (x != y) => Diff(JString(y), JUndefined, JUndefined)
    case (JBool(x), JBool(y)) if (x != y)     => Diff(JBool(y), JUndefined, JUndefined)
    case (x, y)                               => Diff(JUndefined, y, x)
  }

  private def diffFields(vs1: Map[String, JValue], vs2: Map[String, JValue]): Diff = {
    def diffRec(xleft: Map[String, JValue], yleft: Map[String, JValue]): Diff = {
      if (xleft.isEmpty) Diff(JUndefined, if (yleft.isEmpty) JUndefined else JObject(yleft), JUndefined)
      else {
        val x  = xleft.head
        val xs = xleft.tail

        yleft.get(x._1) match {
          case Some(yv) =>
            val diffedPair = diff(x._2, yv) map (v => JObject(JField(x._1, v) :: Nil))

            diffedPair merge (diffRec(xs, yleft - x._1))

          case None =>
            val Diff(c, a, d) = diffRec(xs, yleft)

            Diff(c, a, JObject(x) merge d)
        }
      }
    }

    diffRec(vs1, vs2)
  }

  private def diffVals(vs1: List[JValue], vs2: List[JValue]) = {
    def diffRec(xleft: List[JValue], yleft: List[JValue]): Diff = (xleft, yleft) match {
      case (xs, Nil) => Diff(JUndefined, JUndefined, if (xs.isEmpty) JUndefined else JArray(xs))
      case (Nil, ys) => Diff(JUndefined, if (ys.isEmpty) JUndefined else JArray(ys), JUndefined)
      case (x :: xs, y :: ys) =>
        val Diff(c1, a1, d1) = diff(x, y)
        val Diff(c2, a2, d2) = diffRec(xs, ys)
        Diff(c1 ++ c2, a1 ++ a2, d1 ++ d2)
    }

    diffRec(vs1, vs2)
  }
}
