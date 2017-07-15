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

package quasar.physical.mongodb.workflow

import scalaz._, Scalaz._

sealed abstract class CardinalExpr[A]
final case class MapExpr[A](fn: A)  extends CardinalExpr[A]
/** Like `MapExpr`, but stores the result of `fn` in `place` (which must be
  * assignable).
  */
final case class SubExpr[A](place: A, fn: A)  extends CardinalExpr[A]
final case class FlatExpr[A](fn: A) extends CardinalExpr[A]

object CardinalExpr {
  implicit val traverse: Traverse[CardinalExpr] =
    new Traverse[CardinalExpr] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: CardinalExpr[A])(f: A => G[B]):
          G[CardinalExpr[B]] =
        fa match {
          case MapExpr(e)  => f(e).map(MapExpr(_))
          case SubExpr(p, e) => (f(p) ⊛ f(e))(SubExpr(_, _))
          case FlatExpr(e) => f(e).map(FlatExpr(_))
        }
    }

  implicit def equal[A: Equal]: Equal[CardinalExpr[A]] = new Equal[CardinalExpr[A]] {

    def equal(left: CardinalExpr[A], right: CardinalExpr[A]) = (left, right) match {
      case (MapExpr(lfn), MapExpr(rfn)) => lfn === rfn
      case (SubExpr(lp, lfn), SubExpr(rp, rfn)) => lp === rp && lfn === rfn
      case (FlatExpr(lfn), FlatExpr(rfn)) => lfn === rfn
      case _ => false
    }

    override def equalIsNatural = Equal[A].equalIsNatural
  }
}
