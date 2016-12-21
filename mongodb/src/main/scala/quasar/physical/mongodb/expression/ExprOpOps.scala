/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.mongodb.expression

import quasar.Predef._
import quasar._, Planner._
import quasar.physical.mongodb.Bson
import quasar.jscore, jscore.JsFn

import matryoshka._
import matryoshka.data.Fix
import scalaz._

trait ExprOpOps[IN[_]] {
  /** Type to be emitted from algebras. */
  // TODO: break out the members that use this parameter in a separate typeclass?
  type OUT[_]

  def simplify: AlgebraM[Option, IN, Fix[OUT]]

  def bson: Algebra[IN, Bson]

  /** "Literal" translation to JS. */
  def toJsSimple: AlgebraM[PlannerError \/ ?, IN, JsFn]

  def rewriteRefs0(applyVar: PartialFunction[DocVar, DocVar]): AlgebraM[Option, IN, Fix[OUT]]

  final def rewriteRefs(applyVar: PartialFunction[DocVar, DocVar])(implicit I: IN :<: OUT): Algebra[IN, Fix[OUT]] = {
    val r0 = rewriteRefs0(applyVar)
    x => r0(x).getOrElse(Fix(I.inj(x)))
  }
}
object ExprOpOps {
  /** Useful in implementations, when you need to require an instance with a
    * certain "output" type. */
  type Aux[IN[_], F[_]] = ExprOpOps[IN] { type OUT[A] = F[A] }

  /** For the typical use case where you want the in/out parameters to be the same. */
  type Uni[F[_]] = Aux[F, F]

  implicit def apply[F[_]](implicit ops: ExprOpOps.Aux[F, F]): ExprOpOps.Uni[F] = ops

  implicit def coproduct[F[_], G[_], H[_]](implicit
      F: ExprOpOps.Aux[F, H],
      G: ExprOpOps.Aux[G, H])
      : ExprOpOps.Aux[Coproduct[F, G, ?], H] =
    new ExprOpOps[Coproduct[F, G, ?]] {
      type OUT[A] = H[A]

      override def simplify =
        _.run.fold(F.simplify, G.simplify)

      val bson: Algebra[Coproduct[F, G, ?], Bson] =
        _.run.fold(F.bson(_), G.bson(_))

      val toJsSimple: AlgebraM[PlannerError \/ ?, Coproduct[F, G, ?], JsFn] =
        _.run.fold(F.toJsSimple(_), G.toJsSimple(_))

      override def rewriteRefs0(applyVar: PartialFunction[DocVar, DocVar]) = {
        val rf = F.rewriteRefs0(applyVar)
        val rg = G.rewriteRefs0(applyVar)
        _.run.fold(rf, rg)
      }
    }
}
