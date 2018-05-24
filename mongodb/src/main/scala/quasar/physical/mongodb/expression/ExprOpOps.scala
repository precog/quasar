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

package quasar.physical.mongodb.expression

import slamdata.Predef._
import quasar.physical.mongodb.{Bson, BsonField}

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

trait ExprOpOps[IN[_]] {
  /** Type to be emitted from algebras. */
  // TODO: break out the members that use this parameter in a separate typeclass?
  type OUT[_]

  def simplify: AlgebraM[Option, IN, Fix[OUT]]

  def bson: Algebra[IN, Bson]

  def rebase[T](base: T)(implicit T: Recursive.Aux[T, OUT])
      : TransformM[Option, T, IN, OUT]

  def rewriteRefsM[M[_]: Monad](applyVar: PartialFunction[DocVar, M[DocVar]]): AlgebraM[(Option ∘ M)#λ, IN, Fix[OUT]]

  def rewriteRefs0(applyVar: PartialFunction[DocVar, DocVar]): AlgebraM[Option, IN, Fix[OUT]] =
    rewriteRefsM[Id](applyVar)

  final def rewriteRefs(applyVar: PartialFunction[DocVar, DocVar])(implicit I: IN :<: OUT): Algebra[IN, Fix[OUT]] = {
    val r0 = rewriteRefs0(applyVar)
    x => r0(x).getOrElse(Fix(I.inj(x)))
  }

  def mapUpFieldsM[M[_]: Monad]
    (f: BsonField => M[BsonField])
    (implicit I: IN :<: OUT)
      : AlgebraM[M, IN, Fix[OUT]] = {
    val applyVar: PartialFunction[DocVar, M[DocVar]] =
      { case dv => dv.deref.traverse(f) ∘ (x => dv.copy(deref = x)) }
    val r = rewriteRefsM(applyVar)
    x => r(x).getOrElse(Fix(I.inj(x)).point[M])
  }

}
object ExprOpOps {
  /** Useful in implementations, when you need to require an instance with a
    * certain "output" type. */
  type Aux[IN[_], F[_]] = ExprOpOps[IN] { type OUT[A] = F[A] }

  /** For the typical use case where you want the in/out parameters to be the same. */
  type Uni[F[_]] = Aux[F, F]

  implicit def apply[F[_]](implicit ops: ExprOpOps.Aux[F, F]): ExprOpOps.Uni[F] = ops

  def simplifyAlg[F[_]](implicit ev: Uni[F]): Algebra[F, Fix[F]] = in => apply[F].simplify(in).getOrElse(Fix(in))

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

      def rebase[T](base: T)(implicit T: Recursive.Aux[T, H]) =
        _.run.fold(F.rebase(base), G.rebase(base))

      def rewriteRefsM[M[_]: Monad](applyVar: PartialFunction[DocVar, M[DocVar]]) = {
        val rf = F.rewriteRefsM[M](applyVar)
        val rg = G.rewriteRefsM[M](applyVar)
        _.run.fold(rf, rg)
      }
    }
}
