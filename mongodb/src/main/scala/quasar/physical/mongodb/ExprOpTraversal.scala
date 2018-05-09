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

package quasar.physical.mongodb

import quasar.fp._
import quasar.physical.mongodb.expression.ExprOp
import quasar.physical.mongodb.workflow._

import matryoshka._
import matryoshka.data.Fix

import monocle._
import scalaz._, Scalaz._

trait ExprOpTraversal[IN[_]] {
  def exprOps[A]: Traversal[IN[A], Fix[ExprOp]]
}

object ExprOpTraversal {

  implicit def workflowOpCore: ExprOpTraversal[WorkflowOpCoreF] =
    new ExprOpTraversal[WorkflowOpCoreF] {
      def exprOps[A]: Traversal[WorkflowOpCoreF[A], Fix[ExprOp]] =
        new Traversal[WorkflowOpCoreF[A], Fix[ExprOp]] {
          def modifyF[F[_]: Applicative](f: Fix[ExprOp] => F[Fix[ExprOp]])(s: WorkflowOpCoreF[A]): F[WorkflowOpCoreF[A]] =
            s match {
              case $PureF(_) =>
                Applicative[F].pure(s)
              case $ReadF(_) =>
                Applicative[F].pure(s)
              case $MatchF(_, _) =>
                Applicative[F].pure(s)
              case $ProjectF(src, shape, idExclusion) =>
                modifyReshape(f)(shape).map($ProjectF(src, _, idExclusion))
              case $RedactF(src, value) =>
                f(value).map($RedactF(src, _))
              case $LimitF(_, _) =>
                Applicative[F].pure(s)
              case $SkipF(_, _) =>
                Applicative[F].pure(s)
              case $UnwindF(_, _, _, _) =>
                Applicative[F].pure(s)
              case $GroupF(src, grouped, by) =>
                (modifyGrouped(f)(grouped) |@| modifyShape(f)(by))($GroupF(src, _, _))
              case $SortF(_, _) =>
                Applicative[F].pure(s)
              case $OutF(_, _) =>
                Applicative[F].pure(s)
              case $GeoNearF(_, _, _, _, _, _, _, _, _, _) =>
                Applicative[F].pure(s)
              case $MapF(_, _, _) =>
                Applicative[F].pure(s)
              case $SimpleMapF(_, _, _) =>
                Applicative[F].pure(s)
              case $FlatMapF(_, _, _) =>
                Applicative[F].pure(s)
              case $ReduceF(_, _, _) =>
                Applicative[F].pure(s)
              case $FoldLeftF(_, _) =>
                Applicative[F].pure(s)
              case $LookupF(_, _, _, _, _) =>
                Applicative[F].pure(s)
              case $SampleF(_, _) =>
                Applicative[F].pure(s)
            }
        }
    }

  implicit def workflowOp3_4: ExprOpTraversal[WorkflowOp3_4F] =
    new ExprOpTraversal[WorkflowOp3_4F] {
      def exprOps[A]: Traversal[WorkflowOp3_4F[A], Fix[ExprOp]] =
        new Traversal[WorkflowOp3_4F[A], Fix[ExprOp]] {
          def modifyF[F[_]: Applicative](f: Fix[ExprOp] => F[Fix[ExprOp]])(s: WorkflowOp3_4F[A]): F[WorkflowOp3_4F[A]] =
            s match {
              case $AddFieldsF(src, shape) =>
                modifyReshape(f)(shape).map($AddFieldsF(src, _))
            }
        }
    }

  implicit def coproduct[T[_[_]], G[_], H[_]]
    (implicit G: ExprOpTraversal[G], H: ExprOpTraversal[H])
      : ExprOpTraversal[Coproduct[G, H, ?]] =
    new ExprOpTraversal[Coproduct[G, H, ?]] {
      def exprOps[A]: Traversal[Coproduct[G, H, A], Fix[ExprOp]] =
        new Traversal[Coproduct[G, H, A], Fix[ExprOp]] {
          def modifyF[F[_]: Applicative](f: Fix[ExprOp] => F[Fix[ExprOp]])(s: Coproduct[G, H, A]): F[Coproduct[G, H, A]] = {
            s.run.bitraverse[F, G[A], H[A]](
              G.exprOps.modifyF(f),
              H.exprOps.modifyF(f)
            ).map(Coproduct(_))
          }
        }
    }

  def apply[F[_]](implicit ev: ExprOpTraversal[F]): ExprOpTraversal[F] = ev

  private def modifyShape[F[_]: Applicative, EX[_]](f: Fix[EX] => F[Fix[EX]])(s: Reshape.Shape[EX]): F[Reshape.Shape[EX]] =
    s.bitraverse(modifyReshape(f), f)

  private def modifyReshape[F[_]: Applicative, EX[_]](f: Fix[EX] => F[Fix[EX]])(s: Reshape[EX]): F[Reshape[EX]] =
    s.value.traverse(modifyShape(f)).map(Reshape(_))

  private def modifyGrouped[F[_]: Applicative, EX[_]](f: Fix[EX] => F[Fix[EX]])(s: Grouped[EX]): F[Grouped[EX]] =
    s.value.traverse(_.traverse(f)).map(Grouped(_))
}
