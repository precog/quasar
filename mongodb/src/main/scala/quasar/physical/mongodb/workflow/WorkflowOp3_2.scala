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

package quasar.physical.mongodb.workflow

import quasar.Predef._
import quasar.{RenderTree, Terminal}
import quasar.physical.mongodb.{Bson, BsonField, CollectionName}

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

/** Ops that are provided by MongoDB since 3.2. */
sealed abstract class WorkflowOp3_2F[+A]

final case class $LookupF[A](
  src: A,
  from: CollectionName,
  localField: BsonField,
  foreignField: BsonField,
  as: BsonField)
  extends WorkflowOp3_2F[A] { self =>
  def pipeline: PipelineF[WorkflowOp3_2F, A] =
    new PipelineF[WorkflowOp3_2F, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = self.copy(src = newSrc).pipeline

      def op = "$lookup"
      def rhs = Bson.Doc(ListMap(
        "from" -> from.bson,
        "localField" -> localField.bson,
        "foreignField" -> foreignField.bson,
        "as" -> as.bson
      ))
    }
}
object $lookup {
  def apply[F[_]: Coalesce](
    from: CollectionName,
    localField: BsonField,
    foreignField: BsonField,
    as: BsonField)
    (implicit I: WorkflowOp3_2F :<: F): FixOp[F] =
      src => Fix(Coalesce[F].coalesce(I.inj($LookupF(src, from, localField, foreignField, as))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOp3_2F :<: F)
    : Option[(A, CollectionName, BsonField, BsonField, BsonField)] =
    I.prj(op) collect {
      case $LookupF(src, from, lf, ff, as) => (src, from, lf, ff, as)
    }
}

final case class $SampleF[A](src: A, size: Int)
  extends WorkflowOp3_2F[A] { self =>
  def shapePreserving: ShapePreservingF[WorkflowOp3_2F, A] =
    new ShapePreservingF[WorkflowOp3_2F, A] {
      def wf = self
      def src = self.src
      def reparent[B](newSrc: B) = copy(src = newSrc).shapePreserving

      def op = "$sample"
      def rhs = Bson.Int32(size)
    }
}
object $sample {
  def apply[F[_]: Coalesce](size: Int)(implicit I: WorkflowOp3_2F :<: F): FixOp[F] =
    src => Fix(Coalesce[F].coalesce(I.inj($SampleF(src, size))))

  def unapply[F[_], A](op: F[A])(implicit I: WorkflowOp3_2F :<: F)
    : Option[(A, Int)] =
    I.prj(op) collect {
      case $SampleF(src, size) => (src, size)
    }
  }

object WorkflowOp3_2F {
  // NB: this extractor has to be used instead of the simpler ones provided for
  // each op if you need to bind the op itself, and not just its fields.
  // For example: `case $lookup(src, from, lf, ff, as) => ` vs.
  // `case WorkflowOp3_2F(l @ $LookupF(_, _, _, _, _)) =>`
  def unapply[F[_], A](fa: F[A])(implicit I: WorkflowOp3_2F :<: F): Option[WorkflowOp3_2F[A]] =
    I.prj(fa)

  implicit val traverse: Traverse[WorkflowOp3_2F] =
    new Traverse[WorkflowOp3_2F] {
      def traverseImpl[G[_], A, B](fa: WorkflowOp3_2F[A])(f: A => G[B])
        (implicit G: Applicative[G]):
          G[WorkflowOp3_2F[B]] = fa match {
        case $LookupF(src, from, localField, foreignField, as) =>
          G.apply(f(src))($LookupF(_, from, localField, foreignField, as))
        case $SampleF(src, size)       => G.apply(f(src))($SampleF(_, size))
      }
    }

  implicit val refs: Refs[WorkflowOp3_2F] = Refs.fromRewrite[WorkflowOp3_2F](rewriteRefs3_2)

  implicit lazy val coalesce: Coalesce[Workflow3_2F] = coalesceAll[Workflow3_2F]

  implicit val classify: Classify[WorkflowOp3_2F] =
    new Classify[WorkflowOp3_2F] {
      override def source[A](op: WorkflowOp3_2F[A]) =
        None

      override def singleSource[A](op: WorkflowOp3_2F[A]) = op match {
        case op @ $LookupF(_, _, _, _, _) => op.pipeline.widen[A].some
        case op @ $SampleF(_, _)          => op.shapePreserving.widen[A].some
      }

      override def pipeline[A](op: WorkflowOp3_2F[A]) = op match {
        case op @ $LookupF(_, _, _, _, _) => op.pipeline.widen[A].some
        case op @ $SampleF(_, _)          => op.shapePreserving.widen[A].some
      }

      override def shapePreserving[A](op: WorkflowOp3_2F[A]) = op match {
        case op @ $SampleF(_, _)          => op.shapePreserving.widen[A].some
        case _ => None
      }
    }

  implicit val renderTree: RenderTree[WorkflowOp3_2F[Unit]] =
    new RenderTree[WorkflowOp3_2F[Unit]] {
      val wfType = "Workflow3.2" :: Nil

      def render(v: WorkflowOp3_2F[Unit]) = v match {
        case $LookupF(_, from, localField, foreignField, as) =>
          Terminal("$LookupF" :: wfType, s"from ${from.value} with (this).${localField.asText} = (that).${foreignField.asText} as ${as.asText}".some)
        case $SampleF(_, size) =>
          Terminal("$SampleF" :: wfType, size.toString.some)
      }
    }
}
