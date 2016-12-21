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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._

import matryoshka._
import matryoshka.data.Fix
import scalaz._, Scalaz._

package object workflowtask {
  type WorkflowTask = Fix[WorkflowTaskF]

  type Pipeline = List[PipelineOp]

  // NB: it's only safe to emit "core" expr ops here, but we always use the
  // largest type here, so they're immediately injected into ExprOp.
  import fixExprOp._

  val simplifyProject: WorkflowOpCoreF[Unit] => Option[PipelineF[WorkflowOpCoreF, Unit]] =
    {
      case $ProjectF(src, Reshape(cont), id) =>
        $ProjectF(src,
          Reshape[ExprOp](cont.map {
            case (k, \/-($var(DocField(v)))) if k == v => k -> $include().right
            case x                                     => x
          }),
          id).pipeline.some
      case _ => None
    }

  def normalize: WorkflowTaskF ~> WorkflowTaskF =
    new (WorkflowTaskF ~> WorkflowTaskF) {
      def apply[α](wt: WorkflowTaskF[α]) = wt match {
        case PipelineTaskF(src, pipeline) =>
          PipelineTaskF(src, pipeline.map(_.rewrite[WorkflowOpCoreF](simplifyProject(_))))
        case x => x
      }
    }

  /** Run once a task is known to be completely built. */
  def finish(base: DocVar, task: WorkflowTask):
      (DocVar, WorkflowTask) = task match {
    case PipelineTask(src, pipeline) =>
      // possibly toss duplicate `_id`s created by `Unwind`s
      val uwIdx = pipeline.map(_.op.run).lastIndexWhere {
        case \/-($UnwindF(_, _)) => true
        case _ => false
      }
      // we’re fine if there’s no `Unwind`, or some existing op fixes the `_id`s
      if (uwIdx == -1 ||
        pipeline.map(_.op.run).indexWhere(
          { case \/-($GroupF(_, _, _))           => true
            case \/-($ProjectF(_, _, ExcludeId)) => true
            case _                              => false
          },
          uwIdx) != -1)
        (base, task)
      else shape(pipeline) match {
        case Some(names) =>
          (DocVar.ROOT(),
            PipelineTask(
              src,
              pipeline :+
                PipelineOp($ProjectF((),
                  Reshape[ExprOp](names.map(_ -> $include().right).toListMap),
                  ExcludeId).pipeline)))

        case None =>
          (ExprVar,
            PipelineTask(
              src,
              pipeline :+
                PipelineOp($ProjectF((),
                  Reshape[ExprOp](ListMap(ExprName -> $var(base).right)),
                  ExcludeId).pipeline)))
      }
    case _ => (base, task)
  }

  private def shape(p: Pipeline): Option[List[BsonField.Name]] = {
    def src = shape(p.dropRight(1))

    p.lastOption.flatMap(_.op match {
      case IsShapePreserving(_)                    => src

      case $project((), Reshape(shape), _)         => Some(shape.keys.toList)
      case $group((), Grouped(shape), _)           => Some(shape.keys.toList)
      case $unwind((), _)                          => src
      case $redact((), _)                          => None
      case $geoNear((), _, _, _, _, _, _, _, _, _) => src.map(_ :+ BsonField.Name("dist"))

      case $lookup((), _, _, _, as)                => src.map(_ :+ as.flatten.head)
    })
  }
}
