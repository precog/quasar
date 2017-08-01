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

package quasar.mimir

import quasar.blueeyes._, json._
import quasar.yggdrasil._
import quasar.yggdrasil.execution.EvaluationContext

import scalaz._, Scalaz._

trait MemoryDatasetConsumer[M[+ _]] extends EvaluatorModule[M] {
  type IdType

  type X      = Throwable
  type SEvent = (Vector[IdType], SValue)

  implicit def M: Monad[M] with Comonad[M]

  def Evaluator[N[+ _]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M): EvaluatorLike[N]

  def extractIds(jv: JValue): Seq[IdType]

  def consumeEval(graph: DepGraph, ctx: EvaluationContext, optimize: Boolean = true): Validation[X, Set[SEvent]] = {
    Validation.fromTryCatchNonFatal {
      implicit val nt = NaturalTransformation.refl[M]
      val evaluator   = Evaluator(M)
      val result      = evaluator.eval(graph, ctx, optimize)
      val json = result.flatMap(_.toJson).copoint filterNot { jvalue =>
        (jvalue \ "value") == JUndefined
      }

      var extractIdTime: Long      = 0L
      var jvalueToSValueTime: Long = 0L

      val events = json map { jvalue =>
        (Vector(extractIds(jvalue \ "key"): _*), jvalueToSValue(jvalue \ "value"))
      }

      val back = events.toSet
      evaluator.report.done.copoint
      back
    }
  }

  protected def jvalueToSValue(value: JValue): SValue = value match {
    case JUndefined      => sys.error("don't use jnothing; doh!")
    case JNull           => SNull
    case JBool(value)    => SBoolean(value)
    case JNum(bi)        => SDecimal(bi)
    case JString(str)    => SString(str)
    case JObject(fields) => SObject(fields mapValues jvalueToSValue toMap)
    case JArray(values)  => SArray(Vector(values map jvalueToSValue: _*))
  }
}

trait LongIdMemoryDatasetConsumer[M[+ _]] extends MemoryDatasetConsumer[M] {
  type IdType = SValue
  def extractIds(jv: JValue): Seq[SValue] = (jv --> classOf[JArray]).elements map jvalueToSValue
}
