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

package quasar.impl.evaluate

import slamdata.Predef.{Stream => _, _}

import quasar.{IdStatus, ScalarStage, ScalarStageSpec}
import quasar.common.data.RValue

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO}

import fs2.Stream

import org.specs2.matcher.Matcher

import org.typelevel.jawn.{AsyncParser, Facade}

import qdata.json.QDataFacade

import scalaz.NonEmptyList

object RValueScalarStagesInterpreterSpec extends ScalarStageSpec {
  import quasar.impl.evaluate.{RValueScalarStagesInterpreter => Interpreter}

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)

  type JsonElement = RValue

  val idsPendingExamples: Set[Int] = Set()
  val wrapPendingExamples: Set[Int] = Set()
  val projectPendingExamples: Set[Int] = Set()
  val maskPendingExamples: Set[Int] = Set()
  val pivotPendingExamples: Set[Int] = Set()
  val focusedPendingExamples: Set[Int] = Set()
  val cartesianPendingExamples: Set[Int] = Set()

  override def bestSemanticEqual(js: JsonStream): Matcher[JsonStream] = {
    containTheSameElementsAs(js) ^^ { str: JsonStream =>
      str.map(RValue.removeUndefined) collect { case Some(v) => v }
    }
  }

  def evalIds(idStatus: IdStatus, stream: JsonStream): JsonStream =
    Stream.emits(stream)
      .through(Interpreter.interpretIdStatus(idStatus))
      .compile.toList

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    stream.map(Interpreter.interpretMask(mask, _))

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    stream.flatMap(Interpreter.interpretPivot(pivot, _))

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    stream.map(Interpreter.interpretWrap(wrap, _))

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    stream.map(Interpreter.interpretProject(project, _))

  def evalFocused(stages: List[ScalarStage.Focused], stream: JsonStream): JsonStream =
    if (stages.isEmpty)
      stream
    else
      stream.flatMap(Interpreter.interpretFocused(
        NonEmptyList(stages.head, stages.tail:_*), _)).toList

  def evalCartesian(cartesian: Cartesian, stream: JsonStream): JsonStream = {
    val parallelism = java.lang.Runtime.getRuntime().availableProcessors()
    val minUnit = 1024

    Stream.emits(stream)
      .through(Interpreter.interpretCartesian[IO](parallelism, minUnit, cartesian))
      .compile.toList
      .unsafeRunSync()
  }

  protected def ldjson(str: String): JsonStream = {
    implicit val facade: Facade[RValue] = QDataFacade[RValue](isPrecise = false)

    val parser: AsyncParser[RValue] =
      AsyncParser[RValue](AsyncParser.ValueStream)

    val events1: List[RValue] = parser.absorb(str).right.get.toList
    val events2: List[RValue] = parser.finish().right.get.toList

    events1 ::: events2
  }
}
