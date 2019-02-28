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

import quasar.{IdStatus, ScalarStageSpec}
import quasar.common.data.RValue

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO}

import fs2.Stream

import qdata.json.QDataFacade

import org.typelevel.jawn.{AsyncParser, Facade}

object RValueScalarStagesInterpreterSpec extends ScalarStageSpec {
  import quasar.impl.evaluate.{RValueScalarStagesInterpreter => Interpreter}

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)

  type JsonElement = RValue

  val idsPendingExamples: Set[Int] = Set()
  val wrapPendingExamples: Set[Int] = Set()
  val projectPendingExamples: Set[Int] = Set(8, 9)
  val maskPendingExamples: Set[Int] = Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 20, 21)
  val pivotPendingExamples: Set[Int] = Set(1, 2, 3, 4, 5, 6)
  val cartesianPendingExamples: Set[Int] = Set(7, 8)

  def evalIds(idStatus: IdStatus, stream: JsonStream): JsonStream =
    Stream.emits(stream)
      .through(Interpreter.interpretIdStatus(idStatus))
      .compile.toList

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    stream.flatMap(Interpreter.interpretMask(mask, _).toList)

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    stream.flatMap(Interpreter.interpretPivot(pivot, _))

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    stream.map(Interpreter.interpretWrap(wrap, _))

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    stream.flatMap(Interpreter.interpretProject(project, _))

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
