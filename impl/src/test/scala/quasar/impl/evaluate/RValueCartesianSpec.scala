/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar._
import quasar.ScalarStage.{Cartesian, Pivot}
import quasar.api.table.ColumnType
import quasar.common._
import quasar.common.data._

import scala.concurrent.ExecutionContext

import cats.effect.{ContextShift, IO}

import fs2.Stream

import org.specs2.matcher.Matcher
import org.typelevel.jawn.{AsyncParser, Facade}

import qdata.json.QDataFacade

object RValueCartesianSpec extends JsonSpec {
  import quasar.impl.evaluate.{RValueScalarStagesInterpreter => Interpreter}

  type JsonElement = RValue

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)

  "RValue Cartesian interpreter" should {

    val pivot = Pivot(IdStatus.ExcludeId, ColumnType.Array)

    val cartouches = Map(
      (CPathField("a_"), (CPathField("a"), List(pivot))),
      (CPathField("b_"), (CPathField("b"), List(pivot))),
      (CPathField("c_"), (CPathField("c"), List(pivot))),
      (CPathField("d_"), (CPathField("d"), List(pivot))))

    // cross size: 4*3*2*1=24
    val input = ldjson("""
      { "a": ["a1", "a2", "a3", "a4"], "b": ["b1", "b2", "b3"], "c": ["c1", "c2"], "d": ["d1"] }
      """)

    val expected = ldjson("""
      { "a_": "a1", "b_": "b1", "c_": "c1", "d_": "d1" }
      { "a_": "a1", "b_": "b1", "c_": "c2", "d_": "d1" }
      { "a_": "a1", "b_": "b2", "c_": "c1", "d_": "d1" }
      { "a_": "a1", "b_": "b2", "c_": "c2", "d_": "d1" }
      { "a_": "a1", "b_": "b3", "c_": "c1", "d_": "d1" }
      { "a_": "a1", "b_": "b3", "c_": "c2", "d_": "d1" }
      { "a_": "a2", "b_": "b1", "c_": "c1", "d_": "d1" }
      { "a_": "a2", "b_": "b1", "c_": "c2", "d_": "d1" }
      { "a_": "a2", "b_": "b2", "c_": "c1", "d_": "d1" }
      { "a_": "a2", "b_": "b2", "c_": "c2", "d_": "d1" }
      { "a_": "a2", "b_": "b3", "c_": "c1", "d_": "d1" }
      { "a_": "a2", "b_": "b3", "c_": "c2", "d_": "d1" }
      { "a_": "a3", "b_": "b1", "c_": "c1", "d_": "d1" }
      { "a_": "a3", "b_": "b1", "c_": "c2", "d_": "d1" }
      { "a_": "a3", "b_": "b2", "c_": "c1", "d_": "d1" }
      { "a_": "a3", "b_": "b2", "c_": "c2", "d_": "d1" }
      { "a_": "a3", "b_": "b3", "c_": "c1", "d_": "d1" }
      { "a_": "a3", "b_": "b3", "c_": "c2", "d_": "d1" }
      { "a_": "a4", "b_": "b1", "c_": "c1", "d_": "d1" }
      { "a_": "a4", "b_": "b1", "c_": "c2", "d_": "d1" }
      { "a_": "a4", "b_": "b2", "c_": "c1", "d_": "d1" }
      { "a_": "a4", "b_": "b2", "c_": "c2", "d_": "d1" }
      { "a_": "a4", "b_": "b3", "c_": "c1", "d_": "d1" }
      { "a_": "a4", "b_": "b3", "c_": "c2", "d_": "d1" }
      """)

    "run when parallelism exceeds largest number of possible fibers" in {
      val parallelism = 8
      val minUnit = 3

      input must cartesianInto(parallelism, minUnit, cartouches)(expected)
    }

    "run when not all fibers are used" in {
      val parallelism = 3
      val minUnit = 11

      input must cartesianInto(parallelism, minUnit, cartouches)(expected)
    }

    "run when largest size limits number of fibers" in {
      val parallelism = 3
      val minUnit = 8

      input must cartesianInto(parallelism, minUnit, cartouches)(expected)
    }

    "run when minUnit exceeds result size (1 fiber used)" in {
      val parallelism = 4
      val minUnit = 50

      input must cartesianInto(parallelism, minUnit, cartouches)(expected)
    }

    "run when parallelism is 1 (1 fiber used)" in {
      val parallelism = 1
      val minUnit = 10

      input must cartesianInto(parallelism, minUnit, cartouches)(expected)
    }
  }

  protected def ldjson(str: String): JsonStream = {
    implicit val facade: Facade[RValue] = QDataFacade[RValue](isPrecise = false)

    val parser: AsyncParser[RValue] =
      AsyncParser[RValue](AsyncParser.ValueStream)

    val events1: List[RValue] = parser.absorb(str).right.get.toList
    val events2: List[RValue] = parser.finish().right.get.toList

    events1 ::: events2
  }

  def evalCartesian(
      parallelism: Int,
      minUnit: Int,
      cartesian: Cartesian,
      stream: JsonStream): JsonStream =
    Stream.emits(stream)
      .through(Interpreter.interpretCartesian[IO](parallelism, minUnit, cartesian))
      .compile.toList
      .unsafeRunSync()

  def cartesianInto(
      parallelism: Int,
      minUnit: Int,
      cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])])(
      expected: JsonStream)
      : Matcher[JsonStream] =
    bestSemanticEqual(expected) ^^ { str: JsonStream =>
      evalCartesian(parallelism, minUnit, Cartesian(cartouches), str)
    }
}
