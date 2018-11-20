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

package quasar.yggdrasil.table

import cats.effect.IO

import quasar.{CompositeParseType, IdStatus, ParseInstructionSpec, ParseType}
import quasar.common.CPath

import tectonic.{Plate, Signal}
import tectonic.json.Parser
import tectonic.test.{Event, ReifiedTerminalPlate}

object TectonicParseInstructionSpec extends ParseInstructionSpec {

  type JsonStream = List[Event]

  "MaskPlate" should {
    "consider skipping to be contents-seen" in {
      val events = List(
        Event.NestMap("a"),
        Event.Skipped(42),
        Event.Unnest,
        Event.FinishRow)

      val plateF = ReifiedTerminalPlate[IO].flatMap(
        MaskPlate[IO, List[Event]](
          Mask(Map(CPath.parse(".a") -> Set(ParseType.Object))), _))

      val plate = plateF.unsafeRunSync()

      plate.nestMap("a") mustEqual Signal.Continue
      plate.skipped(42)
      plate.unnest() mustEqual Signal.Continue
      plate.finishRow()
      (plate.finishBatch(false) ++ plate.finishBatch(true)) mustEqual events
    }
  }

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    evalPlate(stream)(MaskPlate[IO, List[Event]](mask, _))

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    evalPlate(stream)(WrapPlate[IO, List[Event]](wrap, _))

  def evalIds(stream: JsonStream): JsonStream =
    evalPlate(stream)(IdsPlate[IO, List[Event]](_))

  def evalSinglePivot(path: CPath, idStatus: IdStatus, structure: CompositeParseType, stream: JsonStream)
      : JsonStream =
    evalPlate(stream)(SinglePivotPlate[IO, List[Event]](path, idStatus, structure, _))

  private def evalPlate(stream: JsonStream)(f: Plate[List[Event]] => IO[Plate[List[Event]]]): JsonStream = {
    val plate = ReifiedTerminalPlate[IO].flatMap(f).unsafeRunSync()
    stripNoOps(ReifiedTerminalPlate.visit(stream, plate))
  }

  // remove no-op nesting (paired adjacent nest/unnest)
  private def stripNoOps(stream: JsonStream): JsonStream = {
    import Event._

    val (_, backV) = stream.foldLeft((Vector[Event](), Vector[Event]())) {
      case ((Vector(), acc), Unnest) => (Vector(), acc :+ Unnest)

      case ((buffer, acc), n @ NestMap(_)) => (buffer :+ n, acc)
      case ((buffer, acc), NestArr) => (buffer :+ NestArr, acc)
      case ((buffer, acc), n @ NestMeta(_)) => (buffer :+ n, acc)

      case ((buffer, acc), Unnest) => (buffer.dropRight(1), acc)

      case ((buffer, acc), e) => (Vector(), acc ++ buffer :+ e)
    }

    backV.toList
  }

  protected def ldjson(str: String): JsonStream = {
    val eff = for {
      parser <- Parser(ReifiedTerminalPlate[IO], Parser.ValueStream)

      events1 <- parser.absorb(str)
      events2 <- parser.finish
    } yield events1.right.get ::: events2.right.get

    eff.unsafeRunSync()
  }
}
