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

package quasar.mimir.evaluate

import slamdata.Predef.{Stream => _, _}

import quasar.ParseInstruction
import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.impl.evaluate.{Source => EvalSource}
import quasar.mimir._, MimirCake._
import quasar.mimir.evaluate.Config.{Associates, AssociatesT, EvaluatorConfig}
import quasar.precog.common.RValue
import quasar.qscript._, PlannerError.InternalError
import quasar.yggdrasil.{MonadFinalizers, TransSpecModule}

import cats.effect.{ContextShift, IO, LiftIO}
import fs2.{Chunk, Stream}
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

import scala.concurrent.ExecutionContext
import scala.collection.mutable.ArrayBuffer

final class FederatedShiftedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: LiftIO: Monad: MonadPlannerErr: MonadFinalizers[?[_], IO]](
    val P: Cake)(
    implicit
    cs: ContextShift[IO],
    ec: ExecutionContext) {

  type Assocs = Associates[T, IO]
  type M[A] = AssociatesT[T, F, IO, A]

  type Read[A] = Const[ShiftedRead[AFile], A] \/ Const[InterpretedRead[AFile], A]

  def plan: AlgebraM[M, Read, MimirRepr] = {
    case -\/(Const(ShiftedRead(file, status))) =>
      planRead(file, status, List())

    case \/-(Const(InterpretedRead(file, instructions))) =>
      planRead(file, ExcludeId, instructions)
  }

  ////

  private val dsl = construction.mkGeneric[T, QScriptRead[T, ?]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private def planRead(file: AFile, readStatus: IdStatus, instructions: List[ParseInstruction])
      : M[MimirRepr] = {

    val sourceM: M[Option[EvalSource[QueryAssociate[T, IO]]]] =
      Kleisli.ask[F, EvaluatorConfig[T, IO]].map(_.associates(file))

    sourceM andThenK { maybeSource =>
      for {
        source <- MonadPlannerErr[F].unattempt_(
          maybeSource \/> InternalError.fromMsg(s"No source for '${posixCodec.printPath(file)}'."))

        tbl <- sourceTable(source, instructions)

        repr = MimirRepr(P)(tbl)
      } yield {
        import repr.P.trans._

        readStatus match {
          case IdOnly =>
            repr.map(_.transform(constants.SourceKey.Single))

          case IncludeId =>
            val ids = constants.SourceKey.Single
            val values = constants.SourceValue.Single

            // note that ids are already an array
            repr.map(_.transform(InnerArrayConcat(ids, WrapArray(values))))

          case ExcludeId =>
            repr.map(_.transform(constants.SourceValue.Single))
        }
      }
    }
  }

  private def sourceTable(
      source: EvalSource[QueryAssociate[T, IO]],
      instructions: List[ParseInstruction])
      : F[P.Table] = {
    val queryResult =
      source.src match {
        case QueryAssociate.Lightweight(f) =>
          f(source.path)

        case QueryAssociate.Heavyweight(f) =>
          val shiftedRead =
            dsl.LeftShift(
              refineType(source.path.toPath)
                .fold(dsl.Read(_), dsl.Read(_)),
              recFunc.Hole,
              ExcludeId,
              ShiftType.Map,
              OnUndefined.Omit,
              func.RightSide)

          f(shiftedRead)
      }

    queryResult.to[F].flatMap(tableFromStream(_, instructions))
  }

  // we do not preserve the order of shifted results
  private def tableFromStream(
      rvalues: Stream[IO, RValue],
      instructions: List[ParseInstruction])
      : F[P.Table] = {

    val interpretedRValues: Stream[IO, RValue] =
      instructions match {
        case Nil => rvalues
        case instrs =>
          rvalues mapChunks { chunk =>
            val buf = ArrayBuffer.empty[RValue]
            chunk.foreach(rv => buf ++= RValueParseInstructionInterpreter.interpret(instrs, rv))
            Chunk.buffer(buf)
          }
      }

    P.Table.fromQDataStream[F, RValue](interpretedRValues) map { table =>
      import P.trans._

      // TODO depending on the id status we may not need to wrap the table
      // Also, we will need to handle this using ParseInstruction
      table.transform(OuterObjectConcat(
        WrapObject(
          Scan(Leaf(Source), P.freshIdScanner),
          TransSpecModule.paths.Key.name),
        WrapObject(
          Leaf(Source),
          TransSpecModule.paths.Value.name)))
    }
  }
}
