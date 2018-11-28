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
import quasar.api.resource.ResourcePath
import quasar.common.data.RValue
import quasar.connector.{CompressionScheme, ParsableType, QueryResult, ResourceError}
import quasar.connector.ParsableType.JsonVariant
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.impl.evaluate.{Source => EvalSource}
import quasar.impl.parsing.TectonicResourceError
import quasar.mimir._, MimirCake._
import quasar.mimir.evaluate.Config.{Associates, EvalConfigT, EvaluatorConfig}
import quasar.qscript._, PlannerError.InternalError
import quasar.yggdrasil.MonadFinalizers

import cats.effect.{ContextShift, IO, LiftIO}
import fs2.{gzip, Chunk, Stream}
import matryoshka._
import pathy.Path._
import qdata.{QData, QDataDecode}
import scalaz._, Scalaz._
import shims._

import scala.concurrent.ExecutionContext
import scala.collection.mutable.ArrayBuffer

final class FederatedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: LiftIO: Monad: MonadPlannerErr: MonadFinalizers[?[_], IO]](
    val P: Cake)(
    implicit
    cs: ContextShift[IO],
    ec: ExecutionContext) {

  import FederatedReadPlanner.DefaultDecompressionBufferSize

  type Assocs = Associates[T, IO]
  type M[A] = EvalConfigT[T, F, IO, A]

  type FederatedRead[A] = Const[Read[AFile], A] \/ Const[InterpretedRead[AFile], A]

  def plan: AlgebraM[M, FederatedRead, MimirRepr] = {
    case -\/(Const(Read(file, status))) =>
      planRead(file, Nil) map { repr =>
        import repr.P.trans._

        repr map { table =>
          status match {
            case IdOnly =>
              table.transform(Scan(Leaf(Source), P.freshIdScanner))

            case IncludeId =>
              table.transform(InnerArrayConcat(
                WrapArray(Scan(Leaf(Source), P.freshIdScanner)),
                WrapArray(Leaf(Source))))

            case ExcludeId =>
              table
          }
        }
      }

    case \/-(Const(InterpretedRead(file, instructions))) =>
      planRead(file, instructions)
  }

  ////

  private val dsl = construction.mkGeneric[T, QScriptEducated[T, ?]]

  private def planRead(file: AFile, instructions: List[ParseInstruction])
      : M[MimirRepr] = {

    val sourceM: M[Option[EvalSource[QueryAssociate[T, IO]]]] =
      Kleisli.ask[F, EvaluatorConfig[T, IO]].map(_.associates(file))

    sourceM andThenK { maybeSource =>
      for {
        source <- MonadPlannerErr[F].unattempt_(
          maybeSource \/> InternalError.fromMsg(s"No source for '${posixCodec.printPath(file)}'."))

        tbl <- sourceTable(file, source, instructions)

      } yield MimirRepr(P)(tbl)
    }
  }

  private def sourceTable(
      path: AFile,
      source: EvalSource[QueryAssociate[T, IO]],
      instructions: List[ParseInstruction])
      : F[P.Table] =
    for {
      queryResult <- source.src match {
        case QueryAssociate.Lightweight(f) =>
          f(source.path).to[F]

        case QueryAssociate.Heavyweight(f) =>
          val read = dsl.Read(ResourcePath.leaf(path), ExcludeId)

          f(read).to[F]
      }

      table <- tableFromQueryResult(path, queryResult, instructions)
    } yield table

  @tailrec
  private def tableFromQueryResult(
      path: AFile,
      qr: QueryResult[IO],
      instructions: List[ParseInstruction])
      : F[P.Table] =
    qr match {
      case QueryResult.Parsed(qd, data) =>
        tableFromParsed(data, instructions)(qd)

      case QueryResult.Compressed(CompressionScheme.Gzip, content) =>
        tableFromQueryResult(
          path,
          content.modifyBytes(gzip.decompress[IO](DefaultDecompressionBufferSize)),
          instructions)

      case QueryResult.Typed(tpe, data) =>
        tableFromTyped(path, tpe, data, instructions)
    }

  // we do not preserve the order of shifted results
  private def tableFromParsed[A: QDataDecode](
      data: Stream[IO, A],
      instructions: List[ParseInstruction])
      : F[P.Table] = {

    val interpretedRValues: Stream[IO, RValue] =
      if (instructions.isEmpty)
        data.map(QData.convert[A, RValue] _)
      else
        data mapChunks { chunk =>
          val buf = ArrayBuffer.empty[RValue]

          chunk foreach { a =>
            val rvalues =
              RValueParseInstructionInterpreter.interpret(
                instructions,
                QData.convert[A, RValue](a))

            buf ++= rvalues
          }

          Chunk.buffer(buf)
        }

    P.Table.fromQDataStream[F, RValue](interpretedRValues)
  }

  private def tableFromTyped(
      path: AFile,
      tpe: ParsableType,
      data: Stream[IO, Byte],
      instructions: List[ParseInstruction])
      : F[P.Table] =
    tpe match {
      case ParsableType.Json(vnt, isPrecise) =>
        val isArrayWrapped = vnt === JsonVariant.ArrayWrapped
        P.Table.parseJson[F](data, instructions, isPrecise, isArrayWrapped) map { tbl =>
          val handledSlices = tbl.slices.trans(λ[IO ~> IO](_ handleErrorWith { t =>
            IO.raiseError(
              TectonicResourceError(ResourcePath.leaf(path), tpe, t)
                .fold(t)(ResourceError.throwableP(_)))
          }))

          P.Table(handledSlices, tbl.size)
        }
    }
}

object FederatedReadPlanner {
  // 32k buffer, anything less would be uncivilized.
  val DefaultDecompressionBufferSize: Int = 32768
}
