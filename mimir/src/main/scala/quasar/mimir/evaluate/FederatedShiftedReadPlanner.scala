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

import quasar.Data
import quasar.api._, ResourceError._
import quasar.blueeyes.json.JValue
import quasar.contrib.fs2.convert
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.evaluate.{Source => EvalSource}
import quasar.fs.PathError
import quasar.fs.Planner.{InternalError, PlannerErrorME, PlanPathError}
import quasar.mimir._, MimirCake._
import quasar.precog.common.RValue
import quasar.qscript._
import quasar.yggdrasil.TransSpecModule.paths.{Key, Value}
import quasar.yggdrasil.UnknownSize
import quasar.yggdrasil.table.Slice

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{IO, LiftIO}
import fs2.Stream
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

final class FederatedShiftedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: LiftIO: Monad: PlannerErrorME: MonadFinalizers[?[_], IO]](
    val P: Cake) {

  type Assocs = Associates[T, F, IO]
  type M[A] = AssociatesT[T, F, IO, A]

  val plan: AlgebraM[M, Const[ShiftedRead[AFile], ?], MimirRepr] = {
    case Const(ShiftedRead(file, status)) =>
      Kleisli.ask[F, Assocs].map(_(file)) andThenK { maybeSource =>
        for {
          source <- PlannerErrorME[F].unattempt_(
            maybeSource \/> InternalError.fromMsg(s"No source for '${posixCodec.printPath(file)}'."))

          tbl <- sourceTable(source)

          repr = MimirRepr(P)(tbl)
        } yield {
          import repr.P.trans._

          status match {
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

  ////

  private val dsl = construction.mkGeneric[T, QScriptRead[T, ?]]
  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private def sourceTable(source: EvalSource[QueryAssociate[T, F, IO]]): F[P.Table] = {
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

    queryResult.flatMap(_.fold(handleReadError[P.Table], tableFromStream))
  }

  private def tableFromStream(s: Stream[IO, Data]): F[P.Table] = {
    val dataToRValue: Data => RValue =
      d => RValue.unsafeFromJValueRaw(JValue.fromData(d))

    // TODO{fs2}: Chunkiness
    val sliceStream =
      s.mapChunks(_.map(dataToRValue).toSegment)
        .segments
        .map(seg => Slice.fromRValues(seg.force.toList.toStream))

    for {
      d <- convert.toStreamT(sliceStream).to[F]

      _ <- MonadFinalizers[F, IO].tell(List(d.dispose))

      slices = d.unsafeValue
    } yield {

      import P.trans._

      // TODO depending on the id status we may not need to wrap the table
      P.Table(slices, UnknownSize)
        .transform(OuterObjectConcat(
          WrapObject(
            Scan(Leaf(Source), P.freshIdScanner),
            Key.name),
          WrapObject(
            Leaf(Source),
            Value.name)))
    }
  }

  private def handleReadError[A](rerr: ReadError): F[A] =
    PlannerErrorME[F].raiseError[A](PlanPathError(rerr match {
      case NotAResource(rp) =>
        PathError.invalidPath(rp.toPath, "does not refer to a resource.")

      case PathNotFound(rp) =>
        PathError.pathNotFound(rp.toPath)
    }))
}
