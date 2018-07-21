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

import quasar.common.data.Data
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadTell_
import quasar.evaluate.{Source => EvalSource}
import quasar.mimir._, MimirCake._
import quasar.precog.common.RValue
import quasar.qscript._, PlannerError.InternalError
import quasar.yggdrasil.TransSpecModule

import cats.effect.{IO, LiftIO}
import fs2.Stream
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

final class FederatedShiftedReadPlanner[
    T[_[_]]: BirecursiveT: EqualT: ShowT,
    F[_]: LiftIO: Monad: MonadPlannerErr: MonadTell_[?[_], List[IO[Unit]]]](
    val P: Cake) {

  type Assocs = Associates[T, IO]
  type M[A] = AssociatesT[T, F, IO, A]

  val plan: AlgebraM[M, Const[ShiftedRead[AFile], ?], MimirRepr] = {
    case Const(ShiftedRead(file, status)) =>
      Kleisli.ask[F, Assocs].map(_(file)) andThenK { maybeSource =>
        for {
          source <- MonadPlannerErr[F].unattempt_(
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

  private def sourceTable(source: EvalSource[QueryAssociate[T, IO]]): F[P.Table] = {
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

    queryResult.to[F].flatMap(tableFromStream)
  }

  private def tableFromStream(s: Stream[IO, Data]): F[P.Table] = {
    val dataToRValue: Data => RValue =
      d => RValue.fromData(d).getOrElse(sys.error("There is no representation of CUndefined in SlamDB as a value"))

    // TODO{fs2}: Chunkiness
    P.Table.fromRValueStream[F](s.mapChunks(_.map(dataToRValue).toSegment)) map { table =>

      import P.trans._

      // TODO depending on the id status we may not need to wrap the table
      table
        .transform(OuterObjectConcat(
          WrapObject(
            Scan(Leaf(Source), P.freshIdScanner),
            TransSpecModule.paths.Key.name),
          WrapObject(
            Leaf(Source),
            TransSpecModule.paths.Value.name)))
    }
  }
}
