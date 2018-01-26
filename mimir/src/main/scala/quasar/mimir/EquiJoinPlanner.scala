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

import slamdata.Predef._

import quasar._
import quasar.common._
import quasar.fp.ski.κ
import quasar.mimir.MimirCake._
import quasar.qscript._

import quasar.yggdrasil.TableModule.SortAscending

import fs2.interop.scalaz._

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._

import org.slf4s.Logging

import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class EquiJoinPlanner[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad](
    liftF: Task ~> F) extends Logging {

  def mapFuncPlanner[G[_]: Monad] = MapFuncPlanner[T, G, MapFunc[T, ?]]

  def plan(planQST: AlgebraM[F, QScriptTotal[T, ?], MimirRepr])
      : AlgebraM[F, EquiJoin[T, ?], MimirRepr] = {
    case qscript.EquiJoin(src, lbranch, rbranch, keys, tpe, combine) =>
      import src.P.trans._, scalaz.syntax.std.option._

      def rephrase2(projection: TransSpec2, rootL: TransSpec1, rootR: TransSpec1): Option[SortOrdering[TransSpec1]] = {
        val leftRephrase = TransSpec.rephrase(projection, SourceLeft, rootL).fold(Set.empty[TransSpec1])(Set(_))
        val rightRephrase = TransSpec.rephrase(projection, SourceRight, rootR).fold(Set.empty[TransSpec1])(Set(_))
        val bothRephrased = leftRephrase ++ rightRephrase

        if (bothRephrased.isEmpty)
          None
        else
          SortOrdering(bothRephrased, SortAscending, unique = false).some
      }

      val (lkeys, rkeys) = keys.unfzip

      for {
        leftRepr <- lbranch.cataM(interpretM(κ(src.point[F]), planQST))
        rightRepr <- rbranch.cataM(interpretM(κ(src.point[F]), planQST))

        lmerged = src.unsafeMerge(leftRepr)
        ltable = lmerged.table

        rmerged = src.unsafeMerge(rightRepr)
        rtable = rmerged.table

        transLKeys <- lkeys traverse interpretMapFunc[T, F](src.P, mapFuncPlanner[F])
        transLKey = combineTransSpecs(src.P)(transLKeys)
        transRKeys <- rkeys traverse interpretMapFunc[T, F](src.P, mapFuncPlanner[F])
        transRKey = combineTransSpecs(src.P)(transRKeys)

        transMiddle <- combine.cataM[F, TransSpec2](
          interpretM(
            {
              case qscript.LeftSide => TransSpec2.LeftId.point[F]
              case qscript.RightSide => TransSpec2.RightId.point[F]
            },
            mapFuncPlanner[F].plan(src.P)[Source2](TransSpec2.LeftId)
          )
        ) // TODO weirdly left-biases things like constants

        // identify full-cross and avoid cogroup
        resultAndSort <- {
          if (keys.isEmpty) {
            log.trace("EQUIJOIN: full-cross detected!")

            (ltable.cross(rtable)(transMiddle), src.unsafeMerge(leftRepr).lastSort).point[F]
          } else {
            log.trace("EQUIJOIN: not a full-cross; sorting and cogrouping")

            for {
              lsorted <- liftF(sortT[leftRepr.P.type](MimirRepr.single[leftRepr.P](leftRepr))(leftRepr.table, leftRepr.mergeTS1(transLKey)))
                .map(r => src.unsafeMergeTable(r.table))
              rsorted <- liftF(sortT[rightRepr.P.type](MimirRepr.single[rightRepr.P](rightRepr))(rightRepr.table, rightRepr.mergeTS1(transRKey)))
                .map(r => src.unsafeMergeTable(r.table))

              transLeft <- tpe match {
                case JoinType.LeftOuter | JoinType.FullOuter =>
                  combine.cataM[F, TransSpec1](
                    interpretM(
                      {
                        case qscript.LeftSide => TransSpec1.Id.point[F]
                        case qscript.RightSide => TransSpec1.Undef.point[F]
                      },
                      mapFuncPlanner[F].plan(src.P)[Source1](TransSpec1.Id)
                    )
                  )

                case JoinType.Inner | JoinType.RightOuter =>
                  TransSpec1.Undef.point[F]
              }

              transRight <- tpe match {
                case JoinType.RightOuter | JoinType.FullOuter =>
                  combine.cataM[F, TransSpec1](
                    interpretM(
                      {
                        case qscript.LeftSide => TransSpec1.Undef.point[F]
                        case qscript.RightSide => TransSpec1.Id.point[F]
                      },
                      mapFuncPlanner[F].plan(src.P)[Source1](TransSpec1.Id)
                    )
                  )

                case JoinType.Inner | JoinType.LeftOuter =>
                  TransSpec1.Undef.point[F]
              }
              newSortOrder = rephrase2(transMiddle, transLKey, transRKey)
            } yield (lsorted.cogroup(transLKey, transRKey, rsorted)(transLeft, transRight, transMiddle),
                newSortOrder.map(order => SortState(None, order :: Nil)))
          }
        }

        (result, newSort) = resultAndSort
      } yield MimirRepr.withSort(src.P)(result)(newSort)
  }
}
