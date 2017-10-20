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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar._, Planner._, Type.{Const => _, _}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs.{FileSystemError, MonadFsErr}, FileSystemError.qscriptPlanningFailed
import quasar.physical.mongodb.planner.common._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz.{ToIdOps => _, _}

object assumeReadType {

  object FreeShiftedRead {
    def unapply[T[_[_]]](fq: FreeQS[T])(
      implicit QSR: Const[ShiftedRead[AFile], ?] :<: QScriptTotal[T, ?]
    ) : Option[ShiftedRead[AFile]] = fq match {
      case Embed(CoEnv(\/-(QSR(qsr: Const[ShiftedRead[AFile], _])))) => qsr.getConst.some
      case _ => none
    }
  }

  object HasShiftedRead {
    def unapply[T[_[_]]: BirecursiveT: EqualT, F[_]: Functor, A](qs: F[A])(implicit
          QC: QScriptCore[T, ?] :<: F,
          SR: Const[ShiftedRead[AFile], ?] :<: F,
          ev: Recursive.Aux[A, F])
        : Option[F[A]] = qs match {
      case QC(Filter(Embed(SR(Const(ShiftedRead(_, ExcludeId)))), _))
         | QC(Sort(Embed(SR(Const(ShiftedRead(_, ExcludeId)))), _, _))
         | QC(Sort(Embed(QC(Filter(Embed(SR(Const(ShiftedRead(_, ExcludeId)))), _))), _ , _))
         | QC(Subset(_, FreeShiftedRead(ShiftedRead(_, ExcludeId)), _ , _))
         | SR(Const(ShiftedRead(_, ExcludeId))) => qs.some
      case _ => none
    }
  }

  def elideMoreGeneralGuards[M[_]: Applicative: MonadFsErr, T[_[_]]: RecursiveT]
    (subType: Type)
      : CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
    def f: CoEnvMap[T, FreeMap[T]] => M[CoEnvMap[T, FreeMap[T]]] = {
      case free @ CoEnv(\/-(MFC(MapFuncsCore.Guard(Embed(CoEnv(-\/(SrcHole))), typ, cont, fb)))) =>
        if (typ.contains(subType)) cont.project.point[M]
        // TODO: Error if there is no overlap between the types.
        else {
          val union = subType ⨯ typ
          if (union ≟ Type.Bottom)
            raiseErr(qscriptPlanningFailed(InternalError.fromMsg(s"can only contain ${subType.shows}, but a(n) ${typ.shows} is expected")))
          else {
            CoEnv[Hole, MapFunc[T, ?], FreeMap[T]](MFC(MapFuncsCore.Guard[T, FreeMap[T]](HoleF[T], union, cont, fb)).right).point[M]
          }
        }
      case x => x.point[M]
    }
    f
  }

  // TODO: Allow backends to provide a “Read” type to the typechecker, which
  //       represents the type of values that can be stored in a collection.
  //       E.g., for MongoDB, it would be `Map(String, Top)`. This will help us
  //       generate more correct PatternGuards in the first place, rather than
  //       trying to strip out unnecessary ones after the fact
  // FIXME: This doesn’t yet traverse branches, so it leaves in some checks.
  def apply[M[_]: Monad: MonadFsErr, T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Functor]
    (typ: Type)
    (implicit
      QC: QScriptCore[T, ?] :<: F,
      SR: Const[ShiftedRead[AFile], ?] :<: F)
      : QScriptCore[T, T[F]] => M[F[T[F]]] = {
    case f @ Filter(src, cond) =>
      src.project match {
        case HasShiftedRead(_) =>
          ((MapFuncCore.flattenAnd(cond))
            .traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ))))
            .map(_.toList.filter {
              case MapFuncsCore.BoolLit(true) => false
              case _                      => true
            } match {
              case Nil    => src.project
              case h :: t => QC(Filter(src, t.foldLeft[FreeMap[T]](h)((acc, e) => Free.roll(MFC(MapFuncsCore.And(acc, e))))))
            })
        case _ => QC.inj(f).point[M]
      }
    case ls @ LeftShift(src, struct, id, repair) =>
      src.project match {
        case HasShiftedRead(_) =>
          struct.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
          (struct => QC.inj(LeftShift(src, struct, id, repair)))
        case _ => QC.inj(ls).point[M]
      }
    case m @ qscript.Map(src, mf) =>
      src.project match {
        case HasShiftedRead(_) =>
          mf.transCataM(elideMoreGeneralGuards[M, T](typ)) ∘
          (mf => QC.inj(qscript.Map(src, mf)))
        case _ => QC.inj(m).point[M]
      }
    case r @ Reduce(src, b, red, rep) =>
      src.project match {
        case HasShiftedRead(_) =>
          (b.traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ))) ⊛
            red.traverse(_.traverse(_.transCataM(elideMoreGeneralGuards[M, T](typ)))))(
            (b, red) => QC.inj(Reduce(src, b, red, rep)))
        case _ => QC.inj(r).point[M]
      }
    case qc =>
      QC.inj(qc).point[M]
  }
}
