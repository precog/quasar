/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.{Common, EJson, Str}
import quasar.fp.coproductShow
import quasar.fp.ski.κ
import quasar.contrib.scalaz.MonadError_
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

package object qscript {
  type MarkLogicPlanErrT[F[_], A] = EitherT[F, MarkLogicPlannerError, A]

  type MonadPlanErr[F[_]] = MonadError_[F, MarkLogicPlannerError]

  object MonadPlanErr {
    def apply[F[_]](implicit F: MonadPlanErr[F]): MonadPlanErr[F] = F
  }

  /** Matches "iterative" FLWOR expressions, those involving at least one `for` clause. */
  object IterativeFlwor {
    def unapply(xqy: XQuery): Option[(NonEmptyList[BindingClause], Option[XQuery], IList[(XQuery, SortDirection)], Boolean, XQuery)] = xqy match {
      case XQuery.Flwor(clauses, filter, order, isStable, result) if clauses.any(BindingClause.forClause.isMatching) =>
        Some((clauses, filter, order, isStable, result))

      case _ => None
    }
  }

  val EJsonTypeKey  = "_ejson.type"
  val EJsonValueKey = "_ejson.value"

  /** Converts the given string to a QName if valid, failing with an error otherwise. */
  def asQName[F[_]: MonadPlanErr: Applicative](s: String): F[QName] =
    (QName.string.getOption(s) orElse QName.string.getOption(encodeForQName(s)))
      .fold(invalidQName[F, QName](s))(_.point[F])

  def mapFuncXQuery[T[_[_]]: BirecursiveT, F[_]: Monad: MonadPlanErr, FMT](
    fm: FreeMap[T],
    src: XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]],
    SP:  StructuralPlanner[F, FMT]
  ): F[XQuery] =
    fm.project match {
      case MapFunc.StaticArray(elements) =>
        for {
          xqyElts <- elements.traverse(planMapFunc[T, F, FMT, Hole](_)(κ(src)))
          arrElts <- xqyElts.traverse(SP.mkArrayElt)
          arr     <- SP.mkArray(mkSeq(arrElts))
        } yield arr

      case MapFunc.StaticMap(entries) =>
        for {
          xqyKV <- entries.traverse(_.bitraverse({
                     case Embed(Common(Str(s))) => s.xs.point[F]
                     case key                   => invalidQName[F, XQuery](key.convertTo[Fix[EJson]].shows)
                   },
                   planMapFunc[T, F, FMT, Hole](_)(κ(src))))
          elts  <- xqyKV.traverse((SP.mkObjectEntry _).tupled)
          map   <- SP.mkObject(mkSeq(elts))
        } yield map

      case other => planMapFunc[T, F, FMT, Hole](other.embed)(κ(src))
    }

  def mergeXQuery[T[_[_]]: RecursiveT, F[_]: Monad, FMT](
    jf: JoinFunc[T],
    l: XQuery,
    r: XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    planMapFunc[T, F, FMT, JoinSide](jf) {
      case LeftSide  => l
      case RightSide => r
    }

  def planMapFunc[T[_[_]]: RecursiveT, F[_]: Monad, FMT, A](
    freeMap: FreeMapA[T, A])(
    recover: A => XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    freeMap.cataM(interpretM(recover(_).point[F], MFP.plan))

  def rebaseXQuery[T[_[_]], F[_]: Monad, FMT](
    fqs: FreeQS[T],
    src: XQuery
  )(implicit
    QTP: Planner[F, FMT, QScriptTotal[T, ?]]
  ): F[XQuery] =
    fqs.cataM(interpretM(κ(src.point[F]), QTP.plan))

  ////

  // A string consisting only of digits.
  private val IntegralNumber = "^(\\d+)$".r

  // Applies transformations to string to make them valid QNames
  private val encodeForQName: String => String = {
    case IntegralNumber(n) => "_" + n
    case other             => other
  }

  private def invalidQName[F[_]: MonadPlanErr, A](s: String): F[A] =
    MonadError_[F, MarkLogicPlannerError].raiseError(
      MarkLogicPlannerError.invalidQName(s))
}
