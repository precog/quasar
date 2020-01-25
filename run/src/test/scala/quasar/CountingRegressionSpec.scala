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

package quasar.run

import slamdata.Predef.{Map => SMap, _}
import quasar._
import quasar.api.QueryEvaluator
import quasar.common.{PhaseResults, PhaseResultTell}
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.qscript._
import quasar.sql.Query

import cats.effect.IO
import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import matryoshka.data._
import pathy.Path

import shims.monadToScalaz

abstract class CountingRegressionSpec extends Qspec {

  def countingEvaluator: QueryEvaluator[IO, Fix[QScriptEducated[Fix, ?]], QScriptCount]

  implicit val ioQuasarError: MonadQuasarErr[IO] =
    MonadError_.facet[IO](QuasarError.throwableP)

  implicit val ioPlannerError: MonadPlannerErr[IO] =
    MonadError_.facet[IO](QuasarError.planning)

  implicit val ioMonadTell: PhaseResultTell[IO] =
    MonadTell_.ignore[IO, PhaseResults]

  lazy val sql2Evaluator: QueryEvaluator[IO, SqlQuery, QScriptCount] =
    Sql2QueryEvaluator[Fix, IO, QScriptCount](countingEvaluator)

  def query(q: String): SqlQuery =
    SqlQuery(Query(q), Variables(SMap()), Path.rootDir)

  def count(sql2: String): QScriptCount =
    sql2Evaluator.evaluate(query(sql2)).unsafeRunSync()

  def countInterpretedReadAs(expected: Int): Matcher[QScriptCount] =
    new Matcher[QScriptCount] {
      def apply[S <: QScriptCount](s: Expectable[S]): MatchResult[S] = {
        val actual = s.value.interpretedRead.count
        result(
          actual == expected,
          s"Received expected ${s.value}",
          s"Received unexpected ${s.value}",
          s,
          expected.toString,
          actual.toString)
      }
    }

  def countReadAs(expected: Int): Matcher[QScriptCount] =
    new Matcher[QScriptCount] {
      def apply[S <: QScriptCount](s: Expectable[S]): MatchResult[S] = {
        val actual = s.value.read.count
        result(
          actual == expected,
          s"Received expected ${s.value}",
          s"Received unexpected ${s.value}",
          s,
          expected.toString,
          actual.toString)
      }
    }

  def countLeftShiftAs(expected: Int): Matcher[QScriptCount] =
    new Matcher[QScriptCount] {
      def apply[S <: QScriptCount](s: Expectable[S]): MatchResult[S] = {
        val actual = s.value.leftShift.count
        result(
          actual == expected,
          s"Received expected ${s.value}",
          s"Received unexpected ${s.value}",
          s,
          expected.toString,
          actual.toString)
      }
    }
}
