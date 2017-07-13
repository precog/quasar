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

package quasar.main.query

import slamdata.Predef._
import quasar.qscript.analysis._
import quasar.fs.QueryFile
import quasar.fp.ski._

import argonaut._, Argonaut._
import matryoshka.{Hole => _, _}
import scalaz._

package object analysis {

  sealed trait AnalysisResult
  case object Instant extends AnalysisResult
  case object Interactive extends AnalysisResult
  case object SemiInteractive extends AnalysisResult
  case object Batch extends AnalysisResult
  case object Unknown extends AnalysisResult

  implicit val encode: EncodeJson[AnalysisResult] =
    EncodeJson((r: AnalysisResult) => Json(("value" := s"$r")))

  /* TODO this needs empirical evaluation */
  private def costToResult: Int => AnalysisResult = {
    case i if i < 100  => Instant
    case i if i < 1000 => Interactive
    case i if i < 5000 => SemiInteractive
    case i             => Batch
  }

  def analyze[S[_], F[_] : Traverse, T](qs: T)(implicit
    R: Recursive.Aux[T, F],
    CA: Cardinality[F],
    CO: Cost[F],
    Q: QueryFile.Ops[S]
  ): Free[S, AnalysisResult] = {
    val resultErr = R.zygoM(qs)(CA.calculate(pathCard[S]), CO.evaluate(pathCard[S])).map(costToResult)
    resultErr.fold(κ(Unknown),ι)
  }
  
}
