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

package quasar
package physical.marklogic

import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.writerT._
import quasar.ejson.EJson
import quasar.fp.numeric._
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._

import matryoshka._
import iotaz.CopK

/** Internal configuration used in the MarkLogic BackendModule.
  *
  * NB: This is in a separate file in order to break up the compilation units.
  */
private[marklogic] sealed abstract class MLBackendConfig {
  type M[A] = MLFSQ[A]
  type QSM[T[_[_]], A] = CopK[MLQScriptCP[T], A]

  type FMT

  val cfg: MarkLogicConfig
  implicit def planner[T[_[_]]: BirecursiveT]: Planner[M, FMT, QSM[T, ?], T[EJson]]
  implicit def structuralPlanner: StructuralPlanner[XccEval, FMT]
  implicit def searchOptions: SearchOptions[FMT]
  implicit def dataAsContent: AsContent[FMT, Data]

  implicit def structuralPlannerM: StructuralPlanner[M, FMT] =
    structuralPlanner transform xccEvalToMLFSQ
}

private[marklogic] object MLBackendConfig {
  final class JsonConfig(val cfg: MarkLogicConfig) extends MLBackendConfig {
    type FMT = DocType.Json
    def planner[T[_[_]]: BirecursiveT] = Planner[M, FMT, QSM[T, ?], T[EJson]]
    val structuralPlanner = StructuralPlanner.jsonStructuralPlanner[XccEval]
    val searchOptions = SearchOptions[FMT]
    val dataAsContent = AsContent[FMT, Data]
  }

  final class XmlConfig(val cfg: MarkLogicConfig) extends MLBackendConfig {
    type FMT = DocType.Xml
    def planner[T[_[_]]: BirecursiveT] = Planner[M, FMT, QSM[T, ?], T[EJson]]
    val structuralPlanner = StructuralPlanner.xmlStructuralPlanner[XccEval]
    val searchOptions = SearchOptions[FMT]
    val dataAsContent = AsContent[FMT, Data]
  }

  def fromMarkLogicConfig(cfg: MarkLogicConfig): MLBackendConfig =
    cfg.docType.fold[MLBackendConfig](json = new JsonConfig(cfg), xml = new XmlConfig(cfg))
}
