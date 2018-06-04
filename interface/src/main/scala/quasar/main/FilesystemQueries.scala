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

package quasar.main

import slamdata.Predef._
import quasar.{Data, Variables}
import quasar.compile.queryPlan
import quasar.contrib.pathy._
import quasar.fp.numeric._
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.fs._
import quasar.sql.Sql

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.stream.{Process0, Process}

class FilesystemQueries[S[_]](implicit val Q: QueryFile.Ops[S]) {
  val CE = CompExec[Q.FreeS]
  import CE._

  /** Returns the source of values from the result of executing the given
    * SQL^2 query.
    */
  def evaluateQuery(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir,
    off: Natural,
    lim: Option[Positive]):
      Process[CompExecM, Data] =
    queryPlan[CompExecM, Fix, Fix[LP]](query, vars, basePath, off, lim)
      .liftM[Process]
      .flatMap(Q.evaluate(_).translate[CompExecM](execToCompExec))

  /** Returns the path to the result of executing the given SQL^2 query
    * using the given output file.
    */
  def executeQuery(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir,
    out: AFile)(
    implicit W: WriteFile.Ops[S], MF: ManageFile.Ops[S]):
      CompExecM[Unit] =
    queryPlan[CompExecM, Fix, Fix[LP]](query, vars, basePath, 0L, None)
      .flatMap(lp => execToCompExec(Q.execute(lp, out)))

  /** Returns the physical execution plan for the given SQL^2 query. */
  def explainQuery(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir
  ): CompExecM[ExecutionPlan] =
    queryPlan[CompExecM, Fix, Fix[LP]](query, vars, basePath, 0L, None)
      .flatMap(lp => execToCompExec(Q.explain(lp)))

  /** The results of executing the given SQL^2 query. */
  def queryResults(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir,
    off: Natural,
    lim: Option[Positive]
  ): CompExecM[Process0[Data]] =
    queryPlan[CompExecM, Fix, Fix[LP]](query, vars, basePath, off, lim)
      .flatMap(lp => execToCompExec(Q.results(lp)))
}
