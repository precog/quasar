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

package quasar.main

import slamdata.Predef._
import quasar.{Data, queryPlan, Variables}
import quasar.fp.ski.κ
import quasar.contrib.pathy._
import quasar.fp.numeric._
import quasar.fs._
import quasar.sql.Sql

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.stream.{Process0, Process}

class FilesystemQueries[S[_]](implicit val Q: QueryFile.Ops[S]) {
  import Q.transforms._

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

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
    queryPlan(query, vars, basePath, off, lim).sequenceU.fold(
      Process(_: _*),
      compToCompExec(_)
        .liftM[Process]
        .flatMap(Q.evaluate(_).translate[CompExecM](execToCompExec)))

  /** Returns the path to the result of executing the given SQL^2 query
    * using the given output file if possible.
    */
  def executeQuery(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir,
    out: AFile)(
    implicit W: WriteFile.Ops[S], MF: ManageFile.Ops[S]):
      CompExecM[AFile] =
    compToCompExec(queryPlan(query, vars, basePath, 0L, None))
      .flatMap(lp => execToCompExec(lp.fold(
        d => fsErrToExec(W.saveThese(out, d.toVector).flatMap(fse => EitherT.fromDisjunction(fse.headOption <\/ out))),
        Q.execute(_, out))))

  /** Returns the physical execution plan for the given SQL^2 query. */
  def explainQuery(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir
  ): CompExecM[ExecutionPlan] =
    compToCompExec(queryPlan(query, vars, basePath, 0L, None))
      .flatMap(lp => execToCompExec(lp.fold(
        κ(ExecutionPlan(FileSystemType("none"), "Constant", ISet.empty).point[ExecM]),
        Q.explain(_))))

  /** The results of executing the given SQL^2 query. */
  def queryResults(
    query: Fix[Sql],
    vars: Variables,
    basePath: ADir,
    off: Natural,
    lim: Option[Positive]
  ): CompExecM[Process0[Data]] =
    compToCompExec(queryPlan(query, vars, basePath, off, lim))
      .flatMap(_.fold(
        ds => Process.emitAll(ds).point[CompExecM],
        lp => execToCompExec(Q.results(lp))))
}
