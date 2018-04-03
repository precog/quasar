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

import slamdata.Predef._
import quasar.common._
import quasar.contrib.pathy._
import quasar.fp.liftMT
import quasar.fs._
import quasar.regression._
import quasar.sql.Sql

import matryoshka.data.Fix
import pathy.Path.dir
import scalaz._, Scalaz._
import scalaz.stream.Process

class ResultFileQueryRegressionSpec
  extends QueryRegressionTest[BackendEffectIO](
    QueryRegressionTest.externalFS.map(_.filter(fs =>
      fs.ref.supports(BackendCapability.query()) &&
      fs.ref.supports(BackendCapability.write()) &&
      // NB: These are prohibitively slow on Couchbase
      !TestConfig.isCouchbase(fs.ref)))
  ) {

  val read = ReadFile.Ops[BackendEffectIO]

  val TestsDir = dir("resultFile").some

  val suiteName = "ResultFile Queries"

  def queryResults(expr: Fix[Sql], vars: Variables, basePath: ADir) = {
    import qfTransforms._

    type M[A] = FileSystemErrT[F, A]

    val hoistM: M ~> CompExecM =
      execToCompExec compose[M] Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

    for {
      tmpFile <- hoistM(manage.tempFile(DataDir, None)).liftM[Process]
      _       <- fsQ.executeQuery(expr, vars, basePath, tmpFile).liftM[Process]
      cleanup =  hoistM(
                   query.fileExists(tmpFile).liftM[FileSystemErrT].ifM(
                     manage.delete(tmpFile),
                     ().point[M]))
      data    <- read.scanAll(tmpFile)
                   .translate(hoistM)
                   .onComplete(Process.eval_(cleanup))
    } yield data
  }
}
