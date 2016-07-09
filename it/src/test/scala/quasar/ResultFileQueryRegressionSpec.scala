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

package quasar

import quasar.fp.liftMT
import quasar.fs._
import quasar.regression._
import quasar.sql.Sql

import matryoshka.Fix
import scalaz._, Scalaz._
import scalaz.stream.Process

class ResultFileQueryRegressionSpec
  extends QueryRegressionTest[FileSystemIO](
    QueryRegressionTest.externalFS.map(_.filterNot(fs => TestConfig.isMongoReadOnly(fs.name)))) {

  val read = ReadFile.Ops[FileSystemIO]

  val suiteName = "ResultFile Queries"

  def queryResults(expr: Fix[Sql], vars: Variables, basePath: ADir) = {
    import qfTransforms._

    type M[A] = FileSystemErrT[F, A]

    val hoistM: M ~> CompExecM =
      execToCompExec compose[M] Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

    for {
      tmpFile <- hoistM(manage.tempFile(DataDir)).liftM[Process]
      outFile <- fsQ.executeQuery(expr, vars, basePath, tmpFile).liftM[Process]
      cleanup =  hoistM(manage.delete(tmpFile)).whenM(outFile ≟ tmpFile)
      data    <- read.scanAll(outFile)
                   .translate(hoistM)
                   .onComplete(Process.eval_(cleanup))
    } yield data
  }
}
