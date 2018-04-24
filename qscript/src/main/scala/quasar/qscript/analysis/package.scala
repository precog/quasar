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

package quasar.qscript

import slamdata.Predef._

import quasar.contrib.pathy.{AFile, APath, FPath}
import quasar.Data
import quasar.Data._int
import quasar.fs.{FileSystemError, FileSystemErrT, QueryFile}
import quasar.fs.PathError.invalidPath
import quasar.frontend.logicalplan.{Read => LPRead, LogicalPlan}
import quasar.std.StdLib.agg

import matryoshka.{Hole => _, _}
import matryoshka.data._
import pathy.Path.{file, peel, file1}
import scalaz._, Scalaz._

package object analysis {
  def pathCard[S[_]](
    implicit queryOps: QueryFile.Ops[S]
  ): APath => FileSystemErrT[Free[S, ?], Int] = (apath: APath) => {
    val afile: Option[AFile] = peel(apath).map {
      case (dirPath, \/-(fileName)) => dirPath </> file1(fileName)
      case (dirPath, -\/(dirName))  => dirPath </> file(dirName.value)
    }

    def lpFromPath[LP](p: FPath)(implicit LP: Corecursive.Aux[LP, LogicalPlan]): LP =
      LP.embed(agg.Count(LP.embed(LPRead(p))))

    val lp: FileSystemErrT[Free[S, ?], Fix[LogicalPlan]] =
      EitherT.fromDisjunction(afile.map(p => lpFromPath[Fix[LogicalPlan]](p)) \/> FileSystemError.pathErr(invalidPath(apath, "Cardinality unsupported")))
    val dataFromLp: Fix[LogicalPlan] => FileSystemErrT[Free[S, ?], Option[Data]] =
      (lp: Fix[LogicalPlan]) => queryOps.first(lp).mapT(_.value)

    (lp >>= (dataFromLp)) map (_.flatMap(d => _int.getOption(d).map(_.toInt)) | 0)
  }
}
