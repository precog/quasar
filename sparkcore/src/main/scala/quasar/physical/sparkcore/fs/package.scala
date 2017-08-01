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

package quasar.physical.sparkcore

import slamdata.Predef._
import quasar.contrib.scalaz.eitherT._
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._, BackendDef._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.pathy._
import quasar.qscript._

import org.apache.spark._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task


package object fs {

  type SparkQScriptCP = QScriptCore[Fix, ?] :\: EquiJoin[Fix, ?] :/: Const[ShiftedRead[AFile], ?]
  type SparkQScript[A] = SparkQScriptCP#M[A]

  implicit val sparkQScriptToQSTotal: Injectable.Aux[SparkQScript, QScriptTotal[Fix, ?]] =
    ::\::[QScriptCore[Fix, ?]](::/::[Fix, EquiJoin[Fix, ?], Const[ShiftedRead[AFile], ?]])

  type SparkQScript0[A] = (Const[ShiftedRead[ADir], ?] :/: SparkQScript)#M[A]


  final case class SparkFSDef[HS[_], S[_]](run: Free[HS, ?] ~> Free[S, ?], close: Free[S, Unit])

  val genSc: SparkConf => EitherT[Task, String, SparkContext] = (sparkConf: SparkConf) =>
  EitherT(Task.delay {
    val sc = new SparkContext(sparkConf)
    sc.right[String]
  }.handleWith {
    case ex : SparkException if ex.getMessage.contains("SPARK-2243") =>
      "You can not mount second Spark based connector... Please unmount existing one first.".left[SparkContext].point[Task]
  })

  def toQScript[M[_] : Monad](
    listContents: ADir => M[FileSystemError \/ Set[PathSegment]]
  )(lp: Fix[LogicalPlan]): FileSystemErrT[PhaseResultT[M, ?], Fix[SparkQScript]] = {
    type F[A] = FileSystemErrT[PhaseResultT[M, ?], A]

    val lc: DiscoverPath.ListContents[F] =
      (adir: ADir) => EitherT(listContents(adir).liftM[PhaseResultT])
    val rewrite = new Rewrite[Fix]
    val optimize = new Optimize[Fix]
    for {
      qs <- QueryFile.convertToQScriptRead[Fix, F, QScriptRead[Fix, ?]](lc)(lp)
      shifted <- Unirewrite[Fix, SparkQScriptCP, F](rewrite, lc).apply(qs)

      optQS = shifted.transHylo(
        optimize.optimize(reflNT[SparkQScript]),
        Unicoalesce[Fix, SparkQScriptCP])

      _     <- EitherT(WriterT[M, PhaseResults, FileSystemError \/ Unit]((Vector(PhaseResult.tree("QScript (Spark)", optQS)), ().right[FileSystemError]).point[M]))
    } yield optQS
  }

  def definition[HS[_],S[_], T](
    fsType: FileSystemType,
    parseUri: ConnectionUri => DefinitionError \/ (SparkConf, T),
    sparkFsDef: SparkConf => Free[S, SparkFSDef[HS, S]],
    fsInterpret: T => (FileSystem ~> Free[HS, ?])
  )(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): BackendDef[Free[S, ?]] = {

    BackendDef.fromPF {
      case (`fsType`, uri) =>
        for {
          config <- EitherT(parseUri(uri).point[Free[S, ?]])
          (sparkConf, t) = config
          res <- {
            sparkFsDef(sparkConf).map {
              case SparkFSDef(run, close) =>
                val fileSystemInterpreter = fsInterpret(t)
                val queryFileIntereter   = fileSystemInterpreter compose Inject[QueryFile, FileSystem]
                val listContents: ADir => Free[QueryFile, FileSystemError \/ Set[PathSegment]] =
                  (adir: ADir) => QueryFile.Ops[QueryFile].ls(adir).run
                val analyze0: Analyze ~> Free[QueryFile, ?] =
                  Analyze.defaultInterpreter[QueryFile, SparkQScript, Fix[SparkQScript]](lp => toQScript(listContents)(lp).mapT(_.value))
                val analyzeInterpreter   = analyze0 andThen flatMapSNT(queryFileIntereter)
                BackendDef.DefinitionResult[Free[S, ?]](
                  (analyzeInterpreter :+: fileSystemInterpreter) andThen run,
                  close)
            }.liftM[DefErrT]
          }
        }  yield res
    }
}

}
