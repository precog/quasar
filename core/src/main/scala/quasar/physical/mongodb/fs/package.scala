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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{EnvironmentError2, EnvErr2T, EnvErr, EnvErrF}
import quasar.{NameGenerator => NG}
import quasar.config._
import quasar.effect.Failure
import quasar.fp._
import quasar.fs.{Path => _, _}
import quasar.fs.mount.{ConnectionUri, FileSystemDef}
import quasar.physical.mongodb.fs.bsoncursor._

import com.mongodb.async.client.MongoClient
import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.either._
import scalaz.syntax.show._
import scalaz.syntax.nel._
import scalaz.concurrent.Task

package object fs {
  import FileSystemDef.{DefinitionError, DefErrT}

  type WFTask[A] = WorkflowExecErrT[Task, A]

  val MongoDBFsType = FileSystemType("mongodb")

  final case class DefaultDb(run: String) extends scala.AnyVal

  object DefaultDb {
    def fromPath(path: APath): Option[DefaultDb] =
      Collection.dbNameFromPath(path).map(DefaultDb(_)).toOption
  }

  final case class TmpPrefix(run: String) extends scala.AnyVal

  def mongoDbFileSystem(
    client: MongoClient,
    defDb: Option[DefaultDb]
  ): EnvErr2T[Task, FileSystem ~> WFTask] = {
    val liftWF = liftMT[Task, WorkflowExecErrT]
    val runM = Hoist[EnvErr2T].hoist(MongoDbIO.runNT(client))

    (
      runM(WorkflowExecutor.mongoDb)                 |@|
      queryfile.run[BsonCursor](client, defDb)
        .liftM[EnvErr2T]                             |@|
      readfile.run(client).liftM[EnvErr2T]           |@|
      writefile.run(client).liftM[EnvErr2T]          |@|
      managefile.run(client).liftM[EnvErr2T]
    )((execMongo, qfile, rfile, wfile, mfile) =>
      interpretFileSystem[WFTask](
        qfile compose queryfile.interpret(execMongo),
        liftWF compose rfile compose readfile.interpret,
        liftWF compose wfile compose writefile.interpret,
        liftWF compose mfile compose managefile.interpret))
  }

  /** TODO: Refactor MongoDB interpreters to interpret into these (and other) effects. */
  def mongoDbFileSystemF[S[_]: Functor](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: WorkflowExecErrF :<: S
  ): EnvErr2T[Task, FileSystem ~> Free[S, ?]] = {
    type M[A] = Free[S, A]

    val wfeErr = Failure.Ops[WorkflowExecutionError, S]
    def liftT[A](ta: Task[A]): M[A] = free.lift(ta).into[S]

    val toS: WFTask ~> M =
      new (WFTask ~> M) {
        def apply[A](wf: WFTask[A]) =
          liftT(wf.run).flatMap(_.fold(wfeErr.fail, _.point[M]))
      }

    mongoDbFileSystem(client, defDb) map (toS compose _)
  }

  def mongoDbFileSystemDef[S[_]: Functor](
    implicit S0: Task :<: S,
             S1: WorkflowExecErrF :<: S
  ): FileSystemDef[Free[S, ?]] = FileSystemDef.fromPF[Free[S, ?]] {
    case (MongoDBFsType, uri) =>
      type M[A] = Free[S, A]
      for {
        client <- asyncClientDef[S](uri)
        defDb  <- free.lift(findDefaultDb.run(client)).into[S].liftM[DefErrT]
        fs     <- EitherT[M, DefinitionError, FileSystem ~> M](free.lift(
                    mongoDbFileSystemF[S](client, defDb)
                      .leftMap(_.right[NonEmptyList[String]])
                      .run
                  ).into[S])
        close  =  free.lift(Task.delay(client.close()).attempt.void).into[S]
      } yield (fs, close)
  }

  ////

  private type Eff0[A] = Coproduct[EnvErrF, CfgErrF, A]
  private type Eff[A]  = Coproduct[Task, Eff0, A]

  private def findDefaultDb: MongoDbIO[Option[DefaultDb]] =
    (for {
      coll0  <- MongoDbIO.liftTask(NG.salt).liftM[OptionT]
      coll   =  s"__${coll0}__"
      dbName <- MongoDbIO.firstWritableDb(coll)
      _      <- MongoDbIO.dropCollection(Collection(dbName, coll))
                  .attempt.void.liftM[OptionT]
    } yield DefaultDb(dbName)).run

  private def asyncClientDef[S[_]: Functor](
    uri: ConnectionUri
  )(implicit
    S0: Task :<: S
  ): DefErrT[Free[S, ?], MongoClient] = {
    import quasar.Errors.convertError
    type M[A] = Free[S, A]
    type ME[A, B] = EitherT[M, A, B]
    type DefM[A] = DefErrT[M, A]

    val evalEnvErr: EnvErrF ~> DefM =
      Coyoneda.liftTF[EnvErr, DefM](
        convertError[M]((_: EnvironmentError2).right[NonEmptyList[String]])
          .compose[EnvErr](Failure.toError[ME, EnvironmentError2]))

    val evalCfgErr: CfgErrF ~> DefM =
      Coyoneda.liftTF[CfgErr, DefM](
        convertError[M]((_: ConfigError).shows.wrapNel.left[EnvironmentError2])
          .compose[CfgErr](Failure.toError[ME, ConfigError]))

    val liftTask: Task ~> DefM =
      liftMT[M, DefErrT] compose liftFT[S] compose injectNT[Task, S]

    util.createAsyncMongoClient[Eff](uri)
      .foldMap[DefM](free.interpret3(liftTask, evalEnvErr, evalCfgErr))
  }
}
