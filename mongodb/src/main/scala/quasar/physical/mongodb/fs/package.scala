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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.{NameGenerator => NG}
import quasar.connector.{EnvironmentError, EnvErrT, EnvErr}
import quasar.config._
import quasar.effect.Failure
import quasar.contrib.pathy._
import quasar.fp._, free._
import quasar.fs._, mount._
import quasar.physical.mongodb.fs.bsoncursor._
import quasar.physical.mongodb.fs.fsops._

import com.mongodb.async.client.MongoClient
import pathy.Path.{depth, dirName}
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{Writer => _, _}

package object fs {
  import BackendDef.{DefinitionError, DefErrT}

  val FsType = FileSystemType("mongodb")

  final case class DefaultDb(run: DatabaseName)

  object DefaultDb {
    def fromPath(path: APath): Option[DefaultDb] =
      Collection.dbNameFromPath(path).map(DefaultDb(_)).toOption
  }

  final case class TmpPrefix(run: String) extends scala.AnyVal

  def fileSystem[S[_]](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): EnvErrT[Task, BackendEffect ~> Free[S, ?]] = {
    val runM = Hoist[EnvErrT].hoist(MongoDbIO.runNT(client))

    (
      runM(WorkflowExecutor.mongoDb)                |@|
      queryfile.run[BsonCursor, S](client, defDb)
        .liftM[EnvErrT]                             |@|
      readfile.run[S](client).liftM[EnvErrT]        |@|
      writefile.run[S](client).liftM[EnvErrT]       |@|
      managefile.run[S](client).liftM[EnvErrT]
    )((execMongo, qfile, rfile, wfile, mfile) => {
      interpretBackendEffect[Free[S, ?]](
        Empty.analyze[Free[S, ?]], // old mongo, will be removed
        qfile compose queryfile.interpret(execMongo),
        rfile compose readfile.interpret,
        wfile compose writefile.interpret,
        mfile compose managefile.interpret)
    })
  }

  def definition[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): BackendDef[Free[S, ?]] = BackendDef.fromPF[Free[S, ?]] {
    case (FsType, uri) =>
      type M[A] = Free[S, A]
      for {
        client <- asyncClientDef[S](uri)
        defDb  <- free.lift(findDefaultDb.run(client)).into[S].liftM[DefErrT]
        fs     <- EitherT[M, DefinitionError, BackendEffect ~> M](free.lift(
                    fileSystem[S](client, defDb)
                      .leftMap(_.right[NonEmptyList[String]])
                      .run
                  ).into[S])
        close  =  free.lift(Task.delay(client.close()).attempt.void).into[S]
      } yield BackendDef.DefinitionResult[M](fs, close)
  }

  val QScriptFsType = FileSystemType("mongodbq")

  def qscriptFileSystem[S[_]](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): EnvErrT[Task, BackendEffect ~> Free[S, ?]] = {
    val runM = Hoist[EnvErrT].hoist(MongoDbIO.runNT(client))

    (
      runM(WorkflowExecutor.mongoDb)                |@|
      queryfile.run[BsonCursor, S](client, defDb)
        .liftM[EnvErrT]                             |@|
      readfile.run[S](client).liftM[EnvErrT]        |@|
      writefile.run[S](client).liftM[EnvErrT]       |@|
      managefile.run[S](client).liftM[EnvErrT]
    )((execMongo, qfile, rfile, wfile, mfile) => {
      val fileSystemInterpreter = interpretFileSystem[Free[S, ?]](
        qfile compose queryfile.interpretQ(execMongo),
        rfile compose readfile.interpret,
        wfile compose writefile.interpret,
        mfile compose managefile.interpret)

      val analyzeInterpreter = Empty.analyze[Free[S, ?]]

      analyzeInterpreter :+: fileSystemInterpreter
    })
  }

  def qscriptDefinition[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): BackendDef[Free[S, ?]] = BackendDef.fromPF[Free[S, ?]] {
    case (QScriptFsType, uri) =>
      type M[A] = Free[S, A]
      for {
        client <- asyncClientDef[S](uri)
        defDb  <- free.lift(findDefaultDb.run(client)).into[S].liftM[DefErrT]
        fs     <- EitherT[M, DefinitionError, BackendEffect ~> M](free.lift(
                    qscriptFileSystem[S](client, defDb)
                      .leftMap(_.right[NonEmptyList[String]])
                      .run
                  ).into[S])
        close  =  free.lift(Task.delay(client.close()).attempt.void).into[S]
      } yield BackendDef.DefinitionResult[M](fs, close)
  }

  val listContents: ADir => EitherT[MongoDbIO, FileSystemError, Set[PathSegment]] =
    dir => EitherT(dirName(dir) match {
      case Some(_) =>
        collectionsInDir(dir)
          .map(_ foldMap (collectionPathSegment(dir) andThen (_.toSet)))
          .run

      case None if depth(dir) ≟ 0 =>
        MongoDbIO.collections
          .map(collectionPathSegment(dir))
          .pipe(process1.stripNone)
          .runLog
          .map(_.toSet.right[FileSystemError])

      case None =>
        nonExistentParent[Set[PathSegment]](dir).run
    })

  ////

  private type Eff[A] = (Task :\: EnvErr :/: CfgErr)#M[A]

  private def findDefaultDb: MongoDbIO[Option[DefaultDb]] =
    (for {
      coll0  <- MongoDbIO.liftTask(NG.salt).liftM[OptionT]
      coll   =  CollectionName(s"__${coll0}__")
      dbName <- MongoDbIO.firstWritableDb(coll)
      _      <- MongoDbIO.dropCollection(Collection(dbName, coll))
                  .attempt.void.liftM[OptionT]
    } yield DefaultDb(dbName)).run

  private[fs] def asyncClientDef[S[_]](
    uri: ConnectionUri
  )(implicit
    S0: Task :<: S
  ): DefErrT[Free[S, ?], MongoClient] = {
    import quasar.convertError
    type M[A] = Free[S, A]
    type ME[A, B] = EitherT[M, A, B]
    type MEEnvErr[A] = ME[EnvironmentError,A]
    type MEConfigErr[A] = ME[ConfigError,A]
    type DefM[A] = DefErrT[M, A]

    val evalEnvErr: EnvErr ~> DefM =
      convertError[M]((_: EnvironmentError).right[NonEmptyList[String]])
        .compose(Failure.toError[MEEnvErr, EnvironmentError])

    val evalCfgErr: CfgErr ~> DefM =
      convertError[M]((_: ConfigError).shows.wrapNel.left[EnvironmentError])
        .compose(Failure.toError[MEConfigErr, ConfigError])

    val liftTask: Task ~> DefM =
      liftMT[M, DefErrT] compose liftFT[S] compose injectNT[Task, S]

    util.createAsyncMongoClient[Eff](uri)
      .foldMap[DefM](liftTask :+: evalEnvErr :+: evalCfgErr)
  }
}
