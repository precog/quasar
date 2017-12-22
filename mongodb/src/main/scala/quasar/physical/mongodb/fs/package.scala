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
import quasar.common.PhaseResultT
import quasar.config._
import quasar.effect.{Failure, KeyValueStore, MonotonicSeq}
import quasar.contrib.pathy._
import quasar.fp._, free._
import quasar.fs._, mount._
import quasar.physical.mongodb.fs.fsops._
import quasar.{qscript => qs}

import com.mongodb.async.client.MongoClient
import java.time.Instant
import pathy.Path.{depth, dirName}
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{Writer => _, _}
import iotaz._, TListK.:::

package object fs {
  import BackendDef.DefErrT
  type JustTask[A] = CopK[Task ::: TNilK, A]
  type PlanT[F[_], A] = ReaderT[FileSystemErrT[PhaseResultT[F, ?], ?], Instant, A]

  type MongoReadHandles[A] = KeyValueStore[ReadFile.ReadHandle, BsonCursor, A]
  type MongoWriteHandles[A] = KeyValueStore[WriteFile.WriteHandle, Collection, A]

  type Eff[A] =
    CopK[
      MonotonicSeq :::
      MongoDbIO :::
      fs.queryfileTypes.MongoQuery[BsonCursor, ?] :::
      fs.managefile.MongoManage :::
      MongoReadHandles :::
      MongoWriteHandles :::
      TNilK, A]

  type MongoM[A] = Free[Eff, A]

  type MongoQScriptCP[T[_[_]]] = qs.QScriptCore[T, ?] :\: qs.EquiJoin[T, ?] :/: Const[qs.ShiftedRead[AFile], ?]
  type MongoQScript[T[_[_]], A] = MongoQScriptCP[T]#M[A]

  final case class MongoConfig(
    client: MongoClient,
    serverVersion: ServerVersion,
    defaultDb: Option[fs.DefaultDb],
    wfExec: WorkflowExecutor[MongoDbIO, BsonCursor])

  final case class DefaultDb(run: DatabaseName)

  object DefaultDb {
    def fromPath(path: APath): Option[DefaultDb] =
      Collection.dbNameFromPath(path).map(DefaultDb(_)).toOption
  }

  final case class TmpPrefix(run: String) extends scala.AnyVal

  type PhysFsEff[A]  = CopK[Task ::: PhysErr ::: TNilK, A]

  def parseConfig(uri: ConnectionUri)
      : DefErrT[Task, MongoConfig] =
    (for {
      client <- asyncClientDef[JustTask](uri)
      version <- free.lift(MongoDbIO.serverVersion.run(client)).into[JustTask].liftM[DefErrT]
      defDb <- free.lift(findDefaultDb.run(client)).into[JustTask].liftM[DefErrT]
      wfExec <- wfExec(client)
    } yield MongoConfig(client, version, defDb, wfExec)).mapT(freeTaskToTask.apply)

  def compile(cfg: MongoConfig): BackendDef.DefErrT[Task, (MongoM ~> Task, Task[Unit])] =
    (effToTask(cfg) map (i => (
      foldMapNT[Eff, Task](i),
      Task.delay(cfg.client.close).void))).liftM[DefErrT]

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

  private val freeTaskToTask: JustTask ~> Task =
    CopK.NaturalTransformation.of[JustTask, Task](NaturalTransformation.refl[Task])

  def wfExec(client: MongoClient): DefErrT[Free[Task, ?], WorkflowExecutor[MongoDbIO, BsonCursor]] = {
    val run: EnvErrT[MongoDbIO, ?] ~> EnvErrT[Task, ?] = Hoist[EnvErrT].hoist(MongoDbIO.runNT(client))
    val runWf: EnvErrT[Task, WorkflowExecutor[MongoDbIO, BsonCursor]] = run(WorkflowExecutor.mongoDb)
    val envErrToDefErr: EnvErrT[Task, ?] ~> DefErrT[Task, ?] =
      quasar.convertError[Task]((_: EnvironmentError).right[NonEmptyList[String]])
    val runWfx: DefErrT[Task, WorkflowExecutor[MongoDbIO, BsonCursor]] = envErrToDefErr(runWf)
    runWfx.mapT(Free.liftF(_))
  }

  private def effToTask(cfg: MongoConfig): Task[Eff ~> Task] = {
    (
      MonotonicSeq.from(0L) |@|
      Task.delay(MongoDbIO.runNT(cfg.client)) |@|
      queryfile.run[BsonCursor, PhysFsEff](cfg.client, cfg.defaultDb) |@|
      managefile.run[PhysFsEff](cfg.client) |@|
      KeyValueStore.impl.default[ReadFile.ReadHandle, BsonCursor] |@|
      KeyValueStore.impl.default[WriteFile.WriteHandle, Collection]
    )((seq, io, qfile, mfile, rh, wh) => {
      CopK.NaturalTransformation.of[Eff, Task](
        seq,
        io,
        freeFsEffToTask compose qfile,
        freeFsEffToTask compose mfile,
        rh,
        wh)
    })
  }

  private def freeFsEffToTask: Free[PhysFsEff, ?] ~> Task = foldMapNT[PhysFsEff, Task](fsEffToTask)

  private def fsEffToTask: PhysFsEff ~> Task = λ[PhysFsEff ~> Task](_.run.fold(
    NaturalTransformation.refl[Task],
    Failure.toRuntimeError[Task, PhysicalError]
  ))

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
    S0: Task :<<: S
  ): DefErrT[Free[S, ?], MongoClient] = {
    import quasar.convertError
    type Eff[A] = (Task :\: EnvErr :/: CfgErr)#M[A]
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
