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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar._
import quasar.common.{Int => _, Map => _, _}, PhaseResult.detail
import quasar.connector.BackendModule
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._, numeric._
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, BackendDef.DefErrT
import quasar.physical.couchbase.common._
import quasar.physical.couchbase.planner.Planner
import quasar.Planner.PlannerError
import quasar.qscript.{Map => _, _}

import scala.Predef.implicitly

import com.couchbase.client.java.transcoder.JsonTranscoder
import matryoshka._, data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

trait Couchbase extends BackendModule {
  type Eff[A] = (
    Task                                       :\:
    MonotonicSeq                               :\:
    GenUUID                                    :\:
    KeyValueStore[ReadHandle,   Cursor,  ?]    :\:
    KeyValueStore[WriteHandle,  Collection, ?] :/:
    KeyValueStore[ResultHandle, Cursor, ?]
  )#M[A]

  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  type Repr = Mu[N1QL]

  type M[A] = Free[Eff, A]

  implicit val codec: DataCodec = CBDataCodec

  val jsonTranscoder = new JsonTranscoder

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = common.Config

  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config] = fs.parseConfig(uri)

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = fs.compile(cfg)

  val Type: FileSystemType = FileSystemType("couchbase")

  val MR = MonadReader_[Backend, Config]
  val MT = MonadTell_[Backend, PhaseResults]
  val ME = MonadError_[Backend, FileSystemError]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
    cp: T[QSM[T, ?]]
  ): Backend[Repr] =
    for {
      cfg  <- MR.ask
      ctx  =  cfg.ctx
      plan <- ME.unattempt(
                cp.cataM(Planner[T, EitherT[Kleisli[Free[Eff, ?], Context, ?], PlannerError, ?], QSM[T, ?]].plan)
                  .bimap(
                    FileSystemError.qscriptPlanningFailed(_),
                    _.convertTo[Mu[N1QL]])
                  .run(Context(BucketName(ctx.bucket.name), ctx.docTypeKey))
                  .liftB)
      _    <- MT.tell(Vector(detail("N1QL AST", RenderTreeT[Mu].render(plan).shows)))
    } yield plan

  val QueryFileModule = new fs.queryfile with QueryFileModule

  val ReadFileModule = new fs.readfile with ReadFileModule

  val WriteFileModule = new fs.writefile with WriteFileModule

  val ManageFileModule = new fs.managefile with ManageFileModule
}

object Couchbase extends Couchbase
