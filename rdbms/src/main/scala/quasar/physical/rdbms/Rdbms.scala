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

package quasar.physical.rdbms

import slamdata.Predef._
import quasar.connector.{BackendModule, DefaultAnalyzeModule}
import quasar.contrib.pathy.{AFile, APath}
import quasar.contrib.scalaz.MonadReader_
import quasar.fp.{:/:, :\:}
import quasar.fp.free._
import quasar.fs.MonadFsErr
import quasar.fs.mount.BackendDef.{DefErrT, DefinitionError}
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.fs._
import quasar.qscript.{EquiJoin, ExtractPath, Injectable, Optimize, QScriptCore, QScriptTotal, ShiftedRead, Unicoalesce, Unirewrite}
import quasar.physical.rdbms.common.Config
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo
import quasar.{RenderTree, RenderTreeT, fp}
import quasar.qscript.analysis._

import scala.Predef.implicitly

import doobie.imports.Transactor
import doobie.hikari.hikaritransactor.HikariTransactor
import matryoshka.{BirecursiveT, Delay, EqualT, RecursiveT, ShowT}
import matryoshka.data._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait Rdbms extends BackendModule with RdbmsReadFile with RdbmsWriteFile with RdbmsManageFile with RdbmsQueryFile with Interpreter with DefaultAnalyzeModule {

  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr        = String // TODO define best Repr for a SQL query (Doobie Fragment?)
  type Eff[A] = model.Eff[A]
  type M[A] = model.M[A]
  type Config = common.Config

  implicit class LiftEffBackend[F[_], A](m: F[A])(implicit I: F :<: Eff) {
    val liftB: Backend[A] = lift(m).into[Eff].liftB
  }

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT]                               = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]]                                             = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM                                                            = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]    = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[
    QSM[T, ?],
    QScriptTotal[T, ?]] = quasar.physical.rdbms.qScriptToQScriptTotal[T]

  override def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]: QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    O.optimize(fp.reflNT[QSM[T, ?]])
  }

  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config] =
    EitherT(Task.delay(parseConnectionUri(uri).map(Config.apply)))

  def transactor(cfg: Config): Task[HikariTransactor[Task]] = {
    HikariTransactor[Task](
      cfg.connInfo.driverClassName,
      cfg.connInfo.url,
      cfg.connInfo.userName,
      cfg.connInfo.password.getOrElse("")
    )
  }

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val xa = transactor(cfg)
    val close = xa.flatMap(_.configure(_.close()))
    (interp(xa) ∘ (i => (foldMapNT[Eff, Task](i), close))).liftM[DefErrT]
  }

  lazy val MR                   = MonadReader_[Backend, Config]
  lazy val MT                   = quasar.effect.Read.monadReader_[Transactor[Task], Eff]
  lazy val ME                   = MonadFsErr[Backend]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {
    ???
  } // TODO

  def parseConnectionUri(uri: ConnectionUri): DefinitionError \/ JdbcConnectionInfo
}
