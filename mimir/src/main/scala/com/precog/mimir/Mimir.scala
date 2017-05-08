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

package quasar.precog.mimir

import slamdata.Predef._
import quasar._
import quasar.common._
import quasar.connector._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly

object Mimir extends BackendModule {

  // optimistically equal to marklogic's
  type QS[T[_[_]]] =
    QScriptCore[T, ?] :\:
    ThetaJoin[T, ?] :\:
    Const[ShiftedRead[ADir], ?] :/:
    Const[Read[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[Read[AFile], ?]]))

  // TODO
  type Repr = Unit
  type M[A] = Task[A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def MonadFsErrM = ???
  def PhaseResultTellM = ???
  def PhaseResultListenM = ???
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = Unit

  def parseConfig(uri: ConnectionUri): EitherT[Task, ErrorMessages, Config] = ???

  def compile(cfg: Config): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    ???

  val Type = FileSystemType("mimir")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): M[Repr] = ???

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Kleisli[M, Config, AFile] = ???
    def evaluatePlan(repr: Repr): Kleisli[M, Config, ResultHandle] = ???
    def more(h: ResultHandle): Kleisli[M, Config, Vector[Data]] = ???
    def close(h: ResultHandle): Kleisli[M, Config, Unit] = ???
    def explain(repr: Repr): Kleisli[M, Config, String] = ???
    def listContents(dir: ADir): Kleisli[M, Config, Set[PathSegment]] = ???
    def fileExists(file: AFile): Kleisli[M, Config, Boolean] = ???
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Kleisli[M, Config, ReadHandle] = ???
    def read(h: ReadHandle): Kleisli[M, Config, Vector[Data]] = ???
    def close(h: ReadHandle): Kleisli[M, Config, Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    def open(file: AFile): Kleisli[M, Config, WriteHandle] = ???
    def write(h: WriteHandle, chunk: Vector[Data]): Kleisli[M, Config, Vector[FileSystemError]] = ???
    def close(h: WriteHandle): Kleisli[M, Config, Unit] = ???
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): Kleisli[M, Config, Unit] = ???
    def delete(path: APath): Kleisli[M, Config, Unit] = ???
    def tempFile(near: APath): Kleisli[M, Config, AFile] = ???
  }
}
