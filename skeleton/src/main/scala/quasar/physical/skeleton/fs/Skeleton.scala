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
package physical.skeleton

import slamdata.Predef._
import quasar.connector._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._
import quasar.qscript.analysis._
import quasar.qscript.rewrites.{Unicoalesce, Unirewrite}
import quasar.contrib.iota.SubInject

import matryoshka._
import matryoshka.data._
import scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly
import iotaz.TListK.:::
import iotaz.TNilK

object Skeleton extends BackendModule with DefaultAnalyzeModule {

  // default QS subset; change if you're cool/weird/unique!
  type QS[T[_[_]]] = QScriptCore[T, ?] ::: EquiJoin[T, ?] ::: Const[ShiftedRead[AFile], ?] ::: TNilK

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[QSM[T, ?], QScriptTotal[T, ?]] = SubInject[QSM[T, ?], QScriptTotal[T, ?]]

  // make this your repr and monad
  type Repr = Unit
  type M[A] = Nothing

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<<: QSM[T, ?]]
  def MonadM = ??? // Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = Unit

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = quasar.fp.ski.ι

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] = ???

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] = ???

  val Type = FileSystemType("skeleton")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = ???

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Backend[Unit] = ???
    def evaluatePlan(repr: Repr): Backend[ResultHandle] = ???
    def more(h: ResultHandle): Backend[Vector[Data]] = ???
    def close(h: ResultHandle): Configured[Unit] = ???
    def explain(repr: Repr): Backend[String] = ???
    def listContents(dir: ADir): Backend[Set[PathSegment]] = ???
    def fileExists(file: AFile): Configured[Boolean] = ???
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = ???
    def read(h: ReadHandle): Backend[Vector[Data]] = ???
    def close(h: ReadHandle): Configured[Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    def open(file: AFile): Backend[WriteHandle] = ???
    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = ???
    def close(h: WriteHandle): Configured[Unit] = ???
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] = ???
    def copy(pair: PathPair): Backend[Unit] = ???
    def delete(path: APath): Backend[Unit] = ???
    def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] = ???
  }
}
