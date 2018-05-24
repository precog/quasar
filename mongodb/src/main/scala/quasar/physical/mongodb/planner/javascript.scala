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

package quasar.physical.mongodb.planner

import quasar._
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.jscore.{JsCore, JsFn}
import quasar.physical.mongodb.planner.common._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object javascript {
  def processMapFunc[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR, A]
    (recovery: A => JsCore)
    (fm: FreeMapA[T, A])
      : M[JsCore] =
    fm.cataM(interpretM[M, MapFunc[T, ?], A, JsCore](recovery(_).point[M], javascript))

  def javascript[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
      : AlgebraM[M, MapFunc[T, ?], JsCore] =
    JsFuncHandler.handle[MapFunc[T, ?], M]

  def getJsMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
    (a1: JsCore, a2: JsCore)
      : JoinFunc[T] => M[JsFn] =
    processMapFunc[T, M, JoinSide] {
      case LeftSide => a1
      case RightSide => a2
    } (_) ∘ (JsFn(JsFn.defaultName, _))

  def getJsRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
      : Free[MapFunc[T, ?], ReduceIndex] => M[JsFn] =
    processMapFunc[T, M, ReduceIndex](_.idx.fold(
      i => jscore.Select(jscore.Select(jscore.Ident(JsFn.defaultName), "_id"), i.toString),
      i => jscore.Select(jscore.Ident(JsFn.defaultName), createFieldName("f", i))))(_) ∘
      (JsFn(JsFn.defaultName, _))

  def getJsFn[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad: MonadFsErr: ExecTimeR]
      : FreeMap[T] => M[JsFn] =
    processMapFunc[T, M, Hole](κ(jscore.Ident(JsFn.defaultName)))(_) ∘
      (JsFn(JsFn.defaultName, _))

}
