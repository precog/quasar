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

import slamdata.Predef._
import quasar.{Planner, Type}, Planner._, Type._
import quasar.contrib.scalaz._
import quasar.fp.ski._
import quasar.fs._, FileSystemError._

import java.time.Instant

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object common {
  type ExecTimeR[F[_]] = MonadReader_[F, Instant]

  def raiseErr[M[_], A](err: FileSystemError)(
    implicit ev: MonadFsErr[M]
  ): M[A] = ev.raiseError(err)

  def handleErr[M[_], A](ma: M[A])(f: FileSystemError => M[A])(
    implicit ev: MonadFsErr[M]
  ): M[A] = ev.handleError(ma)(f)

  def raisePlannerError[M[_]: MonadFsErr, A](err: PlannerError): M[A] =
    raiseErr(qscriptPlanningFailed(err))

  def raiseInternalError[M[_]: MonadFsErr, A](msg: String): M[A] =
    raisePlannerError(InternalError.fromMsg(msg))

  def unimplemented[M[_]: MonadFsErr, A](label: String): M[A] =
    raiseInternalError(s"unimplemented $label")

  def unpack[T[_[_]]: BirecursiveT, F[_]: Traverse](t: Free[F, T[F]]): T[F] =
    t.cata(interpret[F, T[F], T[F]](ι, _.embed))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def generateTypeCheck[In, Out](or: (Out, Out) => Out)(f: PartialFunction[Type, In => Out]):
      Type => Option[In => Out] =
    typ => f.lift(typ).fold(
      typ match {
        case Type.Temporal =>
          generateTypeCheck(or)(f)(Type.OffsetDateTime)
        case Type.LocalDateTime ⨿ OffsetDateTime =>  // time_of_day
          generateTypeCheck(or)(f)(Type.OffsetDateTime)
        case Type.OffsetDateTime ⨿ Type.OffsetDate ⨿
            Type.LocalDateTime ⨿ Type.LocalDate =>  // date_part
          generateTypeCheck(or)(f)(Type.OffsetDateTime)
        case Type.Arr(_) => generateTypeCheck(or)(f)(Type.AnyArray)
        case a ⨿ b =>
          (generateTypeCheck(or)(f)(a) ⊛ generateTypeCheck(or)(f)(b))(
            (a, b) => ((expr: In) => or(a(expr), b(expr))))
        case _ => None
      })(Some(_))

  object Keys {
    val wrap = "wrap"
  }
}
