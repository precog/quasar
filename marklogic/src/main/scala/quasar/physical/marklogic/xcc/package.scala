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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.contrib.scalaz._

import com.marklogic.xcc.{ContentSource, Session}
import com.marklogic.xcc.exceptions.XccException
import eu.timepit.refined.refineV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import monocle.Prism
import scalaz._, Scalaz._

package object xcc {
  type CSourceReader[F[_]] = MonadReader_[F, ContentSource]

  object CSourceReader {
    def apply[F[_]](implicit F: CSourceReader[F]): CSourceReader[F] = F
  }

  type SessionReader[F[_]] = MonadReader_[F, Session]

  object SessionReader {
    def apply[F[_]](implicit F: SessionReader[F]): SessionReader[F] = F
  }

  type XccErr[F[_]] = MonadError_[F, XccError]

  object XccErr {
    def apply[F[_]](implicit F: MonadError_[F, XccError]): XccErr[F] = F
  }

  type ContentUri = String Refined Uri
  val  ContentUri = Prism((s: String) => refineV[Uri](s).right.toOption)(_.get)

  def attemptXcc[F[_], A](fa: F[A])(implicit FM: Monad[F], FC: Catchable[F]): F[XccException \/ A] =
    FM.bind(FC.attempt(fa)) {
      case -\/(xe: XccException) => FM.point(xe.left)
      case -\/(t)                => FC.fail(t)
      case \/-(a)                => FM.point(a.right)
    }

  def handleXcc[F[_]: Monad: Catchable, A, B >: A](
    fa: F[A])(
    pf: PartialFunction[XccException, B]
  ): F[B] =
    handleXccWith[F, A, B](fa)(pf andThen (_.point[F]))

  def handleXccWith[F[_], A, B >: A](
    fa: F[A])(
    pf: PartialFunction[XccException, F[B]]
  )(implicit
    FM: Monad[F],
    FC: Catchable[F]
  ): F[B] =
    FM.bind(attemptXcc(fa)) {
      case -\/(e) => pf.lift(e) getOrElse FC.fail(e)
      case \/-(a) => FM.point[B](a)
    }
}
