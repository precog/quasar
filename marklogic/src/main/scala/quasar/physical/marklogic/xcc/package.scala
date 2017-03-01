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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.catchable._
import quasar.effect.Capture
import quasar.fp.ski.κ

import com.marklogic.xcc.{ContentSource, Session}
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

    def withSession[F[_]: Bind: Capture: SessionReader, A](f: Session => A): F[A] =
      apply[F].ask >>= (s => Capture[F].capture(f(s)))
  }

  type ContentUri = String Refined Uri
  val  ContentUri = Prism((s: String) => refineV[Uri](s).right.toOption)(_.get)

  /** Returns a natural transformation that safely runs a `Session` reader,
    * ensuring the provided sessions are properly closed after use.
    */
  def provideSession[F[_]: Monad: Capture: Catchable](cs: ContentSource): Kleisli[F, Session, ?] ~> F =
    λ[Kleisli[F, Session, ?] ~> F] { sr =>
      contentsource.defaultSession[Kleisli[F, ContentSource, ?]]
        .run(cs)
        .flatMap(s => sr.run(s).ensuring(κ(Capture[F].capture(s.close()))))
    }
}
