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

package quasar.physical.marklogic.xcc

import quasar.physical.marklogic.MonadErrMsgs

import com.marklogic.xcc.Content
import scalaz._

/** Typeclass representing the ability to convert an `A` into `Content`, indexed
  * by the content format.
  */
trait AsContent[FMT, A] { self =>
  def asContent[F[_]: MonadErrMsgs](uri: ContentUri, a: A): F[Content]

  def contramap[B](f: B => A): AsContent[FMT, B] =
    new AsContent[FMT, B] {
      def asContent[F[_]: MonadErrMsgs](uri: ContentUri, b: B): F[Content] =
        self.asContent[F](uri, f(b))
    }
}

object AsContent {
  def apply[FMT, A](implicit A: AsContent[FMT, A]): AsContent[FMT, A] = A

  implicit def contravariant[FMT]: Contravariant[AsContent[FMT, ?]] =
    new Contravariant[AsContent[FMT, ?]] {
      def contramap[A, B](fa: AsContent[FMT, A])(f: B => A) = fa contramap f
    }
}
