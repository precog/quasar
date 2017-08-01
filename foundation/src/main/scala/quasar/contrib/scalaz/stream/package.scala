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

package quasar.contrib.scalaz

import slamdata.Predef._
import quasar.contrib.scalaz.eitherT._

import scalaz._
import scalaz.stream.Process

package object stream {

  implicit class AugmentedProcess[M[_], A](p: Process[M, A]) {
    def runLogCatch(implicit monad: Monad[M]): M[Throwable \/ Vector[A]] = {
      val right = λ[M ~> EitherT[M, Throwable, ?]](EitherT.right(_))
      p.translate(right).runLog.run
    }
  }

}