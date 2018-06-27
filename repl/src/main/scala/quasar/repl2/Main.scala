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
package repl2

import slamdata.Predef._

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.concurrent.Ref
import scalaz._, Scalaz._
import shims._

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    Ref.of[IO, ReplState](ReplState.mk) >>=
      (ref => Repl.mk[IO](ref).loop)
  }
}
