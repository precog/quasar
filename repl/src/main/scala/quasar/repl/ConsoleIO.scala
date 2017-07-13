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

package quasar.repl

import slamdata.Predef._

import quasar.effect.LiftedOps

import scalaz._

sealed abstract class ConsoleIO[A]
object ConsoleIO {
  final case class PrintLn(message: String) extends ConsoleIO[Unit]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]](implicit S: ConsoleIO :<: S)
    extends LiftedOps[ConsoleIO, S] {

    def println(message: String): FreeS[Unit] =
      lift(PrintLn(message))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: ConsoleIO :<: S): Ops[S] =
      new Ops[S]
  }
}
