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

package quasar

import slamdata.Predef._

import java.lang.System
import scalaz.concurrent.Task
import scalaz._, Scalaz._

object console {
  def stdout(msg: => String): Task[Unit] =
    Task.delay(println(msg))

  def stderr(msg: => String): Task[Unit] =
    Task.delay(System.err.println(msg))

  def logErrors(a: quasar.Errors.ETask[String, Unit]): Task[Unit] =
    a.swap
    .flatMapF(e => stderr("Error: " + e).map(_.right))
    .merge
    .handleWith { case err => stderr("Error: " + err.getMessage) }

  /** Check for a Java system property that is defined and has the value
    * "true", case-insensitively.
    */
  def booleanProp(name: String): Task[Boolean] = Task.delay {
    Option(java.lang.System.getProperty(name)).cata(
      _.equalsIgnoreCase("true"),
      false)
  }

  /** Read the value of an environment variable. */
  def readEnv(name: String): OptionT[Task, String] =
    Task.delay(System.getenv).liftM[OptionT]
      .flatMap(env => OptionT(Task.delay(Option(env.get(name)))))
}
