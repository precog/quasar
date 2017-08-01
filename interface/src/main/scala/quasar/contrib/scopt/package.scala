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

package quasar.contrib

import slamdata.Predef._

import _root_.scopt.OptionParser
import _root_.scalaz._
import _root_.scalaz.concurrent.Task

package object scopt {
  implicit class SafeOptionParser[A](p: OptionParser[A]) {
    /** Parse command line arguments and produce an A.
      * Otherwise print an error message to the standard error output and return a left explaining that this was done
      * @param args The command line arguments to be parsed
      * @param default The default values to be chosen in the case where they are not specified by the user
      */
    def safeParse(args: Vector[String], default: A): EitherT[Task, String, A] =
      OptionT(Task.delay(p.parse(args, default)))
        .toRight("Failed to parse command line options. Specific error was printed to standard error output")
  }
}