/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.contrib.pathy

import slamdata.Predef._

import java.io.{File => JFile}

import _root_.pathy.Path, Path._
import _root_.scalaz.concurrent.Task

object Helpers {
  /** Returns the contents of the file as a `String`. */
  def textContents(file: RFile): Task[String] =
    jtextContents(jFile(file))

  /** Returns the contents of the file as a `String`. */
  def jtextContents(file: JFile): Task[String] =
    Task.delay(scala.io.Source.fromInputStream(new FileInputStream(file)).mkString)

  def jFile(path: Path[_, _, Sandboxed]): JFile =
    new JFile(posixCodec.printPath(path))
}
