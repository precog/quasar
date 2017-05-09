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

package quasar.blueeyes
package util

case class CommandLineArguments private (parameters: Map[String, String], values: List[String]) {
  def +(that: CommandLineArguments): CommandLineArguments = new CommandLineArguments(this.parameters ++ that.parameters, this.values ++ that.values)

  def size = parameters.size + values.length
}
object CommandLineArguments {
  def apply(args: String*): CommandLineArguments = {
    var Key   = """-{1,2}(.+)""".r
    var Value = """([^\-].*)""".r

    def parse0(args: List[String]): CommandLineArguments = args match {
      case Key(opt1) :: (second @ Key(opt2)) :: rest =>
        new CommandLineArguments(Map(opt1 -> ""), Nil) + parse0(second :: rest)

      case Key(opt1) :: Value(val1) :: rest =>
        new CommandLineArguments(Map(opt1 -> val1), Nil) + parse0(rest)

      case Value(val1) :: rest =>
        new CommandLineArguments(Map(), val1 :: Nil) + parse0(rest)

      case Key(opt1) :: Nil =>
        new CommandLineArguments(Map(opt1 -> ""), Nil)

      case Nil =>
        new CommandLineArguments(Map(), Nil)
    }

    parse0(args.toList)
  }
}
