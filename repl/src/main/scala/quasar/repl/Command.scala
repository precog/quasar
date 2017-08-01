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
import quasar.contrib.pathy._
import quasar.sql.{Query}

import pathy.Path, Path._
import scalaz._, Scalaz._

sealed abstract class Command
object Command {
  private val ExitPattern         = "(?i)(?:exit)|(?:quit)".r
  private val HelpPattern         = "(?i)(?:help)|(?:commands)|\\?".r
  private val CdPattern           = "(?i)cd(?: +(.+))?".r
  private val NamedExprPattern    = "(?i)([^ :]+) *<- *(.+)".r
  private val ExplainPattern      = "(?i)explain +(.+)".r
  private val CompilePattern      = "(?i)compile +(.+)".r
  private val SchemaPattern       = "(?i)schema +(.+)".r
  private val LsPattern           = "(?i)ls(?: +(.+))?".r
  private val SavePattern         = "(?i)save +([\\S]+) (.+)".r
  private val AppendPattern       = "(?i)append +([\\S]+) (.+)".r
  private val DeletePattern       = "(?i)rm +([\\S]+)".r
  private val DebugPattern        = "(?i)(?:set +)?debug *= *(0|1|2)".r
  private val SummaryCountPattern = "(?i)(?:set +)?summaryCount *= *(\\d+)".r
  private val FormatPattern       = "(?i)(?:set +)?format *= *((?:table)|(?:precise)|(?:readable)|(?:csv))".r
  private val SetVarPattern       = "(?i)(?:set +)?(\\w+) *= *(.*\\S)".r
  private val UnsetVarPattern     = "(?i)unset +(\\w+)".r
  private val ListVarPattern      = "(?i)env".r

  final case object Exit extends Command
  final case object Help extends Command
  final case object ListVars extends Command
  final case class Cd(dir: XDir) extends Command
  final case class Select(name: Option[String], query: Query) extends Command
  final case class Explain(query: Query) extends Command
  final case class Compile(query: Query) extends Command
  final case class Schema(query: Query) extends Command
  final case class Ls(dir: Option[XDir]) extends Command
  final case class Save(path: XFile, value: String) extends Command
  final case class Append(path: XFile, value: String) extends Command
  final case class Delete(path: XFile) extends Command
  final case class Debug(level: DebugLevel) extends Command
  final case class SummaryCount(rows: Int) extends Command
  final case class Format(format: OutputFormat) extends Command
  final case class SetVar(name: String, value: String) extends Command
  final case class UnsetVar(name: String) extends Command

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def parse(input: String): Command =
    input match {
      case ExitPattern()                 => Exit
      case CdPattern(XDir(d))            => Cd(d)
      case CdPattern(_)                  => Cd(rootDir.right)
      case NamedExprPattern(name, query) => Select(Some(name), Query(query))
      case ExplainPattern(query)         => Explain(Query(query))
      case CompilePattern(query)         => Compile(Query(query))
      case SchemaPattern(query)          => Schema(Query(query))
      case LsPattern(XDir(d))            => Ls(d.some)
      case LsPattern(_)                  => Ls(none)
      case SavePattern(XFile(f), value)  => Save(f, value)
      case AppendPattern(XFile(f), value) => Append(f, value)
      case DeletePattern(XFile(f))       => Delete(f)
      case DebugPattern(code)            => Debug(DebugLevel.int.unapply(code.toInt) | DebugLevel.Normal)
      case SummaryCountPattern(rows)     => SummaryCount(rows.toInt)
      case FormatPattern(format)         => Format(OutputFormat.fromString(format) | OutputFormat.Table)
      case HelpPattern()                 => Help
      case SetVarPattern(name, value)    => SetVar(name, value)
      case UnsetVarPattern(name)         => UnsetVar(name)
      case ListVarPattern()              => ListVars
      case _                             => Select(None, Query(input))
    }

  type XDir = RelDir[Unsandboxed] \/ ADir
  object XDir {
    def unapply(str: String): Option[XDir] =
      Option(str)
        .filter(_ ≠ "")
        .map(s => if (s.endsWith("/")) s else s + "/")
        .flatMap { s =>
          posixCodec.parseRelDir(s).map(_.left) orElse
            posixCodec.parseAbsDir(s).map(unsafeSandboxAbs(_).right)
        }
  }
  type XFile = RelFile[Unsandboxed] \/ AFile
  object XFile {
    def unapply(str: String): Option[XFile] =
      Option(str)
        .filter(_ ≠ "")
        .flatMap { s =>
          posixCodec.parseRelFile(s).map(_.left) orElse
            posixCodec.parseAbsFile(s).map(unsafeSandboxAbs(_).right)
        }
  }
}
