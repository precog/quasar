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
package repl

import slamdata.Predef._
import quasar.api.datasource._
import quasar.api.resource.ResourcePath
import quasar.api.table.TableName
import quasar.mimir.evaluate.Pushdown
import quasar.run.optics.{stringUuidP => UuidString}
import quasar.sql.Query

import java.util.UUID

import scalaz._, Scalaz._

sealed abstract class Command

object Command {
  private val NamePattern                  = "[a-zA-Z0-9-]+"

  private val ExitPattern                  = "(?i)(?:exit)|(?:quit)".r
  private val NoOpPattern                  = """\s*""".r
  private val HelpPattern                  = """(?i)(?:help)|(?:commands)|\?""".r
  private val CdPattern                    = "(?i)cd(?: +(.+))?".r
  private val LsPattern                    = "(?i)ls(?: +(.+))?".r
  private val PwdPattern                   = "(?i)pwd".r
  private val SetPhaseFormatPattern        = "(?i)(?:set +)?phaseFormat *= *(tree|code)".r
  private val SetTimingFormatPattern       = "(?i)(?:set +)?timingFormat *= *(tree|onlytotal)".r
  private val DebugPattern                 = "(?i)(?:set +)?debug *= *(0|1|2)".r
  private val SummaryCountPattern          = """(?i)(?:set +)?summaryCount *= *(\d+)""".r
  private val FormatPattern                = "(?i)(?:set +)?format *= *((?:table)|(?:precise)|(?:readable)|(?:csv)|(?:homogeneouscsv))".r
  private val ModePattern                  = "(?i)(?:set +)?mode *= *((?:console)|(?:file))".r
  private val SetPushdownPattern           = "(?i)(?:set +)?pushdown *= *((?:true)|(?:false))".r
  private val SetVarPattern                = """(?i)(?:set +)?(\w+) *= *(.*\S)""".r
  private val UnsetVarPattern              = """(?i)unset +(\w+)""".r
  private val ListVarPattern               = "(?i)env".r
  private val DatasourceListPattern        = "(?i)ds(?: +)(?:list|ls)".r
  private val DatasourceTypesPattern       = "(?i)ds(?: +)types".r
  private val DatasourceAddPattern         = s"(?i)ds(?: +)(?:add +)($NamePattern)(?: +)($NamePattern)(?: +)(.*\\S)".r
  private val DatasourceLookupPattern      = """(?i)ds(?: +)(?:lookup|get) +([\S]+)""".r
  private val DatasourceRemovePattern      = """(?i)ds(?: +)(?:remove|rm) +([\S]+)""".r
  private val TableListPattern             = "(?i)table(?: +)(?:list|ls)".r
  private val TableAddPattern              = s"(?i)table(?: +)(?:add +)($NamePattern)(?: +)(.*\\S)".r
  private val TablePreparePattern          = """(?i)table(?: +)(?:prepare +)([\S]+)""".r
  private val TableCancelPrepPattern       = """(?i)table(?: +)(?:cancelPreparation +|cancel +)([\S]+)""".r
  private val TablePreparedDataPattern     = """(?i)table(?: +)(?:preparedData +|data +)([\S]+)""".r
  private val ResourceSchemaPattern        = "(?i)schema +(.+)".r
  private val ExplainPattern               = """(?i)(?:explain)(?: +)(.*\S)""".r
  private val CompilePattern               = """(?i)(?:compile)(?: +)(.*\S)""".r

  final case object Exit extends Command
  final case object NoOp extends Command
  final case object Help extends Command
  final case object ListVars extends Command
  final case object Pwd extends Command
  final case class Cd(dir: ReplPath) extends Command
  final case class Select(query: Query) extends Command
  final case class Explain(query: Query) extends Command
  final case class Compile(query: Query) extends Command
  final case class Ls(dir: Option[ReplPath]) extends Command
  final case class Debug(level: DebugLevel) extends Command
  final case class SummaryCount(rows: Int) extends Command
  final case class Format(format: OutputFormat) extends Command
  final case class Mode(mode: OutputMode) extends Command
  final case class SetPhaseFormat(format: PhaseFormat) extends Command
  final case class SetTimingFormat(format: TimingFormat) extends Command
  final case class SetVar(name: VarName, value: VarValue) extends Command
  final case class UnsetVar(name: VarName) extends Command
  final case class SetPushdown(pushdown: Pushdown) extends Command

  final case object DatasourceList extends Command
  final case object DatasourceTypes extends Command
  final case class DatasourceLookup(id: UUID) extends Command
  final case class DatasourceAdd(name: DatasourceName, tp: DatasourceType.Name, config: String) extends Command
  final case class DatasourceRemove(id: UUID) extends Command
  final case object TableList extends Command
  final case class TableAdd(name: TableName, query: Query) extends Command
  final case class TablePrepare(id: UUID) extends Command
  final case class TableCancelPrep(id: UUID) extends Command
  final case class TablePreparedData(id: UUID) extends Command
  final case class ResourceSchema(path: ReplPath) extends Command

  implicit val equalCommand: Equal[Command] = Equal.equalA

  def parse(input: String): Command =
    input match {
      case ExitPattern()                            => Exit
      case NoOpPattern()                            => NoOp
      case CdPattern(ReplPath(path))                => Cd(path)
      case CdPattern(_)                             => Cd(ReplPath.Absolute(ResourcePath.Root))
      case LsPattern(ReplPath(path))                => Ls(path.some)
      case LsPattern(_)                             => Ls(none)
      case DebugPattern(code)                       => Debug(DebugLevel.int.unapply(code.toInt) | DebugLevel.Normal)
      case SetPhaseFormatPattern(format)            => SetPhaseFormat(PhaseFormat.fromString(format) | PhaseFormat.Tree)
      case SetTimingFormatPattern(format)           => SetTimingFormat(TimingFormat.fromString(format) | TimingFormat.OnlyTotal)
      case SummaryCountPattern(rows)                => SummaryCount(rows.toInt)
      case FormatPattern(format)                    => Format(OutputFormat.fromString(format) | OutputFormat.Precise)
      case ModePattern(mode)                        => Mode(OutputMode.fromString(mode) | OutputMode.Console)
      case SetPushdownPattern(pd)                   => SetPushdown(PushdownMode.fromString(pd) | Pushdown.EnablePushdown)
      case HelpPattern()                            => Help
      case SetVarPattern(name, value)               => SetVar(VarName(name), VarValue(value))
      case UnsetVarPattern(name)                    => UnsetVar(VarName(name))
      case ListVarPattern()                         => ListVars
      case PwdPattern()                             => Pwd
      case DatasourceListPattern()                  => DatasourceList
      case DatasourceTypesPattern()                 => DatasourceTypes
      case DatasourceLookupPattern(UuidString(u))   => DatasourceLookup(u)
      case DatasourceAddPattern(n, DatasourceType.string(tp), cfg) =>
                                                       DatasourceAdd(DatasourceName(n), tp, cfg)
      case DatasourceRemovePattern(UuidString(u))   => DatasourceRemove(u)
      case TableListPattern()                       => TableList
      case TableAddPattern(n, s)                    => TableAdd(TableName(n), Query(s))
      case TablePreparePattern(UuidString(u))       => TablePrepare(u)
      case TableCancelPrepPattern(UuidString(u))    => TableCancelPrep(u)
      case TablePreparedDataPattern(UuidString(u))  => TablePreparedData(u)
      case ResourceSchemaPattern(ReplPath(path))    => ResourceSchema(path)
      case ExplainPattern(s)                        => Explain(Query(s))
      case CompilePattern(s)                        => Compile(Query(s))
      case _                                        => Select(Query(input))
    }
}
