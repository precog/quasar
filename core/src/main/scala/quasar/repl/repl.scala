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

package quasar.repl

import quasar.Predef._
import quasar._, Backend._, Errors._, Planner._
import quasar.Evaluator._
import quasar.config._
import quasar.fp._
import quasar.fs._, Path._
import quasar.fs.mount.MountingsConfig2
import quasar.physical.mongodb.util
import quasar.sql._
import quasar.stacktrace.StackUtil

import org.jboss.aesh.console.{AeshConsoleCallback, Console, ConsoleOperation, Prompt}
import org.jboss.aesh.console.helper.InterruptHook
import org.jboss.aesh.console.settings.SettingsBuilder
import org.jboss.aesh.edit.actions.Action
import pathy.Path.{File, Sandboxed}
import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

object Repl {
  sealed trait Command
  object Command {
    val ExitPattern         = "(?i)(?:exit)|(?:quit)".r
    val HelpPattern         = "(?i)(?:help)|(?:commands)|\\?".r
    val CdPattern           = "(?i)cd(?: +(.+))?".r
    val NamedExprPattern    = "(?i)([^ :]+) *:= *(.+)".r
    val LsPattern           = "(?i)ls(?: +(.+))?".r
    val SavePattern         = "(?i)save +([\\S]+) (.+)".r
    val AppendPattern       = "(?i)append +([\\S]+) (.+)".r
    val DeletePattern       = "(?i)rm +([\\S]+)".r
    val DebugPattern        = "(?i)(?:set +)?debug *= *(0|1|2)".r
    val SummaryCountPattern = "(?i)(?:set +)?summaryCount *= *(\\d+)".r
    val SetVarPattern       = "(?i)(?:set +)?(\\w+) *= *(.*\\S)".r
    val UnsetVarPattern     = "(?i)unset +(\\w+)".r
    val ListVarPattern      = "(?i)env".r

    final case object Exit extends Command
    final case object Help extends Command
    final case object ListVars extends Command
    final case class Cd(dir: Path) extends Command
    final case class Select(name: Option[String], query: String) extends Command
    final case class Ls(dir: Option[Path]) extends Command
    final case class Save(path: Path, value: String) extends Command
    final case class Append(path: Path, value: String) extends Command
    final case class Delete(path: Path) extends Command
    final case class Debug(level: DebugLevel) extends Command
    final case class SummaryCount(rows: Int) extends Command
    final case class SetVar(name: String, value: String) extends Command
    final case class UnsetVar(name: String) extends Command
  }

  private type Printer = String => Task[Unit]

  sealed trait DebugLevel
  object DebugLevel {
    final case object Silent extends DebugLevel
    final case object Normal extends DebugLevel
    final case object Verbose extends DebugLevel

    def fromInt(code: Int): Option[DebugLevel] = code match {
      case 0 => Some(Silent)
      case 1 => Some(Normal)
      case 2 => Some(Verbose)
      case _ => None
    }
  }

  final case class RunState(
    printer:      Printer,
    backend:      Backend,
    path:         Path,
    unhandled:    Option[Command],
    debugLevel:   DebugLevel,
    summaryCount: Int,
    variables:    Map[String, String])

  def targetPath(s: RunState, path: Option[Path]): Path =
    path.flatMap(_.from(s.path).toOption).getOrElse(s.path)

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Null"))
  private def parseCommand(input: String): Command = {
    import Command._

    input match {
      case ExitPattern()                 => Exit
      case CdPattern(path)               =>
        Cd(
          if (path == null || path.trim.length == 0) Path.Root
          else Path(path.trim))
      case NamedExprPattern(name, query) => Select(Some(name), query)
      case LsPattern(path)               =>
        Ls(
          if (path == null || path.trim.length == 0) None
          else Some(Path(path.trim)))
      case SavePattern(path, value)      => Save(Path(path), value)
      case AppendPattern(path, value)    => Append(Path(path), value)
      case DeletePattern(path)           => Delete(Path(path))
      case DebugPattern(code)            =>
        Debug(DebugLevel.fromInt(code.toInt).getOrElse(DebugLevel.Normal))
      case SummaryCountPattern(rows)     => SummaryCount(rows.toInt)
      case HelpPattern()                 => Help
      case SetVarPattern(name, value)    => SetVar(name, value)
      case UnsetVarPattern(name)         => UnsetVar(name)
      case ListVarPattern()              => ListVars
      case _                             => Select(None, input)
    }
  }

  private def commandInput: Task[(Printer, Process[Task, Command])] =
    Task.delay {
      val queue = async.unboundedQueue[Command](Strategy.Sequential)

      val console =
        new Console(new SettingsBuilder()
          .parseOperators(false)
          .enableExport(false)
          .interruptHook(new InterruptHook {
            def handleInterrupt(console: Console, action: Action) = {
              queue.enqueueOne(Command.Exit).run
              console.getShell.out.println("exit")
              console.stop
            }
          })
          .create())
      console.setPrompt(new Prompt("💪 $ "))

      val out = (s: String) => Task.delay { console.getShell.out.println(s) }
      console.setConsoleCallback(new AeshConsoleCallback() {
        override def execute(input: ConsoleOperation): Int = {
          val command = parseCommand(input.getBuffer.trim)
          command match {
            case Command.Exit => console.stop()
            case _            => ()
          }
          queue.enqueueOne(command).run
          0
        }
      })

      console.start()

      (out, queue.dequeue)
    }

  def showHelp(state: RunState): Task[Unit] =
    state.printer(
      """|Quasar REPL, Copyright (C) 2014 SlamData Inc.
         |
         | Available commands:
         |   exit
         |   help
         |   cd [path]
         |   select [query]
         |   [id] := select [query]
         |   ls [path]
         |   save [path] [value]
         |   append [path] [value]
         |   rm [path]
         |   set debug = [level]
         |   set summaryCount = [rows]
         |   set [var] = [value]
         |   env""".stripMargin)

  def listVars(state: RunState): Task[Unit] =
    state.printer(state.variables.map(t => t._1 + " = " + t._2).mkString("\n"))

  def showError(state: RunState): Task[Unit] =
    state.printer("""|Unrecognized command!""".stripMargin)

  sealed trait EngineError { def message: String }
  object EngineError {
    final case class EParsingError(e: ParsingError) extends EngineError {
      def message = e.message
    }
    final case class ECompilationError(e: CompilationError) extends EngineError {
      def message = e.message
    }
    final case class EPathError(e: PathError) extends EngineError {
      def message = e.message
    }
    final case class EProcessingError(e: ProcessingError) extends EngineError {
      def message = e.message
    }
    final case class EEvaluationError(e: EvaluationError) extends EngineError {
      def message = e.message
    }
    final case class EDataEncodingError(e: DataEncodingError)
        extends EngineError {
      def message = e.message
    }
    final case class EWriteError(e: WriteError)
        extends EngineError {
      def message = e.message
    }
  }

  type EngineTask[A] = ETask[EngineError, A]
  type EngineProc[A] = Process[EngineTask, A]

  object EParsingError {
    def apply(error: ParsingError): EngineError = EngineError.EParsingError(error)
    def unapply(obj: EngineError): Option[ParsingError] = obj match {
      case EngineError.EParsingError(error) => Some(error)
      case _                       => None
    }
  }
  object ECompilationError {
    def apply(error: CompilationError): EngineError = EngineError.ECompilationError(error)
    def unapply(obj: EngineError): Option[CompilationError] = obj match {
      case EngineError.ECompilationError(error) => Some(error)
      case _                       => None
    }
  }
  object EPathError {
    def apply(error: PathError): EngineError = EngineError.EPathError(error)
    def unapply(obj: EngineError): Option[PathError] = obj match {
      case EngineError.EPathError(error) => Some(error)
      case _                       => None
    }
  }
  object EProcessingError {
    def apply(error: ProcessingError): EngineError = EngineError.EProcessingError(error)
    def unapply(obj: EngineError): Option[ProcessingError] = obj match {
      case EngineError.EProcessingError(error) => Some(error)
      case _                       => None
    }
  }
  object EEvaluationError {
    def apply(error: EvaluationError): EngineError = EngineError.EEvaluationError(error)
    def unapply(obj: EngineError): Option[EvaluationError] = obj match {
      case EngineError.EEvaluationError(error) => Some(error)
      case _                       => None
    }
  }
  object EDataEncodingError {
    def apply(error: DataEncodingError): EngineError = EngineError.EDataEncodingError(error)
    def unapply(obj: EngineError): Option[DataEncodingError] = obj match {
      case EngineError.EDataEncodingError(error) => Some(error)
      case _                       => None
    }
  }
  object EWriteError {
    def apply(error: WriteError): EngineError = EngineError.EWriteError(error)
    def unapply(obj: EngineError): Option[WriteError] = obj match {
      case EngineError.EWriteError(error) => Some(error)
      case _                       => None
    }
  }

  def select(state: RunState, query: String, name: Option[String]):
      EngineTask[Unit] = {
    def summarize(max: Int)(rows: IndexedSeq[Data]): String =
      if (rows.lengthCompare(0) <= 0) "No results found"
      else
        (Prettify.renderTable(rows.take(max).toList) ++
          (if (rows.lengthCompare(max) > 0) "..." :: Nil else Nil)).mkString("\n")

    def timeIt[A](t: Task[A]): Task[(A, Double)] = Task.delay {
      import org.threeten.bp.{Instant, Duration}
      def secondsAndTenths(dur: Duration) = dur.toMillis/100/10.0
      val startTime = Instant.now
      val a = t.run
      val endTime = Instant.now
      a -> secondsAndTenths(Duration.between(startTime, endTime))
    }

    import state.printer

    def printLog(log: Vector[PhaseResult]) =
      state.debugLevel match {
        case DebugLevel.Silent  => Task.now(())
        case DebugLevel.Normal  => printer(log.takeRight(1).mkString("\n\n") + "\n")
        case DebugLevel.Verbose => printer(log.mkString("\n\n") + "\n")
      }

    def expr = SQLParser.parseInContext(Query(query), state.path).leftMap(EParsingError(_))
    def out = name.fold[EngineError \/ Option[Path]](
        \/-(None))(
        path => Path(path).from(state.path).bimap(EPathError(_), _.some))

    ((expr |@| out) { case (expr, out) =>
      val req = QueryRequest(expr, Variables.fromMap(state.variables))
      out match {
        case Some(out) =>
          val (log, outT) = state.backend.run(req, out).run
          for {
            _   <- liftE[EngineError](printLog(log))
            out <- outT.fold[EngineTask[(EvaluationError \/ ResultPath, Double)]] (
              e => EitherT.left(Task.now(ECompilationError(e))),
              resT => liftE[EngineError](timeIt(resT.run)))
            (outPath, elapsed) = out
            _   <- liftE(printer("Query time: " + elapsed + "s"))
            _   <- outPath.fold[EngineTask[Unit]](
              e => EitherT.left(Task.now(EEvaluationError(e))),
              p => liftE(printer(
                if (p.path == out) "Source file: " + p.path.simplePathname
                else "Wrote file: " + p.path.simplePathname)))
          } yield ()
        case None =>
          val (log, resultT) = state.backend.eval(req).run
          for {
            _ <- liftE[EngineError](printLog(log))
            result <- resultT.fold[EngineTask[(ProcessingError \/ IndexedSeq[Data], Double)]] (
              e => EitherT.left(Task.now(ECompilationError(e))),
              resT => liftE[EngineError](timeIt(resT.runLog.run)))
            (results, elapsed) = result
            _   <- liftE(printer("Query time: " + elapsed + "s"))
            _   <- results.fold[EngineTask[Unit]](
              e => EitherT.left(Task.now(EProcessingError(e))),
              res => liftE(printer(summarize(state.summaryCount)(res.take(state.summaryCount + 1)))))
          } yield ()
      }
    }).fold(e => EitherT.left(Task.now(e)), ι)
  }

  def ls(state: RunState, path: Option[Path]): PathTask[Unit] = {
    def suffix(node: FilesystemNode) = node match {
      case FilesystemNode(_, Some(typ))            => "@ (" + typ + ")"
      case FilesystemNode(path, _) if path.pureDir => "/"
      case _ => ""
    }
    state.backend.ls(targetPath(state, path).asDir).flatMap(nodes =>
      liftP(state.printer(nodes.toList.sorted.map(n => n.path.simplePathname + suffix(n)).mkString("\n"))))
  }

  def save(state: RunState, path: Path, value: String): EngineTask[Unit] =
    DataCodec.parse(value)(DataCodec.Precise).fold[EngineTask[Unit]](
      e => EitherT.left(Task.now(EDataEncodingError(e))),
      data => {
        state.backend.save(targetPath(state, Some(path)), Process.emit(data)).leftMap(EProcessingError(_))
      })

  def append(state: RunState, path: Path, value: String): EngineTask[Unit] =
    DataCodec.parse(value)(DataCodec.Precise).fold[EngineTask[Unit]](
      e => EitherT.left(Task.now(EDataEncodingError(e))),
      data => {
        val errors = state.backend.append(targetPath(state, Some(path)), Process.emit(data)).runLog
        errors
          .leftMap[EngineError](EPathError(_))
          .flatMap(_.headOption.fold(
            ().point[EngineTask])(
            e => EitherT.left(Task.now(EWriteError(e)))))
      })

  def delete(state: RunState, path: Path): PathTask[Unit] =
    state.backend.delete(targetPath(state, Some(path)))

  def showDebugLevel(state: RunState, level: DebugLevel): Task[Unit] =
    state.printer(s"""|Set debug level: $level""".stripMargin)

  def showSummaryCount(state: RunState, rows: Int): Task[Unit] =
    state.printer(s"""|Set rows to show in result: $rows""".stripMargin)

  def run(args: Array[String]): Process[Task, Unit] = {
    import Command._
    import FsPath._

    def handle(s: RunState, t: EngineTask[Unit]): Task[Unit] =
      t.run.attempt.flatMap(_.fold(
        err => s.printer("Runtime error: " + err),
        _.fold(
          err => s.printer("Quasar error: " + err.message),
          Task.now)))

    def eval(s: RunState, t: EngineTask[Unit]): Process[Task, Unit] =
      // NB: running the task here seems to force it to block the
      // enqueuing thread, which ensures that the next prompt won't
      // be printed until after the task completes, even if the
      // task is forked on to a thread pool (as the mongodb backend
      // does.) The right way to fix this would probably involve
      // actually driving task execution from the prompt callback.
      Process.eval[Task, Unit](Task.delay { handle(s, t).run })

    def backendFromArgs: Task[Backend] = {
      def printErrorAndFail(e: String): Task[Backend] =
        Task.delay {
          println(s"An error occurred attempting to start the REPL:\n$e")
        } *> Task.fail(new RuntimeException(e))

      def parsePath(s: String): Task[FsPath[File, Sandboxed]] =
        parseSystemFile(s).getOrElseF(Task.fail(
          new RuntimeException(s"Invalid path to config file: $s")))

      def configFromPath(fsPath: Option[FsPath[File, Sandboxed]]): Task[CoreConfig] =
        CoreConfig.fromFileOrDefaultPaths(fsPath) | CoreConfig(MountingsConfig2.empty)

      def backendFromConfig(config: CoreConfig): Task[Backend] =
        Mounter.defaultMount(MountingsConfig.fromMC2(config.mountings))
          .fold(e => printErrorAndFail(e.message), Task.now _)
          .join

      for {
        fsPath <- args.headOption.map(parsePath).sequence
        cfg <- configFromPath(fsPath)
        backend <- backendFromConfig(cfg)
      } yield backend
    }

    Process.eval[Task, Process[Task, Unit]](backendFromArgs.tuple(commandInput) map {
      case (backend, (printer, commands)) =>
        val runState0 = RunState(printer, backend, Path.Root, None, DebugLevel.Normal, 10, Map())
        commands.scan(runState0)((state, input) => input match {
          case Cd(path)     =>
            state.copy(
              path      = targetPath(state, Some(path)).asDir,
              unhandled = None)
          case Debug(level) =>
            state.copy(debugLevel = level, unhandled = some(Debug(level)))
          case SummaryCount(rows) =>
            state.copy(summaryCount = rows, unhandled = some(SummaryCount(rows)))
          case SetVar(n, v) =>
            state.copy(variables = state.variables + (n -> v), unhandled = None)
          case UnsetVar(n)  =>
            state.copy(variables = state.variables - n, unhandled = None)
          case _            => state.copy(unhandled = Some(input))
        }).flatMap[Task, Unit] {
          case s @ RunState(_, _, path, Some(command), _, _, _) => command match {
            case Save(path, v)   => eval(s, save(s, path, v))
            case Exit            => Process.Halt(Cause.Kill)
            case Help            => Process.eval(showHelp(s))
            case Select(n, q)    => eval(s, select(s, q, n))
            case Ls(dir)         => eval(s, ls(s, dir).leftMap(EPathError(_)))
            case Append(path, v) => eval(s, append(s, path, v))
            case Delete(path)    => eval(s, delete(s, path).leftMap(EPathError(_)))
            case Debug(level)    => Process.eval(showDebugLevel(s, level))
            case SummaryCount(rows) => Process.eval(showSummaryCount(s, rows))
            case _               => Process.eval(showError(s))
          }
          case _ => Process.eval(Task.now(()))
        }
    }).join
  }

  def main(args: Array[String]): Unit = run(args).run.run
}
