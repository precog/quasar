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

import quasar.{Data, DataCodec, Variables, resolveImports, queryPlan}
import quasar.common.PhaseResults
import quasar.connector.CompileM
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.csv.CsvWriter
import quasar.effect._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.frontend.SemanticErrors
import quasar.frontend.logicalplan.{LogicalPlanR, LogicalPlan}
import quasar.fp._, ski._, numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.main.{analysis, FilesystemQueries, Prettify}
import quasar.qscript.{DiscoverPath, QScriptRead, qScriptReadToQscriptTotal}
import quasar.sql.Sql
import quasar.sql

import argonaut._, Argonaut._
import eu.timepit.refined.auto._
import matryoshka.data.Fix
import pathy.Path, Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task
import spire.std.double._

object Repl {
  import Command.{XDir, XFile}

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  val HelpMessage =
    """Quasar REPL, Copyright © 2014–2017 SlamData Inc.
      |
      |Available commands:
      |  exit
      |  help
      |  cd [path]
      |  [query]
      |  [id] <- [query]
      |  explain [query]
      |  compile [query]
      |  schema [query]
      |  ls [path]
      |  save [path] [value]
      |  append [path] [value]
      |  rm [path]
      |  set debug = 0 | 1 | 2
      |  set summaryCount = [rows]
      |  set format = table | precise | readable | csv
      |  set [var] = [value]
      |  env""".stripMargin


 final case class RunState(
    cwd:          ADir,
    debugLevel:   DebugLevel,
    summaryCount: Int,
    format:       OutputFormat,
    variables:    Map[String, String]) {

  def targetDir(path: Option[XDir]): ADir =
    path match {
      case None          => cwd
      case Some( \/-(a)) => a
      case Some(-\/ (r)) =>
        (unsandbox(cwd) </> r)
          .relativeTo(rootDir[Sandboxed])
          .cata(rootDir </> _, rootDir)
    }

  def targetFile(path: XFile): AFile =
    path match {
      case  \/-(a) => a
      case -\/ (r) =>
        (unsandbox(cwd) </> r)
          .relativeTo(rootDir[Sandboxed])
          .cata(rootDir </> _, rootDir </> file1(fileName(r)))
    }
  }

  type RunStateT[A] = AtomicRef[RunState, A]

  def command[S[_]](cmd: Command)(
    implicit
    Q:  QueryFile.Ops[S],
    M:  ManageFile.Ops[S],
    W:  WriteFile.Ops[S],
    P:  ConsoleIO.Ops[S],
    T:  Timing.Ops[S],
    S0: Mounting :<: S,
    S1: RunStateT :<: S,
    S2: ReplFail :<: S,
    S3: Task :<: S,
    S4: FileSystemFailure :<: S
  ): Free[S, Unit] = {
    import Command._

    val RS = AtomicRef.Ops[RunState, S]
    val DF = Failure.Ops[String, S]

    val fsQ = new FilesystemQueries[S]

    def write(f: (AFile, Vector[Data]) => W.M[Vector[FileSystemError]], dst: XFile, dStr: String): Free[S, Unit] =
      for {
        state <- RS.get
        pres  =  DataCodec.parse(dStr)(DataCodec.Precise)
        errs  <- EitherT.fromDisjunction[W.F](pres leftMap (_.message))
                   .flatMap(d => f(state.targetFile(dst), Vector(d)).leftMap(_.shows))
                   .bimap(s => Vector(s), _.map(_.shows))
                   .merge
        _     <- if (errs.isEmpty) P.println("Data saved.")
                 else DF.fail(errs.mkString("; "))
      } yield ()

    def runQuery[A](state: RunState, query: Q.transforms.CompExecM[A])(f: A => Free[S, Unit]): Free[S, Unit] =
      for {
        t <- T.time(query.run.run.run)
        ((log, v), elapsed) = t
        _ <- printLog[S](state.debugLevel, log)
        _ <- v match {
          case -\/ (semErr)      => DF.fail(semErr.list.map(_.shows).toList.mkString("; "))
          case  \/-(-\/ (fsErr)) => DF.fail(fsErr.shows)
          case  \/-( \/-(a))     =>
            P.println(f"Query time: ${elapsed.toMillis/1000.0}%.1fs") *>
              f(a)
        }
      } yield ()

    def compileQuery(expr: Fix[Sql], vars: Variables, basePath: ADir): (PhaseResults, SemanticErrors \/ (FileSystemError \/ Unit)) = {
      type M[A] = FileSystemErrT[CompileM, A]
      val lpr = new LogicalPlanR[Fix[LogicalPlan]]

      def absFiles(lp: Fix[LogicalPlan]): List[AFile] =
        lpr.paths(lp).foldMap(f => refineTypeAbs(f).swap.toList)

      queryPlan(expr, vars, basePath, 0L, None).liftM[FileSystemErrT]
        .flatMap(_.fold(
          _  => ().point[M],
          lp => QueryFile.convertToQScriptRead[Fix, M, QScriptRead[Fix, ?]](
                  DiscoverPath.ListContents.static[List, M](absFiles(lp)))(
                  lp).void))
        .run.run.run
    }

    cmd match {
      case Help =>
        P.println(HelpMessage)

      case Debug(level) =>
        RS.modify(_.copy(debugLevel = level)) *>
          P.println(s"Set debug level: $level")

      case SummaryCount(rows) =>
        RS.modify(_.copy(summaryCount = rows)) *>
          P.println(s"Set rows to show in result: $rows")

      case Format(fmt) =>
        RS.modify(_.copy(format = fmt)) *>
          P.println(s"Set output format: $fmt")

      case SetVar(n, v) =>
        RS.modify(state => state.copy(variables = state.variables + (n -> v))).void

      case UnsetVar(n) =>
        RS.modify(state => state.copy(variables = state.variables - n)).void

      case ListVars =>
        for {
          vars <- RS.get.map(_.variables)
          _    <- vars.toList.foldMap { case (name, value) => P.println(s"$name = $value") }
        } yield ()

      case Cd(d) =>
        for {
          dir <- RS.get.map(_.targetDir(d.some))
          _   <- DF.unattemptT(Q.ls(dir).leftMap(_.shows))
          _   <- RS.modify(state => state.copy(cwd = dir))
        } yield ()

      case Ls(d) =>
        for {
          path  <- RS.get.map(_.targetDir(d))
          files <- DF.unattemptT(Q.ls(path).leftMap(_.shows))
          names <- files.toList
                    .traverse[Free[S, ?], String](_.fold(
                      d => mountType[S](path </> dir1(d)).map(t =>
                        d.value + t.cata(t => s"@ ($t)", "/")),
                      f => mountType[S](path </> file1(f)).map(t =>
                        f.value + t.cata(t => s"@ ($t)", ""))))
          _     <- names.sorted.foldMap(P.println)
        } yield ()

      case Select(n, q) =>
        n.cata(
          name => {
            for {
              state <- RS.get
              out   =  state.cwd </> file(name)
              expr  <- DF.unattempt_(sql.fixParser.parse(q).leftMap(_.message))
              block <- DF.unattemptT(resolveImports(expr, state.cwd).leftMap(_.message))
              query =  fsQ.executeQuery(block, Variables.fromMap(state.variables), state.cwd, out)
              _     <- runQuery(state, query)(p =>
                        P.println(
                          if (p =/= out) "Source file: " + posixCodec.printPath(p)
                          else "Wrote file: " + posixCodec.printPath(p)))
            } yield ()
          },
          for {
            state <- RS.get
            expr  <- DF.unattempt_(sql.fixParser.parse(q).leftMap(_.message))
            vars  =  Variables.fromMap(state.variables)
            lim   =  (state.summaryCount > 0).option(state.summaryCount)
            block <- DF.unattemptT(resolveImports(expr, state.cwd).leftMap(_.message))
            query =  fsQ.queryResults(block, vars, state.cwd, 0L, lim >>= (l => Positive(l + 1L)))
                       .map(_.toVector)
            _     <- runQuery(state, query)(ds => summarize[S](lim, state.format)(ds))
          } yield ())

      case Explain(q) =>
        for {
          state <- RS.get
          expr  <- DF.unattempt_(sql.fixParser.parse(q).leftMap(_.message))
          vars  =  Variables.fromMap(state.variables)
          block <- DF.unattemptT(resolveImports(expr, state.cwd).leftMap(_.message))
          t     <- fsQ.explainQuery(block, vars, state.cwd).run.run.run
          (log, result) = t
          _     <- printLog(state.debugLevel, log)
          _     <- result.fold(
                    serr => DF.fail(serr.shows),
                    _.fold(
                      perr => DF.fail(perr.shows),
                      κ(().point[Free[S, ?]])))
        } yield ()

      case Compile(q) =>
        for {
          state <- RS.get
          expr  <- DF.unattempt_(sql.fixParser.parse(q).leftMap(_.message))
          vars  =  Variables.fromMap(state.variables)
          block <- DF.unattemptT(resolveImports(expr, state.cwd).leftMap(_.message))
          (log0, result) = compileQuery(block, vars, state.cwd)
          // TODO: This is a bit brittle, it'd be nice if we had a way to refer
          //       to logical sections directly.
          log   = if (state.debugLevel === DebugLevel.Verbose) log0
                  else log0.dropWhile(_.name =/= "Logical Plan")
          _     <- printLog(DebugLevel.Verbose, log)
          _     <- result.fold(
                    serr => DF.fail(serr.shows),
                    _.fold(
                      perr => DF.fail(perr.shows),
                      κ(().point[Free[S, ?]])))
        } yield ()

      case Schema(q) =>
        for {
          state <- RS.get
          expr  <- DF.unattempt_(sql.fixParser.parse(q).leftMap(_.message))
          vars  =  Variables.fromMap(state.variables)
          r     <- DF.unattemptT(analysis.querySchema[S, Fix[EJson], Double](
                     expr, vars, state.cwd, 1000L, analysis.CompressionSettings.Default
                   ).leftMap(_.shows))
          sst   <- DF.unattempt_(r.leftMap(_.shows))
          data  =  sst.map(analysis.schemaToData[Fix, Double])
          js    =  data >>= DataCodec.Precise.encode
          _     <- P.println(js.fold("{}")(_.spaces2))
        } yield ()


      case Save(f, v) =>
        write(W.saveThese(_, _), f, v)

      case Append(f, v) =>
        write(W.appendThese(_, _), f, v)

      case Delete(f) =>
        for {
          state <- RS.get
          res   <- M.delete(state.targetFile(f)).run
          _     <- res.fold(
                     err => DF.fail(err.shows),
                     _   => P.println("File deleted."))
        } yield ()

      case Exit =>
        ().point[Free[S, ?]]
    }
  }

  def mountType[S[_]](path: APath)(implicit
    M: Mounting.Ops[S]
  ): Free[S, Option[String]] =
    M.lookupType(path).run.run.map(_ >>= (_.fold(
      MountingError.invalidMount.getOption(_) ∘ (_._1.fold(_.value, "view", "module")),
      _.fold(_.value, "view", "module").some)))

  def showPhaseResults: PhaseResults => String = _.map(_.shows).mkString("\n\n")

  def printLog[S[_]](debugLevel: DebugLevel, log: PhaseResults)(implicit
    P: ConsoleIO.Ops[S]
  ): Free[S, Unit] =
    debugLevel match {
      case DebugLevel.Silent  => ().point[Free[S, ?]]
      case DebugLevel.Normal  => P.println(showPhaseResults(log.takeRight(1)) + "\n")
      case DebugLevel.Verbose => P.println(showPhaseResults(log) + "\n")
    }

  def summarize[S[_]]
    (max: Option[Int], format: OutputFormat)
    (rows: IndexedSeq[Data])
    (implicit P: ConsoleIO.Ops[S])
      : Free[S, Unit] = {
    def formatJson(codec: DataCodec)(data: Data): Option[String] =
      codec.encode(data).map(_.pretty(minspace))

    if (rows.lengthCompare(0) <= 0) P.println("No results found")
    else {
      val prefix = max.fold(rows)(rows.take).toList
      (format match {
        case OutputFormat.Table =>
          Prettify.renderTable(prefix)
        case OutputFormat.Precise =>
          prefix.map(formatJson(DataCodec.Precise)).unite
        case OutputFormat.Readable =>
          prefix.map(formatJson(DataCodec.Readable)).unite
        case OutputFormat.Csv =>
          Prettify.renderValues(prefix).map(CsvWriter(none)(_).trim)
      }).foldMap(P.println) *>
        (if (max.fold(false)(rows.lengthCompare(_) > 0)) P.println("...")
        else ().point[Free[S, ?]])
    }
  }
}
